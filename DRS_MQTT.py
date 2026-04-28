"""
DRS-MQTT: DRA + DGD + GraphRL + SafeAction reference implementation.

This file is a concise, executable reproduction of the algorithmic流程
给出的主要步骤：
1) 初始化 + 构建无环拓扑（用 level 保证只向下层转发）
2) Consensus 状态同步：Average-Consensus 同步 (RTT, Load, BW)
3) DGD（Bellman-Ford + 平滑）更新 downstream_cost
4) GraphRL 决策：对候选邻居计算 w_ij ∈ [0,1]，选 argmax
5) SafeAction：基于 w_ij 选择 Forward / RateLimit / Buffer+Batch / Compress+Defer

该实现聚焦算法逻辑与数据流，使用轻量级 numpy 作为占位的
GraphRL/Encoder/Transformer，便于后续替换为真实模型。
"""
from __future__ import annotations

import json
import math
import os
import queue
import socket
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Deque

import numpy as np
import paho.mqtt.client as mqtt
try:
    from paho.mqtt.client import CallbackAPIVersion
    _HAS_CB_API = hasattr(CallbackAPIVersion, "V311")
except Exception:
    CallbackAPIVersion = None
    _HAS_CB_API = False


# ------------------------------
# Data structures
# ------------------------------
@dataclass
class BrokerState:
    rtt: Dict[str, float] = field(default_factory=dict)  # RTT_i->j
    load: float = 0.0  # 当前队列负载
    bandwidth: Dict[str, float] = field(default_factory=dict)  # 带宽估计
    downstream_cost: float = math.inf  # 到 sink 的累计代价估计 J_i
    queue_len: float = 0.0  # 队列长度（GraphRL 特征用）
    iteration: int = 0


# ------------------------------
# GraphRL placeholder
# ------------------------------
class GraphRLPolicy:
    """
    一个轻量级的占位策略网络：
    - Embed(obs)  -> 低维向量
    - GraphTransformer(Z) -> 这里用简单邻居平均代替
    - Policy πθ(H_ij) -> MLP(1 hidden) + sigmoid 输出 w_ij ∈ [0,1]
    真实场景可替换为真正的 GraphTransformer / GNN。
    """

    def __init__(self, obs_dim: int, hidden: int = 32, seed: int = 42):
        rng = np.random.default_rng(seed)
        self.W1 = rng.normal(scale=0.1, size=(obs_dim, hidden))
        self.b1 = np.zeros(hidden)
        self.W2 = rng.normal(scale=0.1, size=(hidden, 1))
        self.b2 = np.zeros(1)

    @staticmethod
    def _sigmoid(x: np.ndarray) -> np.ndarray:
        return 1.0 / (1.0 + np.exp(-x))

    def embed(self, obs: np.ndarray) -> np.ndarray:
        # 简单线性映射作为 Embed
        return obs @ self.W1 + self.b1

    def aggregate_neighbors(self, Z: np.ndarray) -> np.ndarray:
        # 这里用均值聚合作为 GraphTransformer 占位
        if Z.ndim == 1:
            return Z
        return Z.mean(axis=0)

    def forward(self, obs_batch: List[np.ndarray]) -> List[float]:
        weights: List[float] = []
        for obs in obs_batch:
            z = self.embed(obs)
            h = self.aggregate_neighbors(z)  # 占位聚合
            logit = float(np.squeeze(h @ self.W2 + self.b2))
            w = float(self._sigmoid(logit))
            weights.append(w)
        return weights


# ------------------------------
# Broker with DRA + DGD + GraphRL + SafeAction
# ------------------------------
class DRSBroker:
    def __init__(self, broker_id: str, neighbors: List[str], level: int):
        self.id = broker_id
        self.neighbors = neighbors
        self.level = level
        self.state = BrokerState()
        self.epsilon = 0.1  # 共识步长 ε
        self.alpha = 1.0  # cost: RTT 权重
        self.beta = 1.0  # cost: Load 权重
        self.gamma = 1.0  # cost: 1/BW 权重
        self.eta = 0.2  # DGD 平滑/学习率
        self.next_hop: Optional[str] = None

        # GraphRL 输入特征: [RTT_ij, Load_j, Queue_j, f_ij, H_neighbors(mean=3 dims)]
        self.obs_dim = 5 + 3
        self.policy = GraphRLPolicy(obs_dim=self.obs_dim)

        # MQTT 相关
        self.mqtt_client: Optional[mqtt.Client] = None
        self.mqtt_connected = False
        self.host = "localhost"
        self.port_map: Dict[str, int] = {}  # 由 orchestrator 设置
        self.message_queue: Deque[Tuple[str, str, float]] = queue.deque()
        self.received_states: Dict[str, Dict] = {}
        self.received_messages: Deque[Dict] = queue.deque()
        self.lock = threading.Lock()
        # SafeAction 缓冲
        self.batch_buffer: List[Tuple[str, str]] = []
        self.batch_max = 10
        self.batch_window_ms = 50
        self.last_batch_flush = time.time()

    # ---------- Step 2: 无环候选集合 ----------
    def candidate_neighbors(self, neighbor_levels: Dict[str, int]) -> List[str]:
        # 只允许 level 更大的邻居，避免形成环
        return [n for n in self.neighbors if neighbor_levels.get(n, self.level + 1) > self.level]

    # ---------- Step 3: Consensus ----------
    def consensus_update(self, neighbor_states: Dict[str, BrokerState]):
        s = self.state
        s.iteration += 1
        for x_name in ("rtt", "bandwidth"):
            store = getattr(s, x_name)
            for n in self.neighbors:
                if n in neighbor_states and x_name in neighbor_states[n].__dict__:
                    neigh_val = getattr(neighbor_states[n], x_name)
                    if isinstance(neigh_val, dict):
                        for k, v in neigh_val.items():
                            cur = store.get(k, v)
                            store[k] = cur + self.epsilon * (v - cur)
        # load / queue 使用平均共识
        for n in self.neighbors:
            if n in neighbor_states:
                l = neighbor_states[n].load
                s.load = s.load + self.epsilon * (l - s.load)
                q = neighbor_states[n].queue_len
                s.queue_len = s.queue_len + self.epsilon * (q - s.queue_len)

    # ---------- cost function ----------
    def link_cost(self, j: str, neighbor_state: Optional[BrokerState]) -> float:
        rtt = neighbor_state.rtt.get(j, self.state.rtt.get(j, 10.0)) if neighbor_state else self.state.rtt.get(j, 10.0)
        bw = neighbor_state.bandwidth.get(j, self.state.bandwidth.get(j, 1.0)) if neighbor_state else self.state.bandwidth.get(j, 1.0)
        load_j = neighbor_state.load if neighbor_state else 0.0
        return self.alpha * rtt + self.beta * load_j + self.gamma * (1.0 / max(bw, 1e-3))

    # ---------- Step 4: DGD 更新 ----------
    def dgd_update(self, neighbor_states: Dict[str, BrokerState], sink_id: str):
        if self.id == sink_id:
            self.state.downstream_cost = 0.0
            return

        total_costs: Dict[str, float] = {}
        for j in self.neighbors:
            ns = neighbor_states.get(j)
            f_ij = self.link_cost(j, ns)
            j_down = ns.downstream_cost if ns else math.inf
            total_costs[j] = f_ij + j_down

        if not total_costs:
            return
        m_i = min(total_costs.values())
        if math.isfinite(m_i):
            self.state.downstream_cost = (1 - self.eta) * self.state.downstream_cost + self.eta * m_i
        # 记录供 GraphRL 使用
        self._last_total_costs = total_costs

    # ---------- Step 5: GraphRL 选路 ----------
    def _build_obs(self, neighbor: str, neighbor_states: Dict[str, BrokerState]) -> np.ndarray:
        ns = neighbor_states.get(neighbor)
        rtt = self.state.rtt.get(neighbor, 10.0)
        load_j = ns.load if ns else 0.0
        queue_j = ns.queue_len if ns else 0.0
        f_ij = self.link_cost(neighbor, ns)
        # 邻居结构特征占位: 用 3 维随机/常数 (可替换为真实拓扑度等)
        h_neighbors = np.array([len(self.neighbors), load_j, queue_j], dtype=float)
        obs = np.array([rtt, load_j, queue_j, f_ij, self.state.load], dtype=float)
        return np.concatenate([obs, h_neighbors])

    def graphrl_select(self, neighbor_states: Dict[str, BrokerState], neighbor_levels: Optional[Dict[str, int]] = None) -> Tuple[Optional[str], Dict[str, float], Dict[str, float]]:
        # 使用候选邻居过滤（如果提供了level信息）
        candidates = self.neighbors
        if neighbor_levels is not None:
            candidates = self.candidate_neighbors(neighbor_levels)

        obs_batch = []
        neigh_order = []
        for n in candidates:
            if n in neighbor_states:  # 只考虑有状态信息的邻居
                obs_batch.append(self._build_obs(n, neighbor_states))
                neigh_order.append(n)

        if not obs_batch:
            return None, {}, getattr(self, "_last_total_costs", {})

        weights = self.policy.forward(obs_batch)
        weight_map = {n: w for n, w in zip(neigh_order, weights)}
        # 最终选择 argmax w_ij
        best = max(weight_map, key=weight_map.get) if weight_map else None
        self.next_hop = best
        return best, weight_map, getattr(self, "_last_total_costs", {})

    # ---------- Step 6: SafeAction ----------
    @staticmethod
    def safe_action(weight: float) -> str:
        if weight <= 0.60:
            return "Forward"
        if weight <= 0.80:
            return "RateLimit"
        if weight <= 0.95:
            return "BufferAndBatch"
        return "CompressAndDefer"

    # ---------- MQTT 接入 ----------
    def _on_connect(self, client, userdata, flags, rc):
        self.mqtt_connected = rc == 0
        if self.mqtt_connected:
            # 订阅本 broker 的消息主题与状态主题
            client.subscribe(f"message/{self.id}")
            client.subscribe(f"state/{self.id}")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            topic = msg.topic
            with self.lock:
                if topic.startswith("state/"):
                    sender = topic.split("/")[1]
                    self.received_states[sender] = payload
                elif topic.startswith("message/"):
                    self.received_messages.append(payload)
        except Exception:
            pass

    def connect_mqtt(self, host: str, port_map: Dict[str, int]):
        self.host = host
        self.port_map = port_map
        if self.mqtt_client:
            return
        # 使用显式协议版本避免老版本警告
        client_kwargs = {
            "client_id": f"drs_{self.id}_{int(time.time()*1000)}",
            "protocol": mqtt.MQTTv311,
        }
        if _HAS_CB_API:
            client_kwargs["callback_api_version"] = CallbackAPIVersion.V311  # type: ignore[attr-defined]
        self.mqtt_client = mqtt.Client(**client_kwargs)
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message
        port = port_map.get(self.id, 1884)
        try:
            self.mqtt_client.connect(host, port, 60)
        except Exception as e:
            raise RuntimeError(f"[{self.id}] connect to {host}:{port} failed: {e}") from e
        self.mqtt_client.loop_start()
        # 等待连接
        for _ in range(30):
            if self.mqtt_connected:
                break
            time.sleep(0.1)

    def publish_state(self):
        if not (self.mqtt_client and self.mqtt_connected):
            return
        payload = json.dumps(
            {
                "rtt": self.state.rtt,
                "bandwidth": self.state.bandwidth,
                "load": self.state.load,
                "queue_len": self.state.queue_len,
                "downstream_cost": self.state.downstream_cost,
                "iteration": self.state.iteration,
            }
        )
        self.mqtt_client.publish(f"state/{self.id}", payload, qos=0)

    def consume_received_states(self) -> Dict[str, BrokerState]:
        out: Dict[str, BrokerState] = {}
        with self.lock:
            items = list(self.received_states.items())
            self.received_states.clear()
        for nid, d in items:
            st = BrokerState()
            st.rtt = d.get("rtt", {})
            st.bandwidth = d.get("bandwidth", {})
            st.load = d.get("load", 0.0)
            st.queue_len = d.get("queue_len", 0.0)
            st.downstream_cost = d.get("downstream_cost", math.inf)
            st.iteration = d.get("iteration", 0)
            out[nid] = st
        return out

    def add_local_message(self, topic: str, message: str):
        with self.lock:
            self.message_queue.append((topic, message, time.time()))
            self.state.load = float(len(self.message_queue))
            self.state.queue_len = self.state.load

    def _publish_to_neighbor(self, neighbor: str, topic: str, message: str):
        # 使用邻居的端口直接发布到其 message 主题
        if not (self.mqtt_client and self.mqtt_connected):
            return False
        payload = json.dumps({"topic": topic, "message": message, "from": self.id, "ts": time.time()})
        return self.mqtt_client.publish(f"message/{neighbor}", payload, qos=0).rc == 0

    def _apply_safe_action(self, action: str, topic: str, message: str, neighbor: str):
        if action == "Forward":
            return self._publish_to_neighbor(neighbor, topic, message)
        if action == "RateLimit":
            # 简单节流：sleep 触发后再发
            time.sleep(0.05)
            return self._publish_to_neighbor(neighbor, topic, message)
        if action == "BufferAndBatch":
            self.batch_buffer.append((topic, message))
            now = time.time()
            if len(self.batch_buffer) >= self.batch_max or (now - self.last_batch_flush) * 1000 >= self.batch_window_ms:
                for t, m in self.batch_buffer:
                    self._publish_to_neighbor(neighbor, t, m)
                self.batch_buffer.clear()
                self.last_batch_flush = now
            return True
        if action == "CompressAndDefer":
            time.sleep(0.1)
            # 简单压缩占位：截断字符串
            compressed = message[: max(1, len(message) // 2)]
            return self._publish_to_neighbor(neighbor, topic, compressed)
        return False

    def pump_incoming_messages(self):
        """把收到的 message/<id> 放入本地队列"""
        pending: List[Tuple[str, str]] = []
        with self.lock:
            while self.received_messages:
                d = self.received_messages.popleft()
                t = d.get("topic")
                m = d.get("message")
                if t and m:
                    pending.append((t, m))
        for t, m in pending:
            self.add_local_message(t, m)

    def forward_one(self, neighbor_states: Dict[str, BrokerState], neighbor_levels: Optional[Dict[str, int]] = None):
        if not self.message_queue:
            return None
        topic, message, _ = self.message_queue.popleft()
        best, weights, _ = self.graphrl_select(neighbor_states, neighbor_levels)
        if not best:
            return None
        action = self.safe_action(weights.get(best, 0.0))
        sent = self._apply_safe_action(action, topic, message, best)
        # 更新负载
        with self.lock:
            self.state.load = float(len(self.message_queue))
            self.state.queue_len = self.state.load
        return {"next": best, "action": action, "sent": sent, "topic": topic}


# ------------------------------
# Orchestrator
# ------------------------------
class DRSNetwork:
    def __init__(self, topology: Dict[str, List[str]], levels: Dict[str, int], sink_id: str):
        self.brokers = {bid: DRSBroker(bid, neighbors, levels.get(bid, 0)) for bid, neighbors in topology.items()}
        self.sink_id = sink_id
        self.port_map = {bid: 1884 + i for i, bid in enumerate(sorted(self.brokers.keys()))}
        self.host = "localhost"
        self.broker_processes: Dict[str, subprocess.Popen] = {}

    def _collect_states(self) -> Dict[str, BrokerState]:
        return {bid: b.state for bid, b in self.brokers.items()}

    def _is_port_in_use(self, port: int, host: str = "localhost") -> bool:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        try:
            res = sock.connect_ex((host, port))
            sock.close()
            return res == 0
        except Exception:
            sock.close()
            return False

    def start_mqtt_brokers(self, host: str = "localhost"):
        """
        启动与 DRA_MQTT 类似的本地 mosquitto 实例，每个 broker 一个端口。
        如果端口已被占用，则跳过该 broker（假设已有外部实例）。
        """
        mosquitto_paths = [
            r"C:\Program Files\mosquitto\mosquitto.exe",
            r"C:\mosquitto\mosquitto.exe",
            "mosquitto",
            "/usr/local/sbin/mosquitto",
            "/usr/sbin/mosquitto",
        ]
        mosquitto_exe = None
        for p in mosquitto_paths:
            if os.path.exists(p) or p in {"mosquitto", "/usr/local/sbin/mosquitto", "/usr/sbin/mosquitto"}:
                try:
                    subprocess.run([p, "-h"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
                    mosquitto_exe = p
                    break
                except Exception:
                    continue
        if not mosquitto_exe:
            print("Warning: mosquitto not found, please install or add to PATH.")
            return

        for bid, port in self.port_map.items():
            if self._is_port_in_use(port, host):
                print(f"Port {port} already in use, skip starting broker {bid}.")
                continue
            cfg = tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False)
            cfg.write(f"listener {port}\nallow_anonymous true\npersistence false\n")
            cfg.close()
            try:
                creationflags = 0
                if os.name == "nt" and mosquitto_exe.endswith(".exe"):
                    creationflags = subprocess.CREATE_NEW_CONSOLE  # type: ignore[attr-defined]
                proc = subprocess.Popen(
                    [mosquitto_exe, "-c", cfg.name, "-p", str(port)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    creationflags=creationflags,
                )
                self.broker_processes[bid] = proc
                time.sleep(0.3)
                if proc.poll() is not None:
                    out, err = proc.communicate(timeout=1)
                    print(f"Start broker {bid} failed: {err.decode(errors='ignore')}")
            except Exception as e:
                print(f"Start broker {bid} failed: {e}")

    def stop_mqtt_brokers(self):
        for bid, proc in self.broker_processes.items():
            try:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=3)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        self.broker_processes.clear()

    def connect_all(self, host: str = "localhost", start_local_brokers: bool = False):
        self.host = host
        if start_local_brokers:
            self.start_mqtt_brokers(host)
        for b in self.brokers.values():
            b.connect_mqtt(host, self.port_map)

    def run_once(self):
        # Step 2: 无环候选集合（这里只做过滤信息，不改邻居列表）
        neighbor_levels = {bid: b.level for bid, b in self.brokers.items()}

        # Step 3: 共识
        states = self._collect_states()
        for b in self.brokers.values():
            b.consensus_update({n: states[n] for n in b.neighbors if n in states})

        # Step 4: DGD 更新
        states = self._collect_states()
        for b in self.brokers.values():
            b.dgd_update({n: states[n] for n in b.neighbors if n in states}, sink_id=self.sink_id)

        # Step 5 + 6: GraphRL 选路 + SafeAction
        states = self._collect_states()
        decisions = {}
        for bid, b in self.brokers.items():
            # 使用level信息进行候选邻居过滤
            best, weights, totals = b.graphrl_select(
                {n: states[n] for n in b.neighbors if n in states},
                neighbor_levels
            )
            action = b.safe_action(weights.get(best, 0.0) if best else 0.0)
            decisions[bid] = {
                "next_hop": best,
                "weights": weights,
                "total_costs": totals,
                "action": action,
                "downstream_cost": b.state.downstream_cost,
            }
        return decisions

    def pump_all(self):
        for b in self.brokers.values():
            b.pump_incoming_messages()

    def publish_states(self):
        for b in self.brokers.values():
            b.publish_state()

    def forward_all_once(self):
        """对每个 broker 若有消息则尝试转发一条"""
        states = self._collect_states()
        neighbor_levels = {bid: b.level for bid, b in self.brokers.items()}
        results = {}
        for bid, b in self.brokers.items():
            res = b.forward_one(
                {n: states[n] for n in b.neighbors if n in states},
                neighbor_levels
            )
            if res:
                results[bid] = res
        return results


# ------------------------------
# Demo
# ------------------------------
def demo():
    """
    运行一个最小示例，演示一次迭代（无需随机模拟数据，只使用初始默认值）：
    拓扑与 DRA_MQTT 相同：A-B-C-D-E，sink=E
    需事先在本机启动 5 个 mosquitto broker（端口 1884~1888），或自行调整 port_map。
    """
    topology = {
        "A": ["B", "C", "D"],
        "B": ["A", "C"],
        "C": ["A", "B", "D", "E"],
        "D": ["A", "C"],
        "E": ["C"],
    }
    levels = {"A": 0, "B": 1, "C": 1, "D": 1, "E": 2}
    net = DRSNetwork(topology, levels, sink_id="E")
    # 若本机没有已运行的 1884~1888 端口 mosquitto，可启用 start_local_brokers=True 自动拉起
    net.connect_all(start_local_brokers=True)

    # 为演示添加一条消息到源节点 A
    net.brokers["A"].add_local_message("demo/topic", "hello")
    # 将 sink 下游代价置 0
    net.brokers["E"].state.downstream_cost = 0.0

    # 状态发布 + 一次迭代
    net.publish_states()
    time.sleep(0.2)  # 等待 MQTT 送达
    net.pump_all()
    decisions = net.run_once()
    fwd = net.forward_all_once()

    print("\n=== DRS-MQTT Demo (single iteration, MQTT-backed) ===")
    for bid, info in decisions.items():
        print(
            f"{bid}: next={info['next_hop']}, action={info['action']}, "
            f"downstream_cost={info['downstream_cost']:.3f}"
        )
        print(f"  w: { {k: round(v, 3) for k, v in info['weights'].items()} }")
    print("Forward results:", fwd)
    # 清理本地启动的 broker
    net.stop_mqtt_brokers()


if __name__ == "__main__":
    demo()

