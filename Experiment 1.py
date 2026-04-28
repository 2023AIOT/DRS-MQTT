import argparse
import asyncio
import json
import math
import os
import socket
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import paho.mqtt.client as mqtt

try:
    import matplotlib  # type: ignore

    matplotlib.use("TkAgg")
    import matplotlib.pyplot as plt  # type: ignore
    from matplotlib import font_manager  # type: ignore

    def _configure_font():
        preferred_fonts = ["SimHei", "Microsoft YaHei", "Microsoft JhengHei", "STHeiti", "Heiti SC"]
        available = {f.name: f for f in font_manager.fontManager.ttflist}
        for name in preferred_fonts:
            if name in available:
                plt.rcParams["font.family"] = name
                plt.rcParams["axes.unicode_minus"] = False
                return
        # fallback to default but still avoid minus sign garbling
        plt.rcParams["axes.unicode_minus"] = False

    _configure_font()
except Exception:  # pragma: no cover - optional dependency
    matplotlib = None  # type: ignore
    plt = None  # type: ignore

from DRA_MQTT import ConsensusDGDNetwork, ConsensusDGDBroker


def find_mosquitto_binary() -> Optional[str]:
    """Try to locate the mosquitto executable on common paths."""
    candidates = [
        r"C:\Program Files\mosquitto\mosquitto.exe",
        r"C:\mosquitto\mosquitto.exe",
        "mosquitto",
        "/usr/local/sbin/mosquitto",
        "/usr/sbin/mosquitto",
    ]
    for path in candidates:
        if os.path.exists(path) or path in {"mosquitto", "/usr/local/sbin/mosquitto", "/usr/sbin/mosquitto"}:
            try:
                subprocess.run([path, "-h"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
                return path
            except Exception:
                continue
    return None


@dataclass
class ConvergenceHistory:
    iterations: List[int]
    downstream_costs: Dict[str, List[float]]
    edge_costs: Dict[str, List[float]]
    global_cost: List[float]
    error_norm: List[float]
    tracked_nodes: List[str]

    def _round_series(self, series: List[float]) -> List[Optional[float]]:
        rounded: List[Optional[float]] = []
        for value in series:
            if math.isfinite(value):
                rounded.append(round(value, 4))
            else:
                rounded.append(None)
        return rounded

    def to_dict(self) -> Dict[str, Any]:
        return {
            "iterations": self.iterations,
            "downstream_costs": {node: self._round_series(values) for node, values in self.downstream_costs.items()},
            "edge_costs": {node: self._round_series(values) for node, values in self.edge_costs.items()},
            "global_cost": self._round_series(self.global_cost),
            "error_norm": self._round_series(self.error_norm),
            "tracked_nodes": self.tracked_nodes,
        }


@dataclass
class MeasurementSummary:
    scenario: str
    message_count: int
    successful_messages: int
    average_latency_ms: float
    median_latency_ms: float
    throughput_messages_per_sec: float
    latencies_ms: List[float]
    timestamps: List[float]  # 时间戳列表（秒），用于横坐标
    convergence: Optional[ConvergenceHistory] = None

    def to_dict(self) -> Dict:
        payload = asdict(self)
        payload["latencies_ms"] = [round(x, 3) for x in self.latencies_ms]
        if self.convergence:
            payload["convergence"] = self.convergence.to_dict()
        return payload


class MosquittoNetwork:
    """Utility to start and stop a chain of Mosquitto brokers with optional bridging."""

    def __init__(
        self,
        topology: List[Tuple[str, int, Optional[Union[str, List[str]]]]],
        configs_dir: Path,
    ):
        self.topology = topology
        self.configs_dir = configs_dir
        self.configs_dir.mkdir(parents=True, exist_ok=True)
        self.processes: Dict[str, subprocess.Popen] = {}
        self.config_files: Dict[str, Path] = {}
        self.mosquitto_path = find_mosquitto_binary()
        if not self.mosquitto_path:
            raise RuntimeError("mosquitto executable not found. Please install Mosquitto and add it to PATH.")

    def _write_config(
        self,
        broker_id: str,
        port: int,
        parent_ports: Optional[List[int]],
    ) -> Path:
        config = [
            f"listener {port}",
            "allow_anonymous true",
            "persistence false",
            "max_inflight_messages 100",
            "max_queued_messages 1000",
        ]
        if parent_ports:
            for parent_port in parent_ports:
                config += [
                    "",
                    f"connection bridge_{broker_id}_to_{parent_port}",
                    f"address localhost:{parent_port}",
                    "keepalive_interval 60",
                    "restart_timeout 2",
                    "cleansession true",
                    "start_type automatic",
                    "notifications false",
                    "topic # both 1",
                    "bridge_protocol_version mqttv311",
                    "try_private true",
                ]
        config_path = self.configs_dir / f"mosquitto_{broker_id}.conf"
        config_path.write_text("\n".join(config), encoding="utf-8")
        self.config_files[broker_id] = config_path
        return config_path

    def start(self):
        for broker_id, port, parent_label in self.topology:
            parent_ports: Optional[List[int]] = None
            if parent_label:
                labels = parent_label if isinstance(parent_label, list) else [parent_label]
                parent_ports = []
                for label in labels:
                    matches = [p for bid, p, _ in self.topology if bid == label]
                    if matches:
                        parent_ports.append(matches[0])
                if not parent_ports:
                    parent_ports = None
            config_path = self._write_config(broker_id, port, parent_ports)
            creationflags = 0
            if os.name == "nt" and self.mosquitto_path.endswith(".exe"):
                creationflags = subprocess.CREATE_NEW_CONSOLE  # type: ignore[attr-defined]
            process = subprocess.Popen(
                [self.mosquitto_path, "-c", str(config_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=creationflags,
                text=True,
            )
            self.processes[broker_id] = process
            time.sleep(0.5)
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise RuntimeError(
                    f"Broker {broker_id} (port {port}) 启动失败。\n"
                    f"stdout:\n{stdout}\n---\nstderr:\n{stderr}"
                )
            print(f"[MosquittoNetwork] Broker {broker_id} 已启动，端口 {port}，PID {process.pid}")
        # allow the bridge links to settle
        time.sleep(2)

    def stop(self):
        for process in self.processes.values():
            try:
                process.terminate()
                process.wait(timeout=5)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass
        self.processes.clear()
        for cfg in self.config_files.values():
            try:
                cfg.unlink(missing_ok=True)  # type: ignore[arg-type]
            except TypeError:
                if cfg.exists():
                    cfg.unlink()
        self.config_files.clear()


class TraditionalMQTTBenchmark:
    """Measure latency/throughput on a classic bridge-based distributed MQTT setup."""

    def __init__(
        self,
        base_port: int = 1884,
        message_topic: str = "experiment/traditional",
        topology: Optional[List[Tuple[str, int, Optional[Union[str, List[str]]]]]] = None,
        scenario_name: str = "D-MQTT",
        source_broker: str = "A",
        target_broker: str = "E",
    ):
        self.base_port = base_port
        self.topic = message_topic
        self.scenario_name = scenario_name
        self.source_broker = source_broker
        self.target_broker = target_broker
        configs_dir = Path(tempfile.gettempdir()) / f"{scenario_name.lower()}_mqtt_configs"
        if topology is None:
            topology = [
                ("A", base_port + 0, None),
                ("B", base_port + 1, "A"),
                ("C", base_port + 2, "B"),
                ("D", base_port + 3, "C"),
                ("E", base_port + 4, "D"),
            ]
        self.network = MosquittoNetwork(topology, configs_dir)
        self.broker_ports: Dict[str, int] = {broker_id: port for broker_id, port, _ in topology}
        if self.source_broker not in self.broker_ports or self.target_broker not in self.broker_ports:
            raise ValueError("Source or target broker missing from topology.")
        self.publisher: Optional[mqtt.Client] = None
        self.subscriber: Optional[mqtt.Client] = None
        self._latencies: List[float] = []
        self._timestamps: List[float] = []  # 记录每条消息的时间戳
        self._expected = 0
        self._receive_event = threading.Event()
        self._last_receive_time: Optional[float] = None
        self._first_send_time: Optional[float] = None

    @staticmethod
    def _wait_for_port(port: int, timeout: float = 10.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.5)
                try:
                    sock.connect(("localhost", port))
                    return True
                except OSError:
                    time.sleep(0.2)
        return False

    @staticmethod
    def _connect_with_retry(client: mqtt.Client, port: int, role: str, attempts: int = 5, delay: float = 1.0):
        last_exc: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                client.connect("localhost", port, keepalive=60)
                return
            except OSError as exc:
                last_exc = exc
                print(f"[TraditionalMQTT] {role} 第 {attempt}/{attempts} 次连接端口 {port} 失败: {exc}")
                time.sleep(delay)
        raise RuntimeError(f"[TraditionalMQTT] {role} 无法连接到端口 {port}，请确认代理已启动。") from last_exc

    def _on_subscribe_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            send_time = payload["send_time"]
            receive_time = time.perf_counter()
            latency_ms = (receive_time - send_time) * 1000.0
            self._latencies.append(latency_ms)
            # 记录接收时间戳（相对于第一条消息的时间）
            if self._first_send_time is not None:
                self._timestamps.append(receive_time - self._first_send_time)
            else:
                self._timestamps.append(0.0)
            self._last_receive_time = receive_time
            if len(self._latencies) >= self._expected:
                self._receive_event.set()
        except Exception as exc:
            print(f"[TraditionalMQTT] Failed to decode message: {exc}")

    def _setup_clients(self, qos: int):
        sub_port = self.broker_ports[self.target_broker]
        pub_port = self.broker_ports[self.source_broker]
        for port, label in [(pub_port, "发布端"), (sub_port, "订阅端")]:
            if not self._wait_for_port(port, timeout=10.0):
                raise RuntimeError(f"{label}端口 {port} 在超时时间内未就绪，请检查 Mosquitto 进程。")
        self.subscriber = mqtt.Client(client_id=f"trad_sub_{int(time.time()*1000)}")
        self.subscriber.on_message = self._on_subscribe_message
        self._connect_with_retry(self.subscriber, sub_port, "订阅客户端")
        self.subscriber.subscribe(self.topic, qos=qos)
        self.subscriber.loop_start()
        time.sleep(1.0)

        self.publisher = mqtt.Client(client_id=f"trad_pub_{int(time.time()*1000)}")
        self._connect_with_retry(self.publisher, pub_port, "发布客户端")
        self.publisher.loop_start()

    def _teardown_clients(self):
        if self.publisher:
            self.publisher.loop_stop()
            self.publisher.disconnect()
            self.publisher = None
        if self.subscriber:
            self.subscriber.loop_stop()
            self.subscriber.disconnect()
            self.subscriber = None

    def run(
        self,
        message_count: int,
        payload_size: int,
        qos: int,
        send_interval: float,
        timeout_per_message: float = 2.0,
        total_duration: Optional[float] = None,
    ) -> MeasurementSummary:
        """
        If total_duration is provided (seconds), publish messages until the elapsed time
        reaches total_duration. Otherwise publish exactly message_count messages.
        """
        self.network.start()
        # prepare structures for optional per-broker monitoring (counts of messages seen by each broker)
        monitor_counts: Dict[str, int] = {bid: 0 for bid in self.broker_ports.keys()}
        monitor_clients: Dict[str, mqtt.Client] = {}
        try:
            self._latencies = []
            self._timestamps = []
            self._receive_event.clear()
            self._last_receive_time = None
            self._first_send_time = None
            self._setup_clients(qos=qos)

            # attach lightweight monitoring clients to each broker to count messages seen locally
            def _make_on_message(bid: str):
                def _on_message(client, userdata, msg):
                    try:
                        monitor_counts[bid] += 1
                    except Exception:
                        pass

                return _on_message

            for bid, port in self.broker_ports.items():
                try:
                    mc = mqtt.Client(client_id=f"mon_{bid}_{int(time.time()*1000)}")
                    mc.on_message = _make_on_message(bid)
                    # connect to the broker instance and subscribe; non-fatal if fails
                    try:
                        mc.connect("localhost", port, keepalive=60)
                        mc.subscribe(self.topic, qos=0)
                        mc.loop_start()
                        monitor_clients[bid] = mc
                    except Exception as exc:
                        print(f"[monitor] Failed to attach monitor to broker {bid} port {port}: {exc}")
                except Exception:
                    # ensure monitoring never breaks the main benchmark
                    continue

            messages_sent = 0
            if total_duration and total_duration > 0:
                # time-based sending
                start_time = time.perf_counter()
                while time.perf_counter() - start_time < total_duration:
                    payload = {
                        "id": f"trad_{messages_sent}",
                        "send_time": time.perf_counter(),
                        "sequence": messages_sent,
                        "data": "X" * payload_size,
                    }
                    if self._first_send_time is None:
                        self._first_send_time = payload["send_time"]
                    info = self.publisher.publish(self.topic, json.dumps(payload), qos=qos)
                    info.wait_for_publish()
                    messages_sent += 1
                    time.sleep(send_interval)
                # after sending, set expected to messages_sent so subscriber waits accordingly
                self._expected = messages_sent
                wait_time = timeout_per_message * max(1, messages_sent)
                self._receive_event.wait(timeout=wait_time)
            else:
                # message-count based sending (legacy behavior)
                self._expected = message_count
                for i in range(message_count):
                    payload = {
                        "id": f"trad_{i}",
                        "send_time": time.perf_counter(),
                        "sequence": i,
                        "data": "X" * payload_size,
                    }
                    if self._first_send_time is None:
                        self._first_send_time = payload["send_time"]
                    info = self.publisher.publish(self.topic, json.dumps(payload), qos=qos)
                    info.wait_for_publish()
                    time.sleep(send_interval)
                wait_time = timeout_per_message * max(1, message_count)
                self._receive_event.wait(timeout=wait_time)

        finally:
            self._teardown_clients()
            # stop monitoring clients before stopping the network so they can report counts
            for mc in list(monitor_clients.values()):
                try:
                    mc.loop_stop()
                    mc.disconnect()
                except Exception:
                    pass
            self.network.stop()

        if not self._latencies:
            raise RuntimeError("Traditional MQTT test produced no latency samples.")
        avg_latency = sum(self._latencies) / len(self._latencies)
        med_latency = sorted(self._latencies)[len(self._latencies) // 2]
        if self._first_send_time is not None and self._last_receive_time is not None:
            total_window = self._last_receive_time - self._first_send_time
        else:
            total_window = 0.0
        throughput = len(self._latencies) / total_window if total_window > 0 else 0.0
        # 确保时间戳列表长度与延迟列表一致
        if len(self._timestamps) != len(self._latencies):
            # 如果时间戳数量不匹配，使用消息序号作为时间（假设均匀分布）
            if self._first_send_time is not None and self._last_receive_time is not None:
                total_time = self._last_receive_time - self._first_send_time
                self._timestamps = [i * total_time / max(1, len(self._latencies) - 1) for i in range(len(self._latencies))]
            else:
                self._timestamps = list(range(len(self._latencies)))
        result = MeasurementSummary(
            scenario=self.scenario_name,
            message_count=len(self._latencies),
            successful_messages=len(self._latencies),
            average_latency_ms=avg_latency,
            median_latency_ms=med_latency,
            throughput_messages_per_sec=throughput,
            latencies_ms=self._latencies,
            timestamps=self._timestamps,
        )
        # attach monitoring results if any were collected
        try:
            result.per_broker_counts = monitor_counts
            result.control_messages_per_broker = monitor_counts
        except Exception:
            pass
        return result


class DRAExperimentBenchmark:
    """Plan a DRA-optimized Mosquitto bridge topology and measure on real brokers."""

    def __init__(self, topic: str = "experiment/traditional", base_port: int = 1884):
        topology = {
            "A": ["B", "C", "D"],
            "B": ["A", "C"],
            "C": ["A", "B", "D", "E"],
            "D": ["A", "C"],
            "E": ["C"],
        }
        self.topic = topic
        self.base_port = base_port
        self.source_broker = "A"
        self.target_broker = "E"
        self.network = ConsensusDGDNetwork(["A", "B", "C", "D", "E"], topology)
        self._planned_path: Optional[List[str]] = None
        self.convergence_history: Optional[ConvergenceHistory] = None
        self._convergence_buffers: Optional[Dict[str, Any]] = None

    def _reset_convergence_tracking(self):
        broker_ids = list(self.network.brokers.keys())
        self._convergence_buffers = {
            "iterations": [],
            "downstream_costs": {broker_id: [] for broker_id in broker_ids},
            "edge_costs": {broker_id: [] for broker_id in broker_ids},
            "global_cost": [],
        }

    def _compute_best_total_cost(self, broker: ConsensusDGDBroker) -> float:
        best_total = float("inf")
        for neighbor in broker.neighbors:
            edge_cost = broker.calculate_cost_function(neighbor)
            neighbor_state = self.network.brokers.get(neighbor)
            neighbor_downstream = (
                neighbor_state.state.downstream_cost if neighbor_state else float("inf")
            )
            if math.isfinite(neighbor_downstream):
                total_cost = edge_cost + neighbor_downstream
            else:
                total_cost = edge_cost
            if total_cost < best_total:
                best_total = total_cost
        return best_total

    def _record_convergence_snapshot(self):
        if self._convergence_buffers is None:
            return
        iteration_index = len(self._convergence_buffers["iterations"]) + 1
        self._convergence_buffers["iterations"].append(iteration_index)
        for broker_id, broker in self.network.brokers.items():
            self._convergence_buffers["downstream_costs"][broker_id].append(broker.state.downstream_cost)
            self._convergence_buffers["edge_costs"][broker_id].append(self._compute_best_total_cost(broker))
        source_cost = self.network.brokers[self.source_broker].state.downstream_cost
        self._convergence_buffers["global_cost"].append(source_cost)

    def _finalize_convergence_history(self, path: List[str]):
        if not self._convergence_buffers:
            return
        iterations = self._convergence_buffers["iterations"]
        if not iterations:
            self.convergence_history = None
            self._convergence_buffers = None
            return
        downstream_costs = self._convergence_buffers["downstream_costs"]
        edge_costs = self._convergence_buffers["edge_costs"]
        global_cost = self._convergence_buffers["global_cost"]
        final_values = {bid: series[-1] for bid, series in downstream_costs.items() if series}
        error_norm: List[float] = []
        for idx in range(len(iterations)):
            accum = 0.0
            for bid, series in downstream_costs.items():
                if idx >= len(series):
                    continue
                current_value = series[idx]
                final_value = final_values.get(bid)
                if final_value is None or not (math.isfinite(current_value) and math.isfinite(final_value)):
                    continue
                diff = current_value - final_value
                accum += diff * diff
            error_norm.append(math.sqrt(accum))
        self.convergence_history = ConvergenceHistory(
            iterations=iterations.copy(),
            downstream_costs={bid: values.copy() for bid, values in downstream_costs.items()},
            edge_costs={bid: values.copy() for bid, values in edge_costs.items()},
            global_cost=global_cost.copy(),
            error_norm=error_norm,
            tracked_nodes=path,
        )
        self._convergence_buffers = None

    async def _plan_route(self, iterations: int = 40) -> List[str]:
        if self._planned_path and self.convergence_history:
            return self._planned_path

        print("[DRA] Starting control-plane consensus to compute optimal path...")
        self.convergence_history = None
        self._reset_convergence_tracking()
        try:
            self.network.start_mqtt_brokers()
            await asyncio.sleep(2)
            self.network.connect_all_brokers()
            await asyncio.sleep(1)
            self.network.brokers[self.target_broker].subscribe(self.topic, "dra_subscriber")
            self.network.set_target_broker(self.target_broker)
            for _ in range(iterations):
                await self.network.run_consensus_iteration()
                self._record_convergence_snapshot()
                await asyncio.sleep(0.05)
        finally:
            self.network.stop_mqtt_brokers()

        path = self._extract_path()
        self._planned_path = path
        self._finalize_convergence_history(path)
        print(f"[DRA] Optimal path selected: {' -> '.join(path)}")
        return path

    def _extract_path(self) -> List[str]:
        current = self.source_broker
        target = self.target_broker
        path = [current]
        visited = {current}
        max_steps = len(self.network.brokers)

        for _ in range(max_steps):
            if current == target:
                return path
            broker = self.network.brokers[current]
            next_hop = broker.next_hop
            if not next_hop:
                raise RuntimeError(f"DRA algorithm未能为代理 {current} 选择下一跳。")
            if next_hop in visited:
                loop_str = " -> ".join(path + [next_hop])
                raise RuntimeError(f"DRA 路径计算出现环：{loop_str}")
            path.append(next_hop)
            visited.add(next_hop)
            current = next_hop

        raise RuntimeError("DRA 算法在最大步数内未能到达目标代理。")

    def _build_topology_from_path(self, path: List[str]) -> List[Tuple[str, int, Optional[str]]]:
        parent_by_node: Dict[str, Optional[str]] = {path[0]: None}
        for prev, curr in zip(path, path[1:]):
            parent_by_node[curr] = prev

        # Ensure every known broker appears in the topology, even if不在路径上
        topology: List[Tuple[str, int, Optional[str]]] = []
        for broker_id, port in ConsensusDGDBroker.PORT_MAP.items():
            parent = parent_by_node.get(broker_id)
            topology.append((broker_id, port, parent))
        return topology

    async def run(
        self,
        message_count: int,
        payload_size: int,
        qos: int,
        send_interval: float,
        total_duration: Optional[float] = None,
        sample_interval: float = 5.0,
    ) -> MeasurementSummary:
        path = await self._plan_route()
        measurement_topology = self._build_topology_from_path(path)
        measurement_runner = TraditionalMQTTBenchmark(
            base_port=self.base_port,
            message_topic=self.topic,
            topology=measurement_topology,
            scenario_name="DRA-MQTT",
            source_broker=self.source_broker,
            target_broker=self.target_broker,
        )
        if total_duration and total_duration > 0 and hasattr(measurement_runner, "run_burst"):
            result = await measurement_runner.run_burst(
                payload_size=payload_size,
                qos=qos,
                send_interval=send_interval,
                total_duration=total_duration,
                sample_interval=sample_interval,
            )
        else:
            # run synchronously in executor to avoid blocking event loop
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: measurement_runner.run(
                    message_count=message_count,
                    payload_size=payload_size,
                    qos=qos,
                    send_interval=send_interval,
                    total_duration=total_duration,
                ),
            )
        result.convergence = self.convergence_history
        return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare traditional distributed MQTT vs DRA_MQTT.")
    parser.add_argument("--messages", type=int, default=1200, help="Number of messages per scenario.")
    parser.add_argument("--payload-bytes", type=int, default=512, help="Payload size per message.")
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=1, help="QoS level for traditional MQTT.")
    parser.add_argument("--send-interval", type=float, default=0.05, help="Delay between publishes (seconds).")
    parser.add_argument("--output", type=Path, help="Optional path to write JSON results.")
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Disable plotting; by default charts open automatically (requires matplotlib/TkAgg).",
    )
    return parser.parse_args()


def print_summary(result: MeasurementSummary):
    print(f"\n=== {result.scenario} Results ===")
    print(f"Messages sent: {result.message_count}")
    print(f"Messages received: {result.successful_messages}")
    print(f"Average latency: {result.average_latency_ms:.3f} ms")
    print(f"Median latency: {result.median_latency_ms:.3f} ms")
    print(f"Throughput: {result.throughput_messages_per_sec:.3f} messages/sec")


def _compute_throughput_series(latencies_ms: List[float]) -> List[float]:
    """Convert per-message latency samples into a throughput trend (msg/sec)."""
    throughput = []
    cumulative_time = 0.0
    for idx, latency_ms in enumerate(latencies_ms, start=1):
        cumulative_time += latency_ms / 1000.0
        throughput.append(idx / cumulative_time if cumulative_time > 0 else 0.0)
    return throughput


def plot_results(traditional: MeasurementSummary, dra: MeasurementSummary):
    if plt is None:
        print("matplotlib/TkAgg 未可用，无法绘图。请运行 `pip install matplotlib`。")
        return

    def _sanitize(values: List[float]) -> List[float]:
        return [value if math.isfinite(value) else float("nan") for value in values]

    def _marker_stride(length: int) -> int:
        # 完全按照five test.py的取点方式：markevery=max(1, len(ts) // 10)
        # 但要确保只显示10个标记点
        # 对于1200个数据点：要显示10个标记点，应该是每120个点一个标记
        # 所以：markevery = length // 10
        return max(1, length // 10)

    def _plot_convergence(history: Optional[ConvergenceHistory]):
        if not history or not history.iterations:
            return
        selected_nodes = history.tracked_nodes or list(history.downstream_costs.keys())
        selected_nodes = selected_nodes[:4]

        # 单独弹窗：Jj
        fig_j, ax_j = plt.subplots(figsize=(9, 6))
        for node in selected_nodes:
            ax_j.plot(
                history.iterations,
                _sanitize(history.downstream_costs.get(node, [])),
                label=f"J_{node}",
            )
        ax_j.set_title("Jj Convergence")
        ax_j.set_xlabel("Iteration")
        ax_j.set_ylabel("Downstream Cost")
        ax_j.grid(True, linestyle="--", alpha=0.3)
        ax_j.legend()
        plt.tight_layout()
        plt.show(block=True)

        # 单独弹窗：Tij
        fig_t, ax_t = plt.subplots(figsize=(9, 6))
        for node in selected_nodes:
            ax_t.plot(
                history.iterations,
                _sanitize(history.edge_costs.get(node, [])),
                label=f"T_{node}",
            )
        ax_t.set_title("Tij Convergence")
        ax_t.set_xlabel("Iteration")
        ax_t.set_ylabel("Edge Cost (f+downstream)")
        ax_t.grid(True, linestyle="--", alpha=0.3)
        ax_t.legend()
        plt.tight_layout()
        plt.show(block=True)

        # 单独弹窗：全局 cost vs 误差
        fig_gc, ax_gc = plt.subplots(figsize=(9, 6))
        ax_gc.plot(
            history.iterations,
            _sanitize(history.global_cost),
            label="Global Cost",
            color="tab:blue",
        )
        ax_err = ax_gc.twinx()
        ax_err.plot(
            history.iterations,
            _sanitize(history.error_norm),
            label="‖J(t)-J*‖",
            color="tab:red",
            linestyle="--",
        )
        ax_gc.set_title("Global Cost and Error Convergence")
        ax_gc.set_xlabel("Iteration")
        ax_gc.set_ylabel("Global Cost")
        ax_err.set_ylabel("Error Norm")
        ax_gc.grid(True, linestyle="--", alpha=0.3)
        lines, labels = ax_gc.get_legend_handles_labels()
        lines2, labels2 = ax_err.get_legend_handles_labels()
        ax_gc.legend(lines + lines2, labels + labels2, loc="upper right")
        plt.tight_layout()
        plt.show(block=True)

    series_names = ("D-MQTT", "DRA-MQTT")
    
    # 准备时间戳数据（归一化，从0开始）
    def normalize_timestamps(ts_list):
        if not ts_list:
            return []
        ts_array = np.array(ts_list)
        if len(ts_array) > 0:
            return ts_array - ts_array[0]  # 归一化，从0开始
        return ts_array
    
    trad_timestamps = normalize_timestamps(traditional.timestamps if traditional.timestamps else [])
    dra_timestamps = normalize_timestamps(dra.timestamps if dra.timestamps else [])
    
    # 如果时间戳为空，使用消息序号作为时间（假设均匀分布）
    if len(trad_timestamps) == 0:
        trad_timestamps = np.array(range(len(traditional.latencies_ms)))
    if len(dra_timestamps) == 0:
        dra_timestamps = np.array(range(len(dra.latencies_ms)))
    
    # 每5秒采样一次，总共60秒，得到12个数据点
    def sample_data(timestamps, values, sample_interval=5.0, total_duration=60.0):
        """
        每5秒统计一次窗口平均延迟（论文标准做法）
        例如：0-5s内的所有消息延迟取平均值作为t=5s的点
        """
        if len(timestamps) == 0 or len(values) == 0:
            return np.array([]), np.array([])
        
        # 转换为numpy数组（确保可以使用布尔索引）
        timestamps = np.array(timestamps)
        values = np.array(values)
        
        # 获取实际数据的时间范围
        actual_max_time = float(np.max(timestamps))
        
        # 根据实际数据范围确定采样时长
        actual_sample_duration = min(actual_max_time, total_duration)
        
        # 生成采样时间点（窗口中心时间）：2.5, 7.5, 12.5, ...（每个窗口的中心）
        # 也可以用窗口起始时间：0, 5, 10, 15, ...
        sample_times = np.arange(sample_interval, actual_sample_duration + sample_interval, sample_interval)
        
        sampled_timestamps = []
        sampled_values = []
        
        for sample_time in sample_times:
            window_start = sample_time - sample_interval
            window_end = sample_time
            
            # 找出落在 [window_start, window_end) 时间窗口内的所有消息
            window_mask = (timestamps >= window_start) & (timestamps < window_end)
            window_values = values[window_mask]
            
            if len(window_values) > 0:
                # 取该窗口内所有消息延迟的平均值
                sampled_timestamps.append(sample_time)
                sampled_values.append(np.mean(window_values))
        
        return np.array(sampled_timestamps), np.array(sampled_values)
    
    # 对数据进行采样：每5秒一次，共12个点
    trad_ts_sampled, trad_lat_sampled = sample_data(trad_timestamps, traditional.latencies_ms)
    dra_ts_sampled, dra_lat_sampled = sample_data(dra_timestamps, dra.latencies_ms)
    
    # 对于12个数据点，要显示10个标记点，使用 five test.py 的方式
    # markevery = max(1, len(ts) // 10) = max(1, 12 // 10) = 1
    latency_stride = _marker_stride(max(len(trad_ts_sampled), len(dra_ts_sampled)))

    # Latency comparison line chart
    fig_latency, ax_latency = plt.subplots(figsize=(9, 6))
    ax_latency.plot(
        trad_ts_sampled,
        trad_lat_sampled,
        label=series_names[0],
        marker="o",
        markevery=latency_stride,
        linewidth=2,
    )
    ax_latency.plot(
        dra_ts_sampled,
        dra_lat_sampled,
        label=series_names[1],
        marker="s",
        markevery=latency_stride,
        linewidth=2,
    )
    ax_latency.set_title("Latency Comparison")
    ax_latency.set_xlabel("time (s)")
    ax_latency.set_ylabel("Latency (ms)")
    ax_latency.grid(True, linestyle="--", alpha=0.4)
    ax_latency.legend()
    plt.tight_layout()
    plt.show(block=True)

    # Throughput comparison line chart derived from latency samples
    trad_throughput = _compute_throughput_series(traditional.latencies_ms)
    dra_throughput = _compute_throughput_series(dra.latencies_ms)
    
    # 为吞吐量使用相同的时间戳（截取到对应长度）
    trad_tp_timestamps_full = trad_timestamps[:len(trad_throughput)] if len(trad_timestamps) >= len(trad_throughput) else np.array(range(len(trad_throughput)))
    dra_tp_timestamps_full = dra_timestamps[:len(dra_throughput)] if len(dra_timestamps) >= len(dra_throughput) else np.array(range(len(dra_throughput)))
    
    # 对吞吐量数据进行采样：每5秒一次，共12个点
    trad_tp_ts_sampled, trad_tp_sampled = sample_data(trad_tp_timestamps_full, trad_throughput)
    dra_tp_ts_sampled, dra_tp_sampled = sample_data(dra_tp_timestamps_full, dra_throughput)
    
    fig_throughput, ax_throughput = plt.subplots(figsize=(9, 6))
    throughput_stride = _marker_stride(max(len(trad_tp_ts_sampled), len(dra_tp_ts_sampled)))
    ax_throughput.plot(
        trad_tp_ts_sampled,
        trad_tp_sampled,
        label=series_names[0],
        marker="o",
        markevery=throughput_stride,
        linewidth=2,
    )
    ax_throughput.plot(
        dra_tp_ts_sampled,
        dra_tp_sampled,
        label=series_names[1],
        marker="s",
        markevery=throughput_stride,
        linewidth=2,
    )
    ax_throughput.set_title("Throughput Comparison")
    ax_throughput.set_xlabel("time (s)")
    ax_throughput.set_ylabel("Throughput (msg/sec)")
    ax_throughput.grid(True, linestyle="--", alpha=0.4)
    ax_throughput.legend()
    plt.tight_layout()
    plt.show(block=True)

    _plot_convergence(dra.convergence)


async def main_async(args: argparse.Namespace):
    print("Running traditional distributed MQTT benchmark...")
    traditional_runner = TraditionalMQTTBenchmark()
    traditional_result = traditional_runner.run(
        message_count=args.messages,
        payload_size=args.payload_bytes,
        qos=args.qos,
        send_interval=args.send_interval,
    )
    print_summary(traditional_result)

    print("\nRunning DRA MQTT benchmark...")
    dra_runner = DRAExperimentBenchmark()
    dra_result = await dra_runner.run(
        message_count=args.messages,
        payload_size=args.payload_bytes,
        qos=args.qos,
        send_interval=args.send_interval,
    )
    print_summary(dra_result)

    if not args.no_plot:
        plot_results(traditional_result, dra_result)

    if args.output:
        args.output.write_text(
            json.dumps(
                {
                    "traditional": traditional_result.to_dict(),
                    "dra": dra_result.to_dict(),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    cli_args = parse_args()
    asyncio.run(main_async(cli_args))
