import subprocess
import asyncio
import re
from collections import namedtuple
import heapq
import time
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import random
import socket


# 进程管理类
class ProcessManager:
    def __init__(self):
        self.processes = {}  # 进程字典
        self.ports = set()  # 使用的端口集合
        self.start_time = time.time()  # 启动时间

    def add_process(self, broker_id, process):
        """添加新的代理进程"""
        self.processes[broker_id] = process
        self.ports.add(broker_id)  # 端口通常是代理ID
        print(f"添加代理进程: {broker_id}, PID: {process.pid}")

    def check_port(self, port):
        """检查端口是否被占用"""
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        try:
            sock.bind(('127.0.0.1', port))
            sock.close()
            return False  # 端口未被占用
        except:
            sock.close()
            return True  # 端口被占用

    async def stop_all(self):
        """停止所有代理进程"""
        print("\n正在停止所有MQTT代理进程...")

        for broker_id, process in list(self.processes.items()):
            if process and process.poll() is None:
                try:
                    print(f"正在停止代理 {broker_id}...")
                    process.terminate()

                    # 等待进程终止(最多5秒)
                    for _ in range(5):
                        if process.poll() is not None:
                            print(f"代理 {broker_id} 已正常终止")
                            break
                        await asyncio.sleep(1)

                    # 如果仍在运行，强制终止
                    if process.poll() is None:
                        print(f"代理 {broker_id} 未响应，强制终止")
                        process.kill()
                except Exception as e:
                    print(f"停止代理 {broker_id} 时出错: {e}")

        # 清理所有可能残留的Mosquitto进程
        try:
            print("清理所有残留的Mosquitto进程...")
            subprocess.run(['taskkill', '/F', '/IM', 'mosquitto.exe'],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
        except Exception as e:
            print(f"清理Mosquitto进程时出错: {e}")

        # 确认端口已释放
        for port in self.ports:
            if self.check_port(port):
                print(f"警告: 端口 {port} 仍被占用")

        print("所有代理进程已停止")

    def get_running_count(self):
        """获取正在运行的进程数量"""
        return sum(1 for p in self.processes.values()
                   if p and p.poll() is None)

    def get_runtime(self):
        """返回运行时间(秒)"""
        return time.time() - self.start_time


# 自动停止控制器
class AutoStopController:
    def __init__(self, max_runtime=300, max_topology_changes=5):
        self.start_time = time.time()
        self.max_runtime = max_runtime  # 最大运行时间(秒)
        self.max_topology_changes = max_topology_changes  # 最大拓扑变更次数
        self.topology_changes = 0  # 拓扑变更计数
        self.should_stop = False  # 是否应该停止
        self.stop_reason = ""  # 停止原因

    def check_stop_conditions(self, current_time=None):
        """检查是否应该停止"""
        if self.should_stop:
            return True

        if current_time is None:
            current_time = time.time()

        # 检查最大运行时间
        if current_time - self.start_time > self.max_runtime:
            self.should_stop = True
            self.stop_reason = f"达到最大运行时间: {self.max_runtime}秒"
            return True

        # 检查拓扑变更次数
        if self.topology_changes >= self.max_topology_changes:
            self.should_stop = True
            self.stop_reason = f"拓扑已变更 {self.topology_changes} 次"
            return True

        return False

    def record_topology_change(self):
        """记录一次拓扑变更"""
        self.topology_changes += 1
        print(f"拓扑变更次数: {self.topology_changes}/{self.max_topology_changes}")

    def get_runtime(self):
        """获取当前运行时间"""
        return time.time() - self.start_time

    def force_stop(self, reason):
        """强制停止"""
        self.should_stop = True
        self.stop_reason = reason


# 代理结构
class BrokerNode:
    def __init__(self, id, priority, address, port, rtt, config_path):
        self.id = id
        self.priority = priority
        self.address = address
        self.port = port
        self.rtt = rtt
        self.config_path = config_path
        self.load = 0  # 负载监控
        self.queue_length = 0  # 队列长度
        self.compute_power = np.random.randint(1, 100)  # 计算能力
        self.power_usage = np.random.uniform(0.1, 5.0)  # 功耗
        self.reliability = np.random.uniform(0.8, 1.0)  # 可靠性
        self.is_alive = True  # 代理存活状态


# 生成随机RTT数值
def generate_random_rtt(brokers, root_broker):
    """为所有代理生成随机RTT数值"""
    rtt_dict = {}

    # 为每个代理生成到其他代理的RTT值
    for broker in brokers:
        if root_broker and broker.id != root_broker.id:
            # 生成1-50ms之间的随机RTT值，模拟真实网络环境
            rtt_value = random.uniform(1.0, 50.0)
            rtt_dict[broker.id] = rtt_value
            print(f"生成代理 {broker.id} 的随机RTT: {rtt_value:.2f} ms")

    return rtt_dict


# 测量所有代理的RTT（使用随机生成）
async def measure_broker_rtts(brokers, root_broker):
    """使用随机生成的RTT数值"""
    try:
        rtt_dict = generate_random_rtt(brokers, root_broker)
        return rtt_dict

    except Exception as e:
        print(f"生成RTT数值时出错: {e}")
        return {}


# 生成基于RTT的网络图
def generate_realistic_graph(brokers, rtt_dict):
    """基于随机RTT数值生成网络图"""
    G = nx.Graph()

    # 添加所有节点
    for broker in brokers:
        G.add_node(broker.id,
                   compute_power=broker.compute_power,
                   power_usage=broker.power_usage,
                   reliability=broker.reliability)

    # 添加边 - 基于随机RTT数值
    for i, broker_i in enumerate(brokers):
        for j, broker_j in enumerate(brokers):
            if i != j:
                # 使用生成的RTT作为边权重，如果没有生成值则使用默认值
                rtt_value = rtt_dict.get(broker_j.id, 10)  # 默认10ms
                G.add_edge(broker_i.id, broker_j.id, weight=rtt_value)

    # 确保图是连通的
    if not nx.is_connected(G) and len(G.nodes()) > 1:
        print("图不是连通的，添加必要的边以确保连通性")
        components = list(nx.connected_components(G))
        for i in range(len(components) - 1):
            # 连接不同连通分量中的节点
            node1 = next(iter(components[i]))
            node2 = next(iter(components[i + 1]))
            G.add_edge(node1, node2, weight=15)  # 使用较大的权重

    # 计算并设置度数
    for node in G.nodes():
        G.nodes[node]["degree"] = G.degree(node)

    return G


# 可视化网络
def visualize_graph(G, best_node, stp_tree=None):
    pos = nx.spring_layout(G, seed=42)  # 使用固定seed确保布局一致性
    node_colors = ["red" if node == best_node else "skyblue" for node in G.nodes()]

    plt.figure(figsize=(12, 10))
    nx.draw(G, pos, with_labels=True, node_color=node_colors,
            node_size=800, edge_color="gray", width=2.0,
            font_size=12, font_weight='bold')

    # 添加边标签（RTT）
    edge_labels = nx.get_edge_attributes(G, "weight")
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=10)

    plt.title(f"MQTT Broker Network - Root Node: {best_node}", fontsize=16)
    plt.savefig("broker_network.png", dpi=300)
    plt.close()

    # 只有当stp_tree存在时才创建树形图
    if stp_tree:
        # 创建并可视化生成树
        T = nx.Graph()
        for broker_id in G.nodes():
            T.add_node(broker_id)

        # 将生成树拓扑添加到图
        for parent, children in stp_tree.items():
            for child in children:
                T.add_edge(parent, child)

        # 使用层次布局可视化树结构
        plt.figure(figsize=(12, 10))
        try:
            # 尝试使用graphviz布局，如果不可用则使用普通布局
            import pygraphviz
            pos_tree = nx.nx_agraph.graphviz_layout(T, prog="dot", root=best_node)
        except (ImportError, Exception):
            # 如果graphviz不可用，使用spring布局
            print("Graphviz不可用，使用spring布局代替")
            pos_tree = nx.spring_layout(T, seed=42)

        nx.draw(T, pos_tree, with_labels=True,
                node_color=["red" if node == best_node else "lightgreen" for node in T.nodes()],
                node_size=800, arrows=True, arrowsize=20,
                font_size=12, font_weight='bold')

        plt.title(f"MQTT Broker Spanning Tree - Root: {best_node}", fontsize=16)
        plt.savefig("broker_tree.png", dpi=300)
        plt.close()


# 使用优先级选择最佳根代理
def elect_best_root_broker(brokers):
    """根据优先级选择最佳根代理（优先级数值最小的为根）"""
    if not brokers:
        return None

    # 选择优先级数值最小的代理作为根
    best_broker = min(brokers, key=lambda b: b.priority)
    print(f"根据优先级选择的根代理: {best_broker.id}, 优先级: {best_broker.priority}")
    return best_broker


# 使用Dijkstra算法基于RTT构建生成树
def generate_stp_tree(G, root_broker):
    """使用Dijkstra算法基于RTT构建生成树"""
    stp_tree = {node: [] for node in G.nodes()}

    if root_broker is None or root_broker.id not in G.nodes():
        print("错误: 根代理不在图中")
        return stp_tree

    try:
        # 使用单源最短路径算法
        predecessors, _ = nx.dijkstra_predecessor_and_distance(G, root_broker.id, weight='weight')

        # 构建生成树
        for node in G.nodes():
            if node != root_broker.id and node in predecessors:
                # 每个节点连接到其直接前驱
                pred_list = predecessors[node]
                if pred_list:  # 确保有前驱节点
                    direct_parent = pred_list[0]  # 取第一个前驱作为直接父节点
                    if direct_parent not in stp_tree:
                        stp_tree[direct_parent] = []
                    if node not in stp_tree[direct_parent]:
                        stp_tree[direct_parent].append(node)

        print(f"基于RTT生成的STP树: {stp_tree}")
        return stp_tree

    except Exception as e:
        print(f"生成STP树时出错: {e}")
        # 回退到星型拓扑
        for node in G.nodes():
            if node != root_broker.id:
                stp_tree[root_broker.id].append(node)
        print(f"使用星型拓扑作为回退: {stp_tree}")
        return stp_tree


# 拓扑管理器
class TopologyManager:
    def __init__(self, auto_stop_controller):
        self.brokers = []
        self.current_root = None
        self.stp_tree = {}
        self.G = None
        self.auto_stop_controller = auto_stop_controller
        self.process_manager = ProcessManager()

    async def initial_setup(self, brokers):
        """初始设置"""
        self.brokers = brokers.copy()

        # 生成RTT数值
        print("\n开始生成RTT数值...")
        rtt_results = await measure_broker_rtts(self.brokers, None)

        # 选择根代理
        self.current_root = elect_best_root_broker(self.brokers)

        # 生成网络图
        self.G = generate_realistic_graph(self.brokers, rtt_results)

        # 生成STP树
        self.stp_tree = generate_stp_tree(self.G, self.current_root)

        # 可视化
        visualize_graph(self.G, self.current_root.id, self.stp_tree)

        # 配置并启动代理
        await self.configure_and_start_brokers()

        print("初始设置完成")
        return self.G, self.stp_tree

    async def handle_topology_change(self, new_brokers):
        """处理拓扑变化：代理加入或离开"""
        old_brokers = self.brokers.copy()
        added, removed = self.detect_broker_changes(old_brokers, new_brokers)

        if added or removed:
            print(f"拓扑变化检测: 新增 {len(added)} 个代理, 移除 {len(removed)} 个代理")
            self.auto_stop_controller.record_topology_change()

            # 重新生成RTT数值
            rtt_results = await measure_broker_rtts(new_brokers, self.current_root)

            # 重新生成网络图
            self.G = generate_realistic_graph(new_brokers, rtt_results)

            # 检查当前根代理是否仍然存在
            current_root_exists = any(broker.id == self.current_root.id for broker in new_brokers)
            if not current_root_exists:
                # 重新选举根代理
                self.current_root = elect_best_root_broker(new_brokers)
                print(f"根代理已变更: {self.current_root.id}")
            else:
                # 更新当前根代理对象
                self.current_root = next(broker for broker in new_brokers if broker.id == self.current_root.id)

            # 重新生成STP树
            self.stp_tree = generate_stp_tree(self.G, self.current_root)

            # 重新配置代理
            self.brokers = new_brokers
            await self.reconfigure_brokers()

            # 可视化新拓扑
            visualize_graph(self.G, self.current_root.id, self.stp_tree)

            return True
        return False

    def detect_broker_changes(self, old_brokers, new_brokers):
        """检测代理变化"""
        old_broker_ids = {broker.id for broker in old_brokers}
        new_broker_ids = {broker.id for broker in new_brokers}

        added_brokers = [broker for broker in new_brokers if broker.id not in old_broker_ids]
        removed_brokers = [broker for broker in old_brokers if broker.id not in new_broker_ids]

        return added_brokers, removed_brokers

    async def configure_and_start_brokers(self):
        """配置并启动所有代理"""
        print("配置并启动所有代理...")

        for broker in self.brokers:
            config = self.generate_bridge_config(broker, self.current_root, self.stp_tree)
            self.write_config_to_file(config, broker.config_path)

            process = self.start_broker(broker.id, broker.config_path)
            if process:
                self.process_manager.add_process(broker.id, process)

            await asyncio.sleep(1)  # 间隔启动

        print("所有代理启动完成")
        await asyncio.sleep(3)  # 等待代理稳定

    async def reconfigure_brokers(self):
        """改进的重新配置方法，避免重复启动"""
        print("重新配置所有代理...")

        # 先停止所有运行中的代理
        for broker_id, process in list(self.process_manager.processes.items()):
            if process and process.poll() is None:
                try:
                    process.terminate()
                    await asyncio.sleep(1)
                    if process.poll() is None:
                        process.kill()
                except Exception as e:
                    print(f"停止代理 {broker_id} 时出错: {e}")

        # 清空进程字典
        self.process_manager.processes.clear()
        await asyncio.sleep(2)  # 确保所有进程都已停止

        # 重新配置并启动
        await self.configure_and_start_brokers()

    def generate_bridge_config(self, broker, root_broker, stp_tree):
        """根据STP树生成桥接配置"""
        config = {
            'port': broker.port,
            'allow_anonymous': True,
            'persistence': True,
            'log_type': 'all'
        }

        bridge_configs = []

        # 如果当前代理在STP树中有子节点，配置到子节点的连接
        children = stp_tree.get(broker.id, [])
        for child_id in children:
            child_broker = next((b for b in self.brokers if b.id == child_id), None)
            if child_broker:
                bridge_config = {
                    'connection': f'bridge_{broker.id}_to_{child_id}',
                    'address': f'{child_broker.address}:{child_broker.port}',
                    'topic': '# out',  # 只发送消息到子节点
                    'bridge_protocol_version': 'mqttv311',
                    'cleansession': True,
                    'try_private': True
                }
                bridge_configs.append(bridge_config)

        # 如果不是根节点，需要连接到父节点
        if broker.id != root_broker.id:
            # 找到父节点
            parent_id = None
            for parent, children in stp_tree.items():
                if broker.id in children:
                    parent_id = parent
                    break

            if parent_id:
                parent_broker = next((b for b in self.brokers if b.id == parent_id), None)
                if parent_broker:
                    bridge_config = {
                        'connection': f'bridge_{broker.id}_to_{parent_id}',
                        'address': f'{parent_broker.address}:{parent_broker.port}',
                        'topic': '# in',  # 只从父节点接收消息
                        'bridge_protocol_version': 'mqttv311',
                        'cleansession': True,
                        'try_private': True
                    }
                    bridge_configs.append(bridge_config)

        config['bridges'] = bridge_configs
        return config

    def write_config_to_file(self, config, config_path):
        """将配置写入文件"""
        try:
            with open(config_path, 'w') as f:
                f.write(f"port {config['port']}\n")
                f.write(f"allow_anonymous {str(config['allow_anonymous']).lower()}\n")
                f.write(f"persistence {str(config['persistence']).lower()}\n")
                f.write(f"log_type {config['log_type']}\n\n")

                if 'bridges' in config:
                    f.write(f"# Bridge configurations\n")
                    for bridge in config['bridges']:
                        f.write(f"connection {bridge['connection']}\n")
                        f.write(f"address {bridge['address']}\n")
                        f.write(f"topic {bridge['topic']} 2\n")
                        f.write(f"bridge_protocol_version {bridge['bridge_protocol_version']}\n")
                        f.write(f"cleansession {str(bridge['cleansession']).lower()}\n")
                        f.write(f"try_private {str(bridge['try_private']).lower()}\n\n")

            print(f"配置已写入 {config_path}")
        except Exception as e:
            print(f"写入配置到 {config_path} 时出错: {e}")
            raise

    def start_broker(self, broker_id, config_path):
        """启动代理"""
        print(f"启动代理 {broker_id}，配置文件: {config_path}")
        try:
            process = subprocess.Popen(
                [r"C:\\Program Files\\mosquitto\\mosquitto.exe", "-c", config_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )

            if process.poll() is None:
                print(f"代理 {broker_id} 启动成功，PID: {process.pid}")
            else:
                stdout, stderr = process.communicate()
                print(f"代理 {broker_id} 启动失败:")
                print(f"stdout: {stdout}")
                print(f"stderr: {stderr}")
                return None

            return process
        except Exception as e:
            print(f"启动代理 {broker_id} 时出错: {e}")
            return None

    async def monitor_broker_health(self):
        """监控代理健康状态"""
        print("开始代理健康监控...")

        while not self.auto_stop_controller.check_stop_conditions():
            try:
                dead_brokers = []

                for broker in self.brokers:
                    if not await self.check_broker_alive(broker):
                        print(f"代理 {broker.id} 故障!")
                        broker.is_alive = False
                        dead_brokers.append(broker)
                    else:
                        broker.is_alive = True

                if dead_brokers:
                    # 移除故障代理并重建拓扑
                    active_brokers = [b for b in self.brokers if b.is_alive]
                    await self.handle_topology_change(active_brokers)

                await asyncio.sleep(10)  # 每10秒检查一次

            except Exception as e:
                print(f"健康监控错误: {e}")
                await asyncio.sleep(10)

    async def check_broker_alive(self, broker, retries=3):
        """改进的代理存活检查，带重试机制"""
        for attempt in range(retries):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)  # 增加超时时间
                result = sock.connect_ex((broker.address, broker.port))
                sock.close()

                if result == 0:
                    return True
                else:
                    print(f"代理 {broker.id} 连接失败，尝试 {attempt + 1}/{retries}")
                    await asyncio.sleep(1)  # 重试前等待

            except Exception as e:
                print(f"检查代理 {broker.id} 时出错: {e}")
                await asyncio.sleep(1)

        return False

    async def add_new_broker(self, new_broker):
        """添加新代理到网络"""
        print(f"新代理加入: {new_broker.id}")

        # 添加到代理列表
        new_brokers = self.brokers + [new_broker]

        # 处理拓扑变化
        await self.handle_topology_change(new_brokers)

        print(f"新代理 {new_broker.id} 已成功集成到网络")

    async def remove_broker(self, broker_id):
        """从网络中移除代理"""
        print(f"移除代理: {broker_id}")

        # 从代理列表中移除
        new_brokers = [b for b in self.brokers if b.id != broker_id]

        # 处理拓扑变化
        await self.handle_topology_change(new_brokers)

        print(f"代理 {broker_id} 已从网络中移除")

    async def cleanup(self):
        """清理资源"""
        await self.process_manager.stop_all()


# 收集性能指标
def collect_performance_metrics(brokers, root_broker, stp_tree, G):
    """收集性能指标和统计数据"""
    metrics = {
        "broker_count": len(brokers),
        "root_broker": root_broker.id,
        "root_priority": root_broker.priority,
        "timestamp": time.time()
    }

    # 计算树高度
    def calculate_tree_height(node, tree, visited=None):
        if visited is None:
            visited = set()

        if node in visited:
            return 0

        visited.add(node)

        if not tree[node]:
            return 0

        heights = [calculate_tree_height(child, tree, visited) for child in tree[node]]
        return 1 + max(heights) if heights else 0

    tree_height = calculate_tree_height(root_broker.id, stp_tree)
    metrics["tree_height"] = tree_height

    # 计算根节点度数
    metrics["root_degree"] = len(stp_tree[root_broker.id])

    # 计算平均节点度数
    total_degree = sum(len(children) for children in stp_tree.values())
    metrics["avg_node_degree"] = total_degree / len(brokers) if brokers else 0

    # 使用NetworkX计算平均最短路径长度
    if nx.is_connected(G):
        avg_path_length = nx.average_shortest_path_length(G, weight='weight')
        metrics["avg_distance"] = avg_path_length

        # 计算最大距离
        max_distance = 0
        for u in G.nodes():
            for v in G.nodes():
                if u != v:
                    try:
                        path_length = nx.shortest_path_length(G, u, v, weight='weight')
                        max_distance = max(max_distance, path_length)
                    except nx.NetworkXNoPath:
                        pass
        metrics["max_distance"] = max_distance
    else:
        metrics["avg_distance"] = float('inf')
        metrics["max_distance"] = float('inf')
        print("图不是连通的，无法计算平均最短路径长度")

    # 打印指标
    print("\n性能指标:")
    for key, value in metrics.items():
        print(f"{key}: {value}")

    return metrics


async def setup_brokers():
    """设置代理"""
    try:
        # 初始化自动停止控制器
        auto_stop_controller = AutoStopController(
            max_runtime=300,  # 5分钟最大运行时间
            max_topology_changes=5  # 最多5次拓扑变更
        )

        # 初始化拓扑管理器
        topology_manager = TopologyManager(auto_stop_controller)

        # 初始化代理节点 - 优先级从1到5
        brokers = [
            BrokerNode(id=1884, priority=1, address="localhost", port=1884, rtt=0,
                       config_path=r"D:\mosquitto-configs\mosquitto_broker1.conf"),
            BrokerNode(id=1885, priority=2, address="localhost", port=1885, rtt=0,
                       config_path=r"D:\mosquitto-configs\mosquitto_broker2.conf"),
            BrokerNode(id=1886, priority=3, address="localhost", port=1886, rtt=0,
                       config_path=r"D:\mosquitto-configs\mosquitto_broker3.conf"),
            BrokerNode(id=1887, priority=4, address="localhost", port=1887, rtt=0,
                       config_path=r"D:\mosquitto-configs\mosquitto_broker4.conf"),
            BrokerNode(id=1888, priority=5, address="localhost", port=1888, rtt=0,
                       config_path=r"D:\mosquitto-configs\mosquitto_broker5.conf"),
        ]

        print("代理节点初始化完成，优先级分配如下:")
        for broker in brokers:
            print(f"代理 {broker.id}: 优先级 {broker.priority}")

        # 初始设置
        G, stp_tree = await topology_manager.initial_setup(brokers)

        # 收集初始性能指标
        initial_metrics = collect_performance_metrics(brokers, topology_manager.current_root, stp_tree, G)

        # 启动健康监控
        health_task = asyncio.create_task(topology_manager.monitor_broker_health())

        # 模拟网络动态变化
        print("\n开始模拟网络动态变化...")

        # 等待一段时间后添加新代理
        await asyncio.sleep(20)

        # 添加新代理
        new_broker = BrokerNode(
            id=1889, priority=6, address="localhost", port=1889, rtt=0,
            config_path=r"D:\mosquitto-configs\mosquitto_broker6.conf"
        )
        await topology_manager.add_new_broker(new_broker)

        # 收集新拓扑的性能指标
        new_metrics = collect_performance_metrics(
            topology_manager.brokers,
            topology_manager.current_root,
            topology_manager.stp_tree,
            topology_manager.G
        )

        # 等待一段时间后移除代理
        await asyncio.sleep(20)

        # 移除一个代理
        await topology_manager.remove_broker(1886)  # 移除代理1886

        # 收集最终性能指标
        final_metrics = collect_performance_metrics(
            topology_manager.brokers,
            topology_manager.current_root,
            topology_manager.stp_tree,
            topology_manager.G
        )

        # 等待自动停止条件触发
        while not auto_stop_controller.check_stop_conditions():
            await asyncio.sleep(5)

        print(f"\n自动停止: {auto_stop_controller.stop_reason}")

        # 取消健康监控任务
        health_task.cancel()
        try:
            await health_task
        except asyncio.CancelledError:
            pass

        # 清理资源
        await topology_manager.cleanup()

        print("\n运行完成!")
        print(f"总运行时间: {auto_stop_controller.get_runtime():.1f}秒")
        print(f"拓扑变更次数: {auto_stop_controller.topology_changes}")

    except Exception as e:
        print(f"setup_brokers中出错: {e}")
        import traceback
        traceback.print_exc()
        # 确保在出错时也能停止代理
        if 'topology_manager' in locals():
            await topology_manager.cleanup()
        raise


def main():
    """主函数"""
    try:
        print("启动MQTT-STP网络设置过程!")
        asyncio.run(setup_brokers())
        print("MQTT-STP网络设置成功完成!")
    except KeyboardInterrupt:
        print("\n用户中断，关闭中...")
    except Exception as e:
        print(f"主进程中出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()