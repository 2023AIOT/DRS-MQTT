import paho.mqtt.client as mqtt
import json
import time
import threading
from collections import defaultdict
import logging
import socket
import struct
import random
import heapq

class TopoMQTT:
    def __init__(self, broker_host="localhost", broker_port=1883, node_id=None):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.node_id = node_id or f"node_{int(time.time())}"
        self.client = mqtt.Client(client_id=self.node_id)
        
        # 初始化logger
        self.logger = logging.getLogger(f"TopoMQTT-{self.node_id}")
        self.logger.setLevel(logging.INFO)
        
        # 创建文件处理器
        file_handler = logging.FileHandler(f'topomqtt_{self.node_id}.log')
        file_handler.setLevel(logging.INFO)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 创建格式化器
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器到logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # 主题树结构
        self.topic_trees = defaultdict(dict)
        # 路由表
        self.routing_table = {}
        # 节点状态
        self.node_status = {}
        # 主题订阅关系
        self.topic_subscriptions = defaultdict(set)
        # Broker发现相关
        self.broker_list = set()
        self.broker_rtt = {}
        self.broker_resources = {}
        self.overlay_tree = {}
        self.parent_broker = None
        
        # 性能监控指标
        self.performance_metrics = {
            'message_count': 0,
            'message_latency': [],
            'broker_load': {},
            'routing_updates': 0,
            'topology_changes': 0,
            'start_time': time.time()
        }
        
        # 设置回调函数
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
        # 启动Broker发现线程
        self.discovery_thread = threading.Thread(target=self.broker_discovery_loop)
        self.discovery_thread.daemon = True
        self.discovery_thread.start()
        
        # 添加订阅路由表(PRT)和发布路由表(SRT)
        self.subscription_routing_table = {}  # PRT
        self.publication_routing_table = {}   # SRT
        
        # 订阅广告主题
        self.client.subscribe("$PUBADV/#")
        
    def connect(self):
        """连接到MQTT代理服务器"""
        try:
            self.client.connect(self.broker_host, self.broker_port)
            self.client.loop_start()
            self.logger.info(f"节点 {self.node_id} 已连接到代理服务器")
        except Exception as e:
            self.logger.error(f"连接失败: {str(e)}")
            
    def on_connect(self, client, userdata, flags, rc):
        """连接回调函数"""
        if rc == 0:
            self.logger.info("连接成功")
            # 订阅系统主题
            self.client.subscribe(f"$SYS/nodes/#")
            self.client.subscribe(f"$SYS/topics/#")
            # 发布节点上线消息
            self.publish_node_status("online")
        else:
            self.logger.error(f"连接失败，返回码: {rc}")
            
    def on_message(self, client, userdata, msg):
        """消息处理回调函数（扩展）"""
        try:
            start_time = time.time()
            topic = msg.topic
            payload = msg.payload.decode()
            
            if topic.startswith("$SYS/"):
                self.handle_system_message(topic, payload)
            elif topic.startswith("$PUBADV/"):
                self.handle_advertisement(topic, payload)
            elif topic.startswith("$SUBADV/"):
                self.handle_subscription_advertisement(topic, payload)
            else:
                self.handle_application_message(topic, payload)
                
            # 计算消息处理延迟
            latency = (time.time() - start_time) * 1000
            self.update_performance_metrics('message', latency)
            
        except Exception as e:
            self.logger.error(f"消息处理错误: {str(e)}")
            
    def handle_system_message(self, topic, payload):
        """处理系统消息"""
        if topic.startswith("$SYS/nodes/"):
            self.update_node_status(payload)
        elif topic.startswith("$SYS/topics/"):
            self.update_topic_tree(payload)
        elif topic.startswith("$SYS/ping/"):
            self.handle_ping(payload)
        elif topic.startswith("$SYS/pingreq/"):
            self.handle_pingreq(topic, payload)
        elif topic.startswith("$SYS/pingresp/"):
            self.handle_pingresp(payload)
        elif topic.startswith("$SYS/resources/"):
            self.handle_resources(payload)
        elif topic == "$SYS/topology":
            self.handle_topology_update(payload)
        elif topic == "$TC":
            self.handle_topology_change(payload)
            
    def handle_application_message(self, topic, payload):
        """处理应用消息"""
        # 实现主题感知路由
        if self.should_forward_message(topic):
            self.forward_message(topic, payload)
            
    def should_forward_message(self, topic):
        """判断是否需要转发消息"""
        # 检查本地订阅
        if topic in self.topic_subscriptions:
            return True
            
        # 检查路由表
        for route_topic in self.routing_table:
            if self.topic_matches(topic, route_topic):
                return True
                
        return False
        
    def topic_matches(self, topic, pattern):
        """检查主题是否匹配"""
        topic_parts = topic.split('/')
        pattern_parts = pattern.split('/')
        
        if len(topic_parts) != len(pattern_parts):
            return False
            
        for t, p in zip(topic_parts, pattern_parts):
            if p != '+' and p != '#' and t != p:
                return False
        return True
        
    def forward_message(self, topic, payload):
        """转发消息"""
        # 获取最优路由
        optimal_route = self.get_optimal_route(topic)
        if not optimal_route:
            self.logger.warning(f"未找到路由: {topic}")
            return
            
        # 检查路由状态
        if self.is_route_available(optimal_route):
            # 使用QoS 1确保消息可靠传递
            self.client.publish(topic, payload, qos=1)
            self.logger.info(f"消息已转发: {topic}")
        else:
            # 寻找备用路由
            backup_route = self.find_backup_route(topic)
            if backup_route:
                self.client.publish(topic, payload, qos=1)
                self.logger.info(f"使用备用路由转发消息: {topic}")
            else:
                self.logger.error(f"无法转发消息: {topic}")
                
    def get_optimal_route(self, topic):
        """获取最优路由"""
        # 实现动态多树优化算法
        # 这里可以根据网络状况、负载等因素选择最优路径
        return self.routing_table.get(topic)
        
    def update_node_status(self, status_data):
        """更新节点状态"""
        try:
            status = json.loads(status_data)
            self.node_status[status['node_id']] = status
            self.optimize_topology()
        except Exception as e:
            self.logger.error(f"更新节点状态失败: {str(e)}")
            
    def update_topic_tree(self, tree_data):
        """更新主题树"""
        try:
            tree = json.loads(tree_data)
            self.topic_trees.update(tree)
            self.update_routing_table()
        except Exception as e:
            self.logger.error(f"更新主题树失败: {str(e)}")
            
    def optimize_topology(self):
        """优化拓扑结构"""
        self.logger.info("开始优化拓扑结构")
        
        # 更新路由表
        for topic, route in self.routing_table.items():
            # 计算当前路由的延迟
            current_delay = self.calculate_route_delay(route)
            
            # 寻找更优路径
            better_route = self.find_better_route(topic, current_delay)
            if better_route:
                self.routing_table[topic] = better_route
                self.logger.info(f"找到更优路由: {topic} -> {better_route}")
                
        # 更新性能指标
        self.update_performance_metrics('routing_update')
        
    def calculate_route_delay(self, route):
        """计算路由延迟"""
        if not route or 'path' not in route:
            return float('inf')
            
        total_delay = 0
        for i in range(len(route['path']) - 1):
            current = route['path'][i]
            next_hop = route['path'][i + 1]
            # 获取两节点间的RTT
            rtt = self.broker_rtt.get(current, float('inf'))
            total_delay += rtt
            
        return total_delay
        
    def find_better_route(self, topic, current_delay):
        """寻找更优路由"""
        if not self.overlay_tree:
            return None
            
        # 使用Dijkstra算法寻找最短路径
        distances = {broker: float('inf') for broker in self.broker_list}
        distances[self.node_id] = 0
        previous = {}
        unvisited = set(self.broker_list)
        unvisited.add(self.node_id)
        
        while unvisited:
            # 找到未访问节点中距离最小的
            current = min(unvisited, key=lambda x: distances[x])
            if distances[current] == float('inf'):
                break
                
            unvisited.remove(current)
            
            # 更新邻居节点的距离
            for neighbor in self.get_neighbors(current):
                if neighbor in unvisited:
                    # 计算新的延迟
                    new_delay = distances[current] + self.calculate_edge_cost(current, neighbor)
                    if new_delay < distances[neighbor]:
                        distances[neighbor] = new_delay
                        previous[neighbor] = current
                        
        # 构建新路由
        target = self.find_target_broker(topic)
        if target and target in previous:
            new_route = {
                'next_hop': None,
                'cost': distances[target],
                'path': []
            }
            
            # 如果新路由的延迟比当前路由小，则使用新路由
            if new_route['cost'] < current_delay:
                current = target
                while current in previous:
                    new_route['path'].append(current)
                    current = previous[current]
                new_route['path'].reverse()
                new_route['next_hop'] = new_route['path'][0] if new_route['path'] else None
                return new_route
                
        return None
        
    def update_routing_table(self):
        """更新路由表"""
        # 根据主题树更新路由表
        self.routing_table.clear()
        for topic, tree in self.topic_trees.items():
            self.routing_table[topic] = self.calculate_route(topic, tree)
            
    def calculate_route(self, topic, tree):
        """计算路由"""
        if not self.overlay_tree:
            return None

        # 使用Dijkstra算法计算最短路径
        distances = {broker: float('inf') for broker in self.broker_list}
        distances[self.node_id] = 0
        previous = {}
        unvisited = set(self.broker_list)
        unvisited.add(self.node_id)

        while unvisited:
            # 找到未访问节点中距离最小的
            current = min(unvisited, key=lambda x: distances[x])
            if distances[current] == float('inf'):
                break

            unvisited.remove(current)

            # 更新邻居节点的距离
            for neighbor in self.get_neighbors(current):
                if neighbor in unvisited:
                    new_distance = distances[current] + self.calculate_edge_cost(current, neighbor)
                    if new_distance < distances[neighbor]:
                        distances[neighbor] = new_distance
                        previous[neighbor] = current

        # 构建路由信息
        route = {
            'next_hop': None,
            'cost': float('inf'),
            'path': []
        }

        # 找到目标节点
        target = self.find_target_broker(topic)
        if target and target in previous:
            route['cost'] = distances[target]
            current = target
            while current in previous:
                route['path'].append(current)
                current = previous[current]
            route['path'].reverse()
            route['next_hop'] = route['path'][0] if route['path'] else None

        return route

    def get_neighbors(self, broker):
        """获取节点的邻居节点"""
        neighbors = set()
        for b, parent in self.overlay_tree.items():
            if parent == broker:
                neighbors.add(b)
        for b, parent in self.overlay_tree.items():
            if b == broker:
                neighbors.add(parent)
        return neighbors

    def find_target_broker(self, topic):
        """根据主题找到目标Broker"""
        # 首先尝试根据locality选择Broker
        target = self.simulate_locality(topic)
        if target:
            return target
        # 如果没有找到合适的Broker，随机选择一个
        return random.choice(list(self.broker_list)) if self.broker_list else None

    def handle_topology_change(self, tc_message):
        """处理拓扑变更消息"""
        try:
            tc_data = json.loads(tc_message)
            if tc_data.get('type') == 'topology_change':
                # 触发拓扑重构
                self.build_overlay_tree()
                # 更新路由表
                self.update_routing_table()
                # 通知其他节点
                self.publish_topology_update()
        except Exception as e:
            self.logger.error(f"处理拓扑变更失败: {str(e)}")

    def publish_topology_update(self):
        """发布拓扑更新消息"""
        update_data = {
            'type': 'topology_update',
            'node_id': self.node_id,
            'overlay_tree': self.overlay_tree,
            'timestamp': time.time()
        }
        self.client.publish("$SYS/topology", json.dumps(update_data), qos=1)
        
    def publish_node_status(self, status):
        """发布节点状态"""
        status_data = {
            'node_id': self.node_id,
            'status': status,
            'timestamp': time.time()
        }
        self.client.publish(f"$SYS/nodes/{self.node_id}", 
                          json.dumps(status_data),
                          qos=1)
        
    def subscribe(self, topic, qos=0):
        """订阅主题（扩展）"""
        self.client.subscribe(topic, qos)
        self.topic_subscriptions[topic].add(self.node_id)
        
        # 更新订阅路由表
        self.update_subscription_routing(topic, self.node_id)
        
        # 发布订阅广告
        self.publish_subscription_advertisement(topic)
        
        self.logger.info(f"已订阅主题: {topic}")
        
    def publish(self, topic, payload, qos=0):
        """发布消息"""
        self.client.publish(topic, payload, qos)
        self.logger.info(f"已发布消息到主题: {topic}")
        
    def on_disconnect(self, client, userdata, rc):
        """断开连接回调函数"""
        self.logger.info("与代理服务器断开连接")
        self.publish_node_status("offline")
        
    def disconnect(self):
        """断开连接"""
        self.client.loop_stop()
        self.client.disconnect()
        self.logger.info("已断开连接")

    def broker_discovery_loop(self):
        """Broker发现循环"""
        while True:
            try:
                # 发送PINGREQ到所有已知Broker
                for broker in self.broker_list:
                    self.send_pingreq(broker)
                # 等待PINGRESP
                time.sleep(5)
                # 更新RTT和资源信息
                self.update_broker_metrics()
                # 构建覆盖树
                self.build_overlay_tree()
            except Exception as e:
                self.logger.error(f"Broker发现错误: {str(e)}")
            time.sleep(30)  # 每30秒执行一次发现

    def send_pingreq(self, broker):
        """发送PINGREQ到指定Broker"""
        try:
            start_time = time.time()
            pingreq_data = {
                "timestamp": start_time,
                "sender": self.node_id
            }
            self.client.publish(f"$SYS/pingreq/{broker}", 
                              json.dumps(pingreq_data),
                              qos=1)
            self.broker_rtt[broker] = float('inf')  # 初始化为无穷大
        except Exception as e:
            self.logger.error(f"发送PINGREQ失败: {str(e)}")

    def handle_pingreq(self, topic, payload):
        """处理PINGREQ消息"""
        try:
            pingreq_data = json.loads(payload)
            if 'timestamp' in pingreq_data and 'sender' in pingreq_data:
                # 发送PINGRESP响应
                pingresp_data = {
                    "timestamp": pingreq_data['timestamp'],
                    "sender": self.node_id,
                    "responder": self.node_id
                }
                self.client.publish(f"$SYS/pingresp/{pingreq_data['sender']}", 
                                  json.dumps(pingresp_data),
                                  qos=1)
        except Exception as e:
            self.logger.error(f"处理PINGREQ消息失败: {str(e)}")

    def handle_pingresp(self, payload):
        """处理PINGRESP消息"""
        try:
            pingresp_data = json.loads(payload)
            if 'timestamp' in pingresp_data and 'responder' in pingresp_data:
                rtt = time.time() - pingresp_data['timestamp']
                responder = pingresp_data['responder']
                if responder in self.broker_rtt:
                    self.broker_rtt[responder] = rtt
                    self.logger.debug(f"更新Broker {responder}的RTT: {rtt:.3f}秒")
        except Exception as e:
            self.logger.error(f"处理PINGRESP消息失败: {str(e)}")

    def update_broker_metrics(self):
        """更新Broker指标"""
        for broker in self.broker_list:
            if broker in self.broker_rtt:
                # 获取详细的资源信息
                resources = {
                    'cpu_usage': self.get_cpu_usage(),
                    'memory_usage': self.get_memory_usage(),
                    'network_usage': self.get_network_usage(),
                    'message_queue_size': self.get_queue_size(),
                    'active_connections': self.get_active_connections(),
                    'message_throughput': self.get_message_throughput(),
                    'last_update': time.time()
                }
                self.broker_resources[broker] = resources
                self.update_performance_metrics('broker_load', resources)
                
    def get_cpu_usage(self):
        """获取CPU使用率"""
        try:
            import psutil
            return psutil.cpu_percent()
        except ImportError:
            return random.uniform(0, 100)  # 模拟数据
            
    def get_memory_usage(self):
        """获取内存使用率"""
        try:
            import psutil
            return psutil.virtual_memory().percent
        except ImportError:
            return random.uniform(0, 100)  # 模拟数据
            
    def get_network_usage(self):
        """获取网络使用率"""
        try:
            import psutil
            net_io = psutil.net_io_counters()
            return {
                'bytes_sent': net_io.bytes_sent,
                'bytes_recv': net_io.bytes_recv,
                'packets_sent': net_io.packets_sent,
                'packets_recv': net_io.packets_recv
            }
        except ImportError:
            return {
                'bytes_sent': random.randint(0, 1000000),
                'bytes_recv': random.randint(0, 1000000),
                'packets_sent': random.randint(0, 1000),
                'packets_recv': random.randint(0, 1000)
            }
            
    def get_queue_size(self):
        """获取消息队列大小"""
        return len(self.client._out_messages)
        
    def get_active_connections(self):
        """获取活动连接数"""
        return len(self.broker_list)
        
    def get_message_throughput(self):
        """获取消息吞吐量"""
        current_time = time.time()
        if not hasattr(self, '_last_throughput_check'):
            self._last_throughput_check = current_time
            self._message_count = 0
            return 0
            
        # 计算每秒消息数
        time_diff = current_time - self._last_throughput_check
        if time_diff > 0:
            throughput = self._message_count / time_diff
            self._last_throughput_check = current_time
            self._message_count = 0
            return throughput
        return 0
        
    def update_performance_metrics(self, metric_type, value=None):
        """更新性能指标"""
        if metric_type == 'message':
            self.performance_metrics['message_count'] += 1
            self._message_count += 1  # 用于计算吞吐量
            if value:  # 消息延迟
                self.performance_metrics['message_latency'].append(value)
                # 只保留最近1000个延迟记录
                if len(self.performance_metrics['message_latency']) > 1000:
                    self.performance_metrics['message_latency'].pop(0)
        elif metric_type == 'broker_load':
            self.performance_metrics['broker_load'] = value
        elif metric_type == 'routing_update':
            self.performance_metrics['routing_updates'] += 1
        elif metric_type == 'topology_change':
            self.performance_metrics['topology_changes'] += 1
            
    def get_performance_report(self):
        """获取性能报告"""
        current_time = time.time()
        runtime = current_time - self.performance_metrics['start_time']
        
        # 计算平均消息延迟
        latencies = self.performance_metrics['message_latency']
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        
        # 计算消息吞吐量
        message_rate = self.performance_metrics['message_count'] / runtime if runtime > 0 else 0
        
        # 计算资源使用情况
        resource_usage = {}
        for broker, resources in self.performance_metrics['broker_load'].items():
            resource_usage[broker] = {
                'cpu': resources.get('cpu_usage', 0),
                'memory': resources.get('memory_usage', 0),
                'network': resources.get('network_usage', {}),
                'queue_size': resources.get('message_queue_size', 0),
                'connections': resources.get('active_connections', 0),
                'throughput': resources.get('message_throughput', 0)
            }
        
        return {
            'runtime': runtime,
            'total_messages': self.performance_metrics['message_count'],
            'message_rate': message_rate,
            'average_latency': avg_latency,
            'routing_updates': self.performance_metrics['routing_updates'],
            'topology_changes': self.performance_metrics['topology_changes'],
            'resource_usage': resource_usage
        }
        
    def log_performance_metrics(self):
        """记录性能指标"""
        report = self.get_performance_report()
        self.logger.info("性能指标报告:")
        self.logger.info(f"运行时间: {report['runtime']:.2f}秒")
        self.logger.info(f"总消息数: {report['total_messages']}")
        self.logger.info(f"消息速率: {report['message_rate']:.2f}消息/秒")
        self.logger.info(f"平均延迟: {report['average_latency']:.2f}毫秒")
        self.logger.info(f"路由更新次数: {report['routing_updates']}")
        self.logger.info(f"拓扑变更次数: {report['topology_changes']}")
        self.logger.info("Broker负载:")
        for broker, load in report['resource_usage'].items():
            self.logger.info(f"  {broker}: {load}")

    def is_route_available(self, route):
        """检查路由是否可用"""
        if not route or 'path' not in route:
            return False
            
        # 检查路径上的所有节点是否在线
        for node in route['path']:
            if node not in self.broker_list:
                return False
                
        return True
        
    def find_backup_route(self, topic):
        """寻找备用路由"""
        # 使用备用策略：选择负载最低的可用Broker
        available_brokers = [b for b in self.broker_list if b in self.broker_resources]
        if not available_brokers:
            return None
            
        # 选择负载最低的Broker
        target_broker = min(
            available_brokers,
            key=lambda b: self.broker_resources[b]['cpu_usage']
        )
        
        # 构建备用路由
        return {
            'next_hop': target_broker,
            'cost': self.broker_rtt.get(target_broker, float('inf')),
            'path': [target_broker]
        }

    def build_overlay_tree(self):
        """构建覆盖树"""
        # 使用Prim算法构建最小生成树
        if not self.broker_list:
            return

        # 初始化
        visited = set()
        edges = []
        start_broker = list(self.broker_list)[0]
        visited.add(start_broker)
        
        # 构建边集合
        for broker in self.broker_list:
            if broker != start_broker:
                cost = self.calculate_edge_cost(start_broker, broker)
                heapq.heappush(edges, (cost, start_broker, broker))

        # Prim算法
        while edges and len(visited) < len(self.broker_list):
            cost, u, v = heapq.heappop(edges)
            if v not in visited:
                visited.add(v)
                self.overlay_tree[v] = u
                
                # 添加新边
                for broker in self.broker_list:
                    if broker not in visited:
                        cost = self.calculate_edge_cost(v, broker)
                        heapq.heappush(edges, (cost, v, broker))

        # 更新路由表
        self.update_routing_table()

    def calculate_edge_cost(self, broker1, broker2):
        """计算两个Broker之间的边代价"""
        rtt1 = self.broker_rtt.get(broker1, float('inf'))
        rtt2 = self.broker_rtt.get(broker2, float('inf'))
        resources1 = self.broker_resources.get(broker1, 0)
        resources2 = self.broker_resources.get(broker2, 0)
        
        # 综合RTT和资源计算代价
        rtt_cost = (rtt1 + rtt2) / 2
        resource_cost = 1 / (resources1 + resources2 + 1)  # 避免除零
        
        return rtt_cost * 0.7 + resource_cost * 0.3  # 权重可调整

    def handle_ping(self, payload):
        """处理PING消息"""
        try:
            ping_data = json.loads(payload)
            if 'timestamp' in ping_data:
                rtt = time.time() - ping_data['timestamp']
                sender = ping_data.get('sender')
                if sender:
                    self.broker_rtt[sender] = rtt
        except Exception as e:
            self.logger.error(f"处理PING消息失败: {str(e)}")

    def handle_resources(self, payload):
        """处理资源信息"""
        try:
            resource_data = json.loads(payload)
            if 'broker_id' in resource_data and 'resources' in resource_data:
                self.broker_resources[resource_data['broker_id']] = resource_data['resources']
        except Exception as e:
            self.logger.error(f"处理资源信息失败: {str(e)}")

    def handle_topology_update(self, payload):
        """处理拓扑更新消息"""
        try:
            update_data = json.loads(payload)
            if update_data.get('type') == 'topology_update':
                # 更新覆盖树
                self.overlay_tree.update(update_data.get('overlay_tree', {}))
                # 更新路由表
                self.update_routing_table()
        except Exception as e:
            self.logger.error(f"处理拓扑更新失败: {str(e)}")

    def detect_failure(self):
        """检测Broker故障"""
        current_time = time.time()
        failed_brokers = []
        
        for broker in self.broker_list:
            last_seen = self.broker_resources.get(broker, {}).get('last_update', 0)
            if current_time - last_seen > self.failure_timeout:
                failed_brokers.append(broker)
                self.logger.warning(f"检测到Broker故障: {broker}")
                
        if failed_brokers:
            self.handle_broker_failure(failed_brokers)
            
    def handle_broker_failure(self, failed_brokers):
        """处理Broker故障"""
        for broker in failed_brokers:
            # 从broker列表中移除
            if broker in self.broker_list:
                self.broker_list.remove(broker)
                
            # 从资源信息中移除
            if broker in self.broker_resources:
                del self.broker_resources[broker]
                
            # 从RTT信息中移除
            if broker in self.broker_rtt:
                del self.broker_rtt[broker]
                
            # 从overlay树中移除
            if broker in self.overlay_tree:
                del self.overlay_tree[broker]
                
            # 更新父节点信息
            if self.parent_broker == broker:
                self.parent_broker = None
                
        # 触发拓扑重建
        self.trigger_topology_rebuild()
        
    def trigger_topology_rebuild(self):
        """触发拓扑重建"""
        self.logger.info("开始拓扑重建")
        
        # 发布拓扑变更通知
        self.publish_topology_change("rebuild")
        
        # 重新构建overlay树
        self.build_overlay_tree()
        
        # 更新路由表
        self.update_routing_table()
        
        # 更新性能指标
        self.update_performance_metrics('topology_change')
        
    def publish_topology_change(self, change_type):
        """发布拓扑变更通知"""
        message = {
            'type': 'topology_change',
            'change_type': change_type,
            'node_id': self.node_id,
            'timestamp': time.time(),
            'broker_list': self.broker_list,
            'overlay_tree': self.overlay_tree
        }
        
        self.client.publish(
            "$SYS/topology",
            json.dumps(message),
            qos=1
        )
        
    def handle_topology_change(self, message):
        """处理拓扑变更通知"""
        try:
            data = json.loads(message.payload)
            if data['type'] != 'topology_change':
                return
                
            change_type = data['change_type']
            source_node = data['node_id']
            
            if change_type == 'rebuild':
                # 更新broker列表
                self.broker_list = data['broker_list']
                
                # 更新overlay树
                self.overlay_tree = data['overlay_tree']
                
                # 重新计算路由
                self.update_routing_table()
                
                self.logger.info(f"处理拓扑重建通知: {source_node}")
                
            elif change_type == 'broker_failure':
                failed_broker = data['failed_broker']
                self.handle_broker_failure([failed_broker])
                
            elif change_type == 'broker_join':
                new_broker = data['new_broker']
                if new_broker not in self.broker_list:
                    self.broker_list.append(new_broker)
                    self.trigger_topology_rebuild()
                    
        except json.JSONDecodeError:
            self.logger.error("无效的拓扑变更消息格式")
        except Exception as e:
            self.logger.error(f"处理拓扑变更时出错: {str(e)}")
            
    def handle_system_message(self, client, userdata, message):
        """处理系统消息"""
        try:
            data = json.loads(message.payload)
            msg_type = data.get('type')
            
            if msg_type == 'ping':
                self.handle_ping_message(data)
            elif msg_type == 'resources':
                self.handle_resource_message(data)
            elif msg_type == 'topology_change':
                self.handle_topology_change(message)
            elif msg_type == 'load_balancing':
                self.handle_load_balancing(data)
                
        except json.JSONDecodeError:
            self.logger.error("无效的系统消息格式")
        except Exception as e:
            self.logger.error(f"处理系统消息时出错: {str(e)}")
            
    def handle_load_balancing(self, data):
        """处理负载均衡请求"""
        overloaded_broker = data.get('overloaded_broker')
        if not overloaded_broker:
            return
            
        # 检查是否需要重新分配负载
        if self.parent_broker == overloaded_broker:
            # 寻找新的父节点
            new_parent = self.find_alternative_parent()
            if new_parent:
                self.parent_broker = new_parent
                self.trigger_topology_rebuild()
                
    def find_alternative_parent(self):
        """寻找替代父节点"""
        available_brokers = [b for b in self.broker_list if b != self.parent_broker]
        if not available_brokers:
            return None
            
        # 选择负载最低的broker作为新的父节点
        return min(
            available_brokers,
            key=lambda b: self.broker_resources.get(b, {}).get('cpu_usage', float('inf'))
        )

    def simulate_locality(self, topic):
        """基于主题局部性选择Broker"""
        # 主题局部性映射
        topic_locality = {
            "sensor/": "broker1",
            "control/": "broker2",
            "monitor/": "broker3"
        }
        
        # 检查主题前缀匹配
        for prefix, broker in topic_locality.items():
            if topic.startswith(prefix):
                return broker
                
        # 如果没有匹配的前缀，使用负载均衡选择
        return self.select_broker_by_load()
        
    def select_broker_by_load(self):
        """基于负载均衡选择Broker"""
        if not self.broker_resources:
            return None
            
        # 计算每个broker的负载分数
        broker_scores = {}
        for broker, resources in self.broker_resources.items():
            if not resources:
                continue
                
            # 计算CPU和内存负载
            cpu_load = resources.get('cpu_usage', 0)
            mem_load = resources.get('memory_usage', 0)
            
            # 考虑连接数和消息吞吐量
            connections = resources.get('connections', 0)
            messages = resources.get('messages_per_second', 0)
            
            # 综合评分（越低越好）
            score = (cpu_load * 0.3 + mem_load * 0.3 + 
                    connections * 0.2 + messages * 0.2)
            
            broker_scores[broker] = score
            
        if not broker_scores:
            return None
            
        # 选择负载最低的broker
        return min(broker_scores.items(), key=lambda x: x[1])[0]

    def calculate_root_balance_score(self, broker):
        """计算Root Balancing分数 RB = γR + δC
        R: 资源负载分数
        C: 连接数分数
        γ, δ: 权重系数
        """
        resources = self.broker_resources.get(broker, {})
        if not resources:
            return float('inf')
        
        # 计算资源负载分数 R
        cpu_load = resources.get('cpu_usage', 0)
        mem_load = resources.get('memory_usage', 0)
        resource_score = (cpu_load * 0.5 + mem_load * 0.5) / 100.0
        
        # 计算连接数分数 C
        connections = resources.get('active_connections', 0)
        max_connections = 1000  # 假设最大连接数
        connection_score = connections / max_connections
        
        # 权重系数
        gamma = 0.7  # 资源负载权重
        delta = 0.3  # 连接数权重
        
        # 计算最终分数
        rb_score = gamma * resource_score + delta * connection_score
        return rb_score

    def select_root_broker(self):
        """选择最优的Root Broker"""
        if not self.broker_list:
            return None
        
        # 计算每个broker的Root Balancing分数
        broker_scores = {
            broker: self.calculate_root_balance_score(broker)
            for broker in self.broker_list
        }
        
        # 选择分数最低的broker作为root
        return min(broker_scores.items(), key=lambda x: x[1])[0]

    def handle_advertisement(self, topic, payload):
        """处理发布广告消息"""
        try:
            adv_data = json.loads(payload)
            pub_topic = adv_data.get('topic')
            source_broker = adv_data.get('broker_id')
            ttl = adv_data.get('ttl', 5)  # 广告生存时间
            
            if not pub_topic or not source_broker:
                return
            
            # 更新发布路由表(SRT)
            if pub_topic not in self.publication_routing_table:
                self.publication_routing_table[pub_topic] = set()
            self.publication_routing_table[pub_topic].add(source_broker)
            
            # 如果TTL > 0，继续转发广告
            if ttl > 0:
                self.forward_advertisement(pub_topic, source_broker, ttl - 1)
            
        except Exception as e:
            self.logger.error(f"处理发布广告失败: {str(e)}")

    def forward_advertisement(self, topic, source_broker, ttl):
        """转发发布广告"""
        adv_data = {
            'topic': topic,
            'broker_id': source_broker,
            'ttl': ttl,
            'timestamp': time.time()
        }
        
        # 发布广告消息
        self.client.publish(
            f"$PUBADV/{topic}",
            json.dumps(adv_data),
            qos=1
        )

    def publish_advertisement(self, topic):
        """发布主题广告"""
        adv_data = {
            'topic': topic,
            'broker_id': self.node_id,
            'ttl': 5,  # 初始TTL
            'timestamp': time.time()
        }
        
        # 发布广告消息
        self.client.publish(
            f"$PUBADV/{topic}",
            json.dumps(adv_data),
            qos=1
        )

    def update_subscription_routing(self, topic, subscriber):
        """更新订阅路由表(PRT)"""
        if topic not in self.subscription_routing_table:
            self.subscription_routing_table[topic] = set()
        self.subscription_routing_table[topic].add(subscriber)

    def get_publication_route(self, topic):
        """获取发布路由"""
        return self.publication_routing_table.get(topic, set())

    def get_subscription_route(self, topic):
        """获取订阅路由"""
        return self.subscription_routing_table.get(topic, set())

    def publish_subscription_advertisement(self, topic):
        """发布订阅广告"""
        sub_adv_data = {
            'topic': topic,
            'subscriber_id': self.node_id,
            'timestamp': time.time()
        }
        
        self.client.publish(
            f"$SUBADV/{topic}",
            json.dumps(sub_adv_data),
            qos=1
        )

    def handle_subscription_advertisement(self, topic, payload):
        """处理订阅广告"""
        try:
            sub_adv_data = json.loads(payload)
            sub_topic = sub_adv_data.get('topic')
            subscriber = sub_adv_data.get('subscriber_id')
            
            if not sub_topic or not subscriber:
                return
            
            # 更新订阅路由表
            self.update_subscription_routing(sub_topic, subscriber)
            
        except Exception as e:
            self.logger.error(f"处理订阅广告失败: {str(e)}")
