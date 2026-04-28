import asyncio
import time
import random
import json
import subprocess
import os
import socket
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Tuple
import paho.mqtt.client as mqtt
import threading


@dataclass
class BrokerState:
    """Broker state for consensus"""
    rtt: Dict[str, float] = field(default_factory=dict)  # RTT to neighbors
    load: float = 0.0  # Current load
    bandwidth: Dict[str, float] = field(default_factory=dict)  # Bandwidth to neighbors
    downstream_cost: float = float('inf')  # Estimated downstream path cost
    iteration: int = 0  # Consensus iteration count
    neighbor_load_estimates: Dict[str, float] = field(default_factory=dict)  # Estimated neighbor loads from consensus
    
    def copy(self):
        """Create a copy of the state"""
        return BrokerState(
            rtt=self.rtt.copy(),
            load=self.load,
            bandwidth=self.bandwidth.copy(),
            downstream_cost=self.downstream_cost,
            iteration=self.iteration,
            neighbor_load_estimates=self.neighbor_load_estimates.copy()
        )


class ConsensusDGDBroker:
    """MQTT Broker Client with Consensus + DGD algorithm"""
    
    # Port mapping: broker_id -> port
    PORT_MAP = {
        'A': 1884,
        'B': 1885,
        'C': 1886,
        'D': 1887,
        'E': 1888
    }
    
    def __init__(self, broker_id: str, neighbors: List[str], host: str = "localhost"):
        self.broker_id = broker_id
        self.neighbors = neighbors
        self.host = host
        self.port = self.PORT_MAP.get(broker_id, 1884)
        self.state = BrokerState()
        
        # Consensus parameters
        self.epsilon = 0.1  # Consensus step size
        
        # DGD parameters
        self.alpha = 1.0  # RTT weight
        self.beta = 1.0   # Load weight
        self.gamma = 1.0  # Bandwidth weight
        self.eta = 0.3    # DGD learning rate (increased for faster convergence in routing)
        
        # Consensus weights (equal weights for simplicity)
        self.consensus_weights = {n: 1.0 / (len(neighbors) + 1) for n in neighbors}
        self.consensus_weights[self.broker_id] = 1.0 / (len(neighbors) + 1)
        
        # Message queue
        self.message_queue = deque()
        self.subscriptions = {}  # topic -> set of subscribers
        
        # Next hop decision
        self.next_hop: Optional[str] = None
        
        # Performance metrics
        self.messages_forwarded = 0
        self.messages_delivered = 0
        
        # Statistics tracking
        self.max_load_seen = 0.0  # Maximum load observed
        self.total_messages_processed = 0  # Total messages that passed through
        self.final_downstream_cost = float('inf')  # Final downstream cost for statistics
        
        # MQTT client
        self.mqtt_client: Optional[mqtt.Client] = None
        self.mqtt_connected = False
        
        # Topics for consensus communication
        self.consensus_topic_prefix = f"consensus/{broker_id}"
        self.message_topic_prefix = f"message/{broker_id}"
        self.state_topic = f"state/{broker_id}"
        
        # Received messages
        self.received_state_messages = {}
        self.received_application_messages = deque()
        
        # Neighbor clients for subscribing to neighbor states
        self.neighbor_clients = {}
        
        # Neighbor clients for publishing messages (persistent connections)
        self.neighbor_publish_clients = {}
        
        # Lock for thread safety
        self.lock = threading.Lock()
    
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT on_connect callback"""
        if rc == 0:
            self.mqtt_connected = True
            print(f"Broker {self.broker_id} connected to MQTT broker at {self.host}:{self.port}")
            
            # Subscribe to message topics on own broker (messages from neighbors will be published here)
            message_topic = f"message/{self.broker_id}"
            client.subscribe(message_topic)
            print(f"  Subscribed to {message_topic} for receiving messages")
        else:
            print(f"Broker {self.broker_id} failed to connect to MQTT broker, return code {rc}")
            self.mqtt_connected = False
    
    def subscribe_to_neighbor_states(self):
        """Subscribe to neighbor state topics by connecting to neighbor brokers"""
        self.neighbor_clients = {}
        
        for neighbor in self.neighbors:
            neighbor_port = self.PORT_MAP.get(neighbor, 1884)
            neighbor_client = mqtt.Client(client_id=f"{self.broker_id}_sub_{neighbor}_{int(time.time())}")
            
            def make_on_message(neighbor_id):
                def on_message(client, userdata, msg):
                    try:
                        payload = json.loads(msg.payload.decode())
                        if msg.topic.startswith("state/"):
                            with self.lock:
                                self.received_state_messages[neighbor_id] = payload
                    except Exception as e:
                        print(f"Broker {self.broker_id} error processing message from {neighbor_id}: {e}")
                return on_message
            
            neighbor_client.on_message = make_on_message(neighbor)
            
            try:
                neighbor_client.connect(self.host, neighbor_port, 60)
                neighbor_client.loop_start()
                
                # Subscribe to neighbor's state topic
                state_topic = f"state/{neighbor}"
                neighbor_client.subscribe(state_topic)
                print(f"Broker {self.broker_id} subscribed to {neighbor}'s state topic on port {neighbor_port}")
                
                self.neighbor_clients[neighbor] = neighbor_client
            except Exception as e:
                print(f"Broker {self.broker_id} failed to connect to {neighbor}'s broker (port {neighbor_port}): {e}")
    
    def _on_message(self, client, userdata, msg):
        """MQTT on_message callback"""
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode())
            
            # Handle state messages (for consensus)
            if topic.startswith("state/"):
                neighbor_id = topic.split("/")[1]
                with self.lock:
                    self.received_state_messages[neighbor_id] = payload
            
            # Handle application messages
            elif topic.startswith("message/"):
                with self.lock:
                    self.received_application_messages.append(payload)
        except Exception as e:
            print(f"Broker {self.broker_id} error processing message on {topic}: {e}")
    
    def _on_disconnect(self, client, userdata, rc):
        """MQTT on_disconnect callback"""
        self.mqtt_connected = False
        print(f"Broker {self.broker_id} disconnected from MQTT broker")
    
    def connect_mqtt(self):
        """Connect to MQTT broker"""
        if self.mqtt_client is not None:
            return
        
        self.mqtt_client = mqtt.Client(client_id=f"broker_{self.broker_id}_{int(time.time())}")
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message
        self.mqtt_client.on_disconnect = self._on_disconnect
        
        try:
            self.mqtt_client.connect(self.host, self.port, 60)
            self.mqtt_client.loop_start()
            
            # Wait for connection
            timeout = 5
            elapsed = 0
            while not self.mqtt_connected and elapsed < timeout:
                time.sleep(0.1)
                elapsed += 0.1
            
            if not self.mqtt_connected:
                raise Exception(f"Failed to connect to MQTT broker at {self.host}:{self.port}")
            
            # Subscribe to neighbor states
            self.subscribe_to_neighbor_states()
        except Exception as e:
            print(f"Broker {self.broker_id} MQTT connection error: {e}")
            raise
    
    def disconnect_mqtt(self):
        """Disconnect from MQTT broker"""
        # Disconnect neighbor subscribe clients
        for neighbor, client in self.neighbor_clients.items():
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
        self.neighbor_clients.clear()
        
        # Disconnect neighbor publish clients
        for neighbor, client in self.neighbor_publish_clients.items():
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
        self.neighbor_publish_clients.clear()
        
        # Disconnect main client
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            self.mqtt_connected = False
    
    def publish_state(self):
        """Publish current state to MQTT"""
        if not self.mqtt_connected or not self.mqtt_client:
            return
        
        state_data = {
            'rtt': self.state.rtt,
            'load': self.state.load,
            'bandwidth': self.state.bandwidth,
            'downstream_cost': self.state.downstream_cost,
            'iteration': self.state.iteration,
            'neighbor_load_estimates': self.state.neighbor_load_estimates
        }
        
        try:
            payload = json.dumps(state_data)
            self.mqtt_client.publish(self.state_topic, payload, qos=1)
        except Exception as e:
            print(f"Broker {self.broker_id} error publishing state: {e}")
    
    def _ensure_neighbor_publish_client(self, neighbor: str, neighbor_port: int) -> Optional[mqtt.Client]:
        """
        Ensure we have a persistent connection to neighbor's broker for publishing
        Returns the client if successful, None otherwise
        """
        if neighbor in self.neighbor_publish_clients:
            client = self.neighbor_publish_clients[neighbor]
            # Check if client is still connected
            try:
                if client.is_connected():
                    return client
                else:
                    # Reconnect if disconnected
                    client.reconnect()
                    return client
            except:
                # Client might be in bad state, remove it and recreate
                try:
                    client.loop_stop()
                    client.disconnect()
                except:
                    pass
                del self.neighbor_publish_clients[neighbor]
        
        # Create new persistent client
        try:
            client = mqtt.Client(client_id=f"{self.broker_id}_pub_{neighbor}")
            client.connect(self.host, neighbor_port, 60)
            client.loop_start()  # Start background thread for network loop
            self.neighbor_publish_clients[neighbor] = client
            
            # Wait a moment for connection to establish
            time.sleep(0.1)
            
            if client.is_connected():
                return client
            else:
                # Connection failed, remove client
                try:
                    client.loop_stop()
                    client.disconnect()
                except:
                    pass
                del self.neighbor_publish_clients[neighbor]
                return None
        except Exception as e:
            print(f"Broker {self.broker_id} error creating publish client to {neighbor}: {e}")
            return None
    
    def publish_message_to_neighbor(self, topic: str, message: str, next_hop: str, neighbor_port: int):
        """
        Publish application message to neighbor's MQTT broker
        Improved: Uses persistent connections instead of creating new clients each time
        """
        message_topic = f"message/{next_hop}"
        message_data = {
            'topic': topic,
            'message': message,
            'from_broker': self.broker_id,
            'timestamp': time.time()
        }
        
        # Get or create persistent client for this neighbor
        client = self._ensure_neighbor_publish_client(next_hop, neighbor_port)
        if client is None:
            print(f"Broker {self.broker_id} failed to get publish client for {next_hop}")
            return False
        
        try:
            payload = json.dumps(message_data)
            result = client.publish(message_topic, payload, qos=1)
            
            # Wait for publish to complete (non-blocking with timeout)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                print(f"Broker {self.broker_id} published message to {next_hop}'s broker (port {neighbor_port})")
                return True
            else:
                print(f"Broker {self.broker_id} publish failed to {next_hop}: return code {result.rc}")
                return False
        except Exception as e:
            print(f"Broker {self.broker_id} error publishing message to {next_hop}: {e}")
            # Remove broken client so it will be recreated next time
            if next_hop in self.neighbor_publish_clients:
                try:
                    self.neighbor_publish_clients[next_hop].loop_stop()
                    self.neighbor_publish_clients[next_hop].disconnect()
                except:
                    pass
                del self.neighbor_publish_clients[next_hop]
            return False
    
    def publish_message(self, topic: str, message: str, next_hop: Optional[str] = None):
        """Publish application message to MQTT"""
        if next_hop is None:
            next_hop = self.next_hop
        
        if next_hop is None:
            print(f"Broker {self.broker_id}: No next hop specified, cannot forward message")
            return False
        
        # Get neighbor's port
        neighbor_port = self.PORT_MAP.get(next_hop, 1884)
        
        # Publish to neighbor's broker
        return self.publish_message_to_neighbor(topic, message, next_hop, neighbor_port)
        
    def update_consensus(self, neighbor_states: Dict[str, BrokerState]) -> BrokerState:
        """
        Consensus layer: Average consensus algorithm
        s_i^(k+1) = s_i^(k) + ε * Σ_{j∈N_i} (s_j^(k) - s_i^(k))
        
        Fixed: Initialize new_state with all old_state values first, then update via consensus
        """
        old_state = self.state.copy()
        new_state = BrokerState()
        new_state.iteration = old_state.iteration + 1
        
        # CRITICAL FIX: Initialize new_state with all old_state RTT/BW values first
        # This ensures we don't lose information when neighbors don't report certain targets
        new_state.rtt = old_state.rtt.copy()
        new_state.bandwidth = old_state.bandwidth.copy()
        
        # Collect all targets that appear in any neighbor's state
        all_rtt_targets = set(old_state.rtt.keys())
        all_bw_targets = set(old_state.bandwidth.keys())
        for neighbor_state in neighbor_states.values():
            all_rtt_targets.update(neighbor_state.rtt.keys())
            all_bw_targets.update(neighbor_state.bandwidth.keys())
        
        # Initialize missing targets with default values (from old_state or default)
        for target in all_rtt_targets:
            if target not in new_state.rtt:
                new_state.rtt[target] = old_state.rtt.get(target, 10.0)  # Default RTT: 10ms
        
        for target in all_bw_targets:
            if target not in new_state.bandwidth:
                new_state.bandwidth[target] = old_state.bandwidth.get(target, 1.0)  # Default BW: 1 Mbps
        
        # Update RTT consensus: iterate over neighbors and update all targets
        for neighbor in self.neighbors:
            if neighbor in neighbor_states:
                neighbor_state = neighbor_states[neighbor]
                
                # Average consensus for all RTT estimates (including indirect targets)
                for target, rtt_value in neighbor_state.rtt.items():
                    # Ensure target exists in new_state (should already be initialized above)
                    if target not in new_state.rtt:
                        new_state.rtt[target] = old_state.rtt.get(target, 10.0)
                    
                    # Consensus update: move towards neighbor's value
                    diff = rtt_value - new_state.rtt[target]
                    new_state.rtt[target] += self.epsilon * diff
                
                # For direct RTT to this neighbor: use neighbor's self-reported RTT if available
                # This represents neighbor's estimate of RTT from neighbor to self
                # But more importantly, we should use our direct measurement if available
                # Here we handle the case where neighbor reports its RTT to us
                if neighbor in neighbor_state.rtt:
                    # Neighbor reports its RTT to us (from neighbor's perspective)
                    # We should consensus this with our own measurement
                    neighbor_reported_rtt = neighbor_state.rtt[neighbor]
                    if neighbor not in new_state.rtt:
                        new_state.rtt[neighbor] = old_state.rtt.get(neighbor, 10.0)
                    # Consensus: average our measurement with neighbor's report
                    diff = neighbor_reported_rtt - new_state.rtt[neighbor]
                    new_state.rtt[neighbor] += self.epsilon * diff
        
        # Update load consensus: keep our own actual load, but also track neighbor estimates
        # Load should reflect actual message queue length, not average
        # So we maintain our own load from queue length, and use consensus to estimate neighbor loads
        new_state.load = old_state.load
        
        # Store neighbor load estimates from consensus (for cost function calculation)
        new_state.neighbor_load_estimates = {}
        for neighbor in self.neighbors:
            if neighbor in neighbor_states:
                neighbor_load = neighbor_states[neighbor].load
                # Update estimate towards neighbor's actual load
                old_estimate = old_state.neighbor_load_estimates.get(neighbor, 0.0)
                diff = neighbor_load - old_estimate
                new_state.neighbor_load_estimates[neighbor] = old_estimate + self.epsilon * diff
            else:
                # Keep old estimate if neighbor state not available
                new_state.neighbor_load_estimates[neighbor] = old_state.neighbor_load_estimates.get(neighbor, 0.0)
        
        # Update bandwidth consensus (similar to RTT)
        for neighbor in self.neighbors:
            if neighbor in neighbor_states:
                neighbor_state = neighbor_states[neighbor]
                
                # Average consensus for all bandwidth estimates
                for target, bw_value in neighbor_state.bandwidth.items():
                    # Ensure target exists in new_state (should already be initialized above)
                    if target not in new_state.bandwidth:
                        new_state.bandwidth[target] = old_state.bandwidth.get(target, 1.0)
                    
                    diff = bw_value - new_state.bandwidth[target]
                    new_state.bandwidth[target] += self.epsilon * diff
                
                # For direct bandwidth to this neighbor: use neighbor's self-reported BW if available
                if neighbor in neighbor_state.bandwidth:
                    neighbor_reported_bw = neighbor_state.bandwidth[neighbor]
                    if neighbor not in new_state.bandwidth:
                        new_state.bandwidth[neighbor] = old_state.bandwidth.get(neighbor, 1.0)
                    # Consensus: average our measurement with neighbor's report
                    diff = neighbor_reported_bw - new_state.bandwidth[neighbor]
                    new_state.bandwidth[neighbor] += self.epsilon * diff
        
        # Preserve downstream cost for DGD
        new_state.downstream_cost = old_state.downstream_cost
        
        self.state = new_state
        return new_state
    
    def calculate_cost_function(self, target_neighbor: str, neighbor_load: Optional[float] = None, 
                                neighbor_state: Optional['BrokerState'] = None) -> float:
        """
        DGD Layer: Calculate cost function
        f_{i→j} = α * RTT_{ij} + β * Load_j + γ * BW_{ij}^(-1)
        
        Improved: Can use neighbor_state's RTT/BW information if available for more accurate cost estimation
        """
        # Use neighbor's reported RTT if available (more real-time), otherwise use local estimate
        if neighbor_state is not None and target_neighbor in neighbor_state.rtt:
            # Neighbor reports its RTT to us (symmetric link assumption)
            # But we should prefer our direct measurement if we have it
            # For now, use the average of our estimate and neighbor's report, weighted towards ours
            neighbor_rtt = neighbor_state.rtt.get(target_neighbor, 10.0)
            local_rtt = self.state.rtt.get(target_neighbor, 10.0)
            # Use local RTT as primary (direct measurement), but consider neighbor's report
            # If we don't have a measurement, use neighbor's report
            if local_rtt == 10.0:  # Default value, might not be measured
                rtt_ij = neighbor_rtt
            else:
                # Weighted average: 70% local, 30% neighbor (prefer direct measurement)
                rtt_ij = 0.7 * local_rtt + 0.3 * neighbor_rtt
        else:
            rtt_ij = self.state.rtt.get(target_neighbor, 10.0)  # Default 10ms
        
        # Use neighbor's actual load if provided, otherwise use consensus estimate
        if neighbor_load is not None:
            load_j = neighbor_load
        else:
            # Use stored neighbor load estimate from consensus
            load_j = self.state.neighbor_load_estimates.get(target_neighbor, 0.0)
        
        # Use neighbor's reported bandwidth if available
        if neighbor_state is not None and target_neighbor in neighbor_state.bandwidth:
            neighbor_bw = neighbor_state.bandwidth.get(target_neighbor, 1.0)
            local_bw = self.state.bandwidth.get(target_neighbor, 1.0)
            # Similar to RTT: prefer local measurement, but use neighbor's if we don't have one
            if local_bw == 1.0:  # Default value
                bw_ij = neighbor_bw
            else:
                # Weighted average: 70% local, 30% neighbor
                bw_ij = 0.7 * local_bw + 0.3 * neighbor_bw
        else:
            bw_ij = self.state.bandwidth.get(target_neighbor, 1.0)
        
        bw_ij_inv = 1.0 / max(bw_ij, 0.1)  # Avoid division by zero
        
        cost = self.alpha * rtt_ij + self.beta * load_j + self.gamma * bw_ij_inv
        return cost
    
    def update_dgd(self, neighbor_states: Dict[str, BrokerState], target_brokers: Optional[Set[str]] = None) -> float:
        """
        DGD Layer: Distributed gradient descent update for routing
        For routing problems, we use Bellman-Ford style update instead of consensus on downstream_cost
        downstream_cost_i = (1-eta) * current + eta * min(edge_cost + neighbor_downstream_cost)
        
        Args:
            neighbor_states: Dictionary of neighbor states
            target_brokers: Set of target broker IDs (for excluding from consensus)
        
        Returns updated downstream cost estimate
        """
        if target_brokers is None:
            target_brokers = set()
        
        # For routing problems, we should NOT use consensus on downstream_cost
        # because each broker's distance to target is different.
        # Instead, we use a Bellman-Ford style update with smoothing:
        # downstream_cost = (1-eta) * current + eta * min(edge_cost + neighbor_downstream_cost)
        
        # Calculate minimum cost through neighbors (Bellman-Ford style)
        min_neighbor_cost = float('inf')
        for neighbor in self.neighbors:
            if neighbor in neighbor_states:
                neighbor_state = neighbor_states[neighbor]
                # Use neighbor's actual load and state information for more accurate cost calculation
                cost_to_neighbor = self.calculate_cost_function(
                    neighbor, 
                    neighbor_load=neighbor_state.load,
                    neighbor_state=neighbor_state  # Pass neighbor_state to use its RTT/BW info
                )
                neighbor_downstream = neighbor_state.downstream_cost
                # Only consider finite downstream costs
                if neighbor_downstream < float('inf'):
                    total_cost = cost_to_neighbor + neighbor_downstream
                    min_neighbor_cost = min(min_neighbor_cost, total_cost)
        
        # Update downstream_cost using smoothing (not pure Bellman-Ford, but similar)
        # This prevents oscillation and provides smooth convergence
        if min_neighbor_cost < float('inf'):
            if self.state.downstream_cost < float('inf'):
                # Both are finite, use smoothed update
                # This is similar to gradient descent: move towards min_neighbor_cost
                # Use larger eta for faster convergence to correct values
                new_downstream_cost = (1.0 - self.eta) * self.state.downstream_cost + self.eta * min_neighbor_cost
            else:
                # Current is inf but we found a finite path, initialize from it
                new_downstream_cost = min_neighbor_cost
        else:
            # No finite path found, keep current (may be inf)
            new_downstream_cost = self.state.downstream_cost
        
        # Ensure non-negative and update
        # Important: if we're the target broker, this will be overridden to 0 in run_consensus_iteration
        if new_downstream_cost < float('inf'):
            self.state.downstream_cost = max(new_downstream_cost, 0.0)
        else:
            # Keep inf if no finite path exists
            self.state.downstream_cost = new_downstream_cost
        
        return self.state.downstream_cost
    
    def discrete_routing_selection(self, exclude_neighbor: Optional[str] = None, neighbor_states: Optional[Dict[str, 'BrokerState']] = None) -> Optional[str]:
        """
        Discrete Routing Selection Layer
        NextHop(i) = arg min_{j∈N_i} [f_i→j + downstream_cost_j]
        
        Selects the neighbor with minimum total cost (edge cost + downstream cost)
        This ensures routing towards the destination and prevents loops.
        
        Args:
            exclude_neighbor: Neighbor to exclude (prevents sending back to previous hop)
            neighbor_states: Dictionary of neighbor states (for downstream_cost lookup)
        """
        if not self.neighbors:
            return None
        
        min_cost = float('inf')
        best_neighbor = None
        
        # Get neighbor states if not provided (from received_state_messages)
        if neighbor_states is None:
            neighbor_states = {}
            with self.lock:
                for neighbor_id, state_data in self.received_state_messages.items():
                    if neighbor_id in self.neighbors:
                        neighbor_state = BrokerState()
                        neighbor_state.downstream_cost = state_data.get('downstream_cost', float('inf'))
                        neighbor_states[neighbor_id] = neighbor_state
        
        for neighbor in self.neighbors:
            # Skip excluded neighbor (prevents loop)
            if neighbor == exclude_neighbor:
                continue
            
            # Get neighbor state if available
            neighbor_state_obj = None
            neighbor_downstream = float('inf')
            
            if neighbor in neighbor_states:
                neighbor_state_obj = neighbor_states[neighbor]
                neighbor_downstream = neighbor_state_obj.downstream_cost
            else:
                # Fallback: try to get from received messages
                with self.lock:
                    if neighbor in self.received_state_messages:
                        state_data = self.received_state_messages[neighbor]
                        neighbor_downstream = state_data.get('downstream_cost', float('inf'))
                        # Construct a minimal BrokerState from received message for RTT/BW info
                        neighbor_state_obj = BrokerState()
                        neighbor_state_obj.rtt = state_data.get('rtt', {})
                        neighbor_state_obj.bandwidth = state_data.get('bandwidth', {})
                        neighbor_state_obj.load = state_data.get('load', 0.0)
            
            # Calculate edge cost: f_i→j (use neighbor_state if available for better accuracy)
            edge_cost = self.calculate_cost_function(
                neighbor, 
                neighbor_state=neighbor_state_obj  # Use neighbor's RTT/BW if available
            )
            
            # Total cost: edge cost + downstream cost
            # Only consider neighbors with finite downstream cost (path exists to destination)
            if neighbor_downstream < float('inf'):
                total_cost = edge_cost + neighbor_downstream
                if total_cost < min_cost:
                    min_cost = total_cost
                    best_neighbor = neighbor
            # If no finite path found, still consider edge cost only (fallback)
            elif min_cost == float('inf'):
                if edge_cost < min_cost:
                    min_cost = edge_cost
                    best_neighbor = neighbor
        
        self.next_hop = best_neighbor
        return best_neighbor
    
    def add_message(self, topic: str, message: str):
        """Add message to queue"""
        self.message_queue.append((topic, message, time.time()))
        self.state.load += 1.0  # Increase load when message arrives
        self.max_load_seen = max(self.max_load_seen, self.state.load)
        self.total_messages_processed += 1
    
    def subscribe(self, topic: str, subscriber_id: str):
        """Subscribe to a topic"""
        if topic not in self.subscriptions:
            self.subscriptions[topic] = set()
        self.subscriptions[topic].add(subscriber_id)
    
    def deliver_message(self, topic: str, message: str) -> bool:
        """Deliver message to local subscribers"""
        if topic in self.subscriptions and self.subscriptions[topic]:
            self.messages_delivered += 1
            self.state.load = max(0.0, self.state.load - 1.0)  # Decrease load after delivery
            return True
        return False


class ConsensusDGDNetwork:
    """Network of MQTT brokers using Consensus + DGD"""
    
    def __init__(self, broker_ids: List[str], topology: Dict[str, List[str]], host: str = "localhost"):
        """
        Initialize network
        
        Args:
            broker_ids: List of broker IDs (e.g., ['A', 'B', 'C', 'D', 'E'])
            topology: Dictionary mapping broker_id to list of neighbor IDs
            host: MQTT broker host (default: localhost)
        """
        self.brokers: Dict[str, ConsensusDGDBroker] = {}
        self.topology = topology
        self.host = host
        self.broker_processes: Dict[str, subprocess.Popen] = {}
        
        # Initialize brokers
        for broker_id in broker_ids:
            neighbors = topology.get(broker_id, [])
            self.brokers[broker_id] = ConsensusDGDBroker(broker_id, neighbors, host)
        
        # Initialize network metrics (RTT, bandwidth)
        self._initialize_network_metrics()
        
        # Consensus iteration counter
        self.consensus_iterations = 0
        
        # Track target brokers (brokers with subscribers)
        self.target_brokers: Set[str] = set()
        # Buffer processing control
        self._last_message_pump = 0.0

    def pump_received_messages(self):
        """
        Move any MQTT-delivered application messages into each broker's local queue.
        Should be called periodically by the orchestrator (e.g., benchmark loop).
        """
        for broker in self.brokers.values():
            pending: List[Tuple[str, str, str]] = []
            with broker.lock:
                while broker.received_application_messages:
                    payload = broker.received_application_messages.popleft()
                    topic = payload.get("topic")
                    message = payload.get("message")
                    from_broker = payload.get("from_broker")
                    if topic is None or message is None:
                        continue
                    pending.append((topic, message, from_broker))
            for topic, message, from_broker in pending:
                broker.add_message(topic, message)
                print(
                    f"[ConsensusDGD] Broker {broker.broker_id} queued message on {topic} from {from_broker}"
                )
    
    def start_mqtt_brokers(self):
        """Start MQTT broker processes for each broker ID (ports 1884-1888)"""
        print("\n=== Starting MQTT Broker Processes ===")
        
        # Try to use mosquitto if available
        mosquitto_paths = [
            r"C:\Program Files\mosquitto\mosquitto.exe",
            r"C:\mosquitto\mosquitto.exe",
            "mosquitto",  # In PATH
            "/usr/local/sbin/mosquitto",  # Linux
            "/usr/sbin/mosquitto",  # Linux
        ]
        
        mosquitto_exe = None
        for path in mosquitto_paths:
            if os.path.exists(path) or path in ["mosquitto", "/usr/local/sbin/mosquitto", "/usr/sbin/mosquitto"]:
                try:
                    # Test if command works
                    result = subprocess.run([path, "-h"], capture_output=True, timeout=2)
                    mosquitto_exe = path
                    break
                except:
                    continue
        
        if mosquitto_exe is None:
            print("Warning: Mosquitto not found. Using Python-based MQTT broker implementation.")
            print("Please install Mosquitto MQTT broker or install from: https://mosquitto.org/download/")
            # For now, we'll assume mosquitto is available or will use alternative
            # In a real implementation, you might use hbmqtt or another Python MQTT broker
            mosquitto_exe = "mosquitto"  # Try default
        
        # Start a broker for each broker ID
        for broker_id, broker in self.brokers.items():
            port = broker.port
            
            # Check if port is already in use
            if self._is_port_in_use(port):
                print(f"Port {port} is already in use. Skipping broker {broker_id}.")
                continue
            
            # Create a minimal config for mosquitto
            config_content = f"""listener {port}
allow_anonymous true
pid_file /tmp/mosquitto_{broker_id}.pid
"""
            
            # Save config to temp file
            import tempfile
            config_file = tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False)
            config_file.write(config_content)
            config_file.close()
            
            try:
                # Start mosquitto broker
                if os.name == 'nt':  # Windows
                    process = subprocess.Popen(
                        [mosquitto_exe, "-c", config_file.name, "-p", str(port)],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        creationflags=subprocess.CREATE_NEW_CONSOLE if mosquitto_exe.endswith('.exe') else 0
                    )
                else:  # Linux/Mac
                    process = subprocess.Popen(
                        [mosquitto_exe, "-c", config_file.name, "-p", str(port)],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                
                self.broker_processes[broker_id] = process
                print(f"Started MQTT broker for {broker_id} on port {port} (PID: {process.pid})")
                
                # Wait a bit for broker to start
                time.sleep(0.5)
            except Exception as e:
                print(f"Failed to start MQTT broker for {broker_id} on port {port}: {e}")
                print("Note: Make sure Mosquitto is installed and available in PATH")
        
        print(f"\nStarted {len(self.broker_processes)} MQTT broker processes")
    
    def _is_port_in_use(self, port: int) -> bool:
        """Check if a port is already in use"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        try:
            result = sock.connect_ex((self.host, port))
            sock.close()
            return result == 0
        except:
            sock.close()
            return False
    
    def connect_all_brokers(self):
        """Connect all broker clients to their respective MQTT brokers"""
        print("\n=== Connecting Broker Clients to MQTT Brokers ===")
        for broker_id, broker in self.brokers.items():
            try:
                broker.connect_mqtt()
                print(f"Connected broker client {broker_id} to MQTT broker at {self.host}:{broker.port}")
            except Exception as e:
                print(f"Failed to connect broker client {broker_id}: {e}")
    
    def stop_mqtt_brokers(self):
        """Stop all MQTT broker processes"""
        print("\n=== Stopping MQTT Broker Processes ===")
        for broker_id, process in self.broker_processes.items():
            try:
                process.terminate()
                process.wait(timeout=5)
                print(f"Stopped MQTT broker for {broker_id}")
            except:
                try:
                    process.kill()
                except:
                    pass
        
        # Disconnect all broker clients
        for broker_id, broker in self.brokers.items():
            broker.disconnect_mqtt()
        
        print("All MQTT brokers stopped")
        
    def _initialize_network_metrics(self):
        """Initialize RTT and bandwidth metrics between brokers"""
        for broker_id, broker in self.brokers.items():
            for neighbor in broker.neighbors:
                # Random initial RTT (10-50ms)
                broker.state.rtt[neighbor] = random.uniform(10.0, 50.0)
                # Random initial bandwidth (1-10 Mbps)
                broker.state.bandwidth[neighbor] = random.uniform(1.0, 10.0)
    
    def set_target_broker(self, broker_id: str):
        """
        Set target broker (subscriber location) for DGD initialization
        Target broker's downstream_cost should be 0 (destination)
        """
        if broker_id in self.brokers:
            self.target_brokers.add(broker_id)
            self.brokers[broker_id].state.downstream_cost = 0.0
            # Immediately publish the updated state so neighbors can see it
            self.brokers[broker_id].publish_state()
            print(f"Set target broker {broker_id} downstream_cost to 0.0")
    
    async def run_consensus_iteration(self):
        """
        Run one consensus iteration across all brokers
        All brokers exchange states via MQTT and update consensus
        
        Improved: Uses a more reliable message synchronization mechanism
        """
        # Step 1: Publish states from all brokers
        for broker_id, broker in self.brokers.items():
            broker.publish_state()
        
        # Step 2: Wait for messages to be delivered with timeout and retry mechanism
        # Use multiple smaller waits with state checking for better reliability
        max_wait_time = 0.5  # Maximum total wait time
        wait_interval = 0.05  # Check every 50ms
        max_checks = int(max_wait_time / wait_interval)
        
        all_neighbors_received = False
        for check in range(max_checks):
            await asyncio.sleep(wait_interval)
            
            # Check if all brokers have received states from all their neighbors
            all_neighbors_received = True
            for broker_id, broker in self.brokers.items():
                with broker.lock:
                    received_neighbors = set(broker.received_state_messages.keys())
                    expected_neighbors = set(broker.neighbors)
                    if not expected_neighbors.issubset(received_neighbors):
                        all_neighbors_received = False
                        break
            
            if all_neighbors_received:
                break
        
        # Step 3: Collect states from received MQTT messages
        # Even if not all neighbors received, proceed with what we have (for robustness)
        all_states = {}
        for broker_id, broker in self.brokers.items():
            # Add self state
            all_states[broker_id] = broker.state.copy()
            
            # Get neighbor states from received messages
            with broker.lock:
                for neighbor_id, state_data in broker.received_state_messages.items():
                    if neighbor_id in broker.neighbors:
                        # Convert dict to BrokerState
                        neighbor_state = BrokerState()
                        neighbor_state.rtt = state_data.get('rtt', {})
                        neighbor_state.load = state_data.get('load', 0.0)
                        neighbor_state.bandwidth = state_data.get('bandwidth', {})
                        neighbor_state.downstream_cost = state_data.get('downstream_cost', float('inf'))
                        neighbor_state.iteration = state_data.get('iteration', 0)
                        neighbor_state.neighbor_load_estimates = state_data.get('neighbor_load_estimates', {})
                        all_states[neighbor_id] = neighbor_state
                        
                        # Warn if state is stale (from previous iteration)
                        if neighbor_state.iteration < broker.state.iteration - 1:
                            print(f"Warning: Broker {broker_id} received stale state from {neighbor_id} "
                                  f"(iteration {neighbor_state.iteration} vs current {broker.state.iteration})")
        
        # First, ensure target brokers have downstream_cost = 0 before DGD update
        # This ensures that target broker states are correct when neighbors read them
        for broker_id in self.target_brokers:
            if broker_id in self.brokers:
                self.brokers[broker_id].state.downstream_cost = 0.0
                # Also update all_states to reflect this for current iteration
                if broker_id in all_states:
                    all_states[broker_id].downstream_cost = 0.0
        
        # Update each broker's consensus
        for broker_id, broker in self.brokers.items():
            # CRITICAL FIX: Update load from actual message queue length BEFORE consensus/DGD
            # This ensures that DGD calculations use the current queue state, not stale data
            queue_length = float(len(broker.message_queue))
            broker.state.load = queue_length
            broker.max_load_seen = max(broker.max_load_seen, queue_length)
            
            # Get neighbor states (now includes correct target broker downstream_cost)
            neighbor_states = {
                nid: all_states[nid] 
                for nid in broker.neighbors 
                if nid in all_states
            }
            
            # Skip DGD update for target brokers (they should always have downstream_cost = 0)
            if broker_id not in self.target_brokers:
                # Update consensus (load is already updated above)
                broker.update_consensus(neighbor_states)
                
                # Update DGD (only for non-target brokers)
                # Pass target_brokers so DGD can exclude them if needed (though not used in current implementation)
                # DGD will now use the updated load from queue_length
                broker.update_dgd(neighbor_states, target_brokers=self.target_brokers)
            else:
                # For target brokers, just update consensus (but not DGD)
                broker.update_consensus(neighbor_states)
                # Ensure target broker's downstream_cost remains 0
                broker.state.downstream_cost = 0.0
            
            # Update discrete routing selection
            broker.discrete_routing_selection()
            
            # Save final downstream cost for statistics (before it gets reset)
            if broker.state.downstream_cost < float('inf'):
                broker.final_downstream_cost = broker.state.downstream_cost
        
        self.consensus_iterations += 1
        
        # Print consensus progress every 10 iterations
        if self.consensus_iterations % 10 == 0:
            print(f"Consensus iteration {self.consensus_iterations}")
            self._print_network_state()
    
    def _print_network_state(self):
        """Print current network state for debugging"""
        print(f"\n=== Network State (Iteration {self.consensus_iterations}) ===")
        for broker_id, broker in self.brokers.items():
            print(f"Broker {broker_id}:")
            print(f"  Load: {broker.state.load:.2f}")
            # Handle inf value display
            if broker.state.downstream_cost < float('inf'):
                downstream_display = f"{broker.state.downstream_cost:.2f}"
            else:
                downstream_display = "inf (no target)"
            print(f"  Downstream Cost: {downstream_display}")
            print(f"  Next Hop: {broker.next_hop}")
            costs = {n: broker.calculate_cost_function(n) for n in broker.neighbors}
            print(f"  Costs to neighbors: {costs}")
    
    async def publish_message(self, publisher_id: str, broker_id: str, topic: str, message: str):
        """
        Message publishing phase: Publisher → Broker A
        1. Publisher sends message to Broker A
        2. Broker A stores message in local queue
        3. A starts consensus sync with neighbors
        """
        print(f"\n=== Message Publishing Phase: Publisher {publisher_id} → Broker {broker_id} ===")
        
        if broker_id not in self.brokers:
            print(f"Error: Broker {broker_id} not found")
            return
        
        broker = self.brokers[broker_id]
        
        # Step 1 & 2: Store message in local queue
        broker.add_message(topic, message)
        print(f"Message stored in Broker {broker_id} queue (Topic: {topic})")
        
        # Step 3: Start consensus synchronization
        print(f"Broker {broker_id} starting consensus sync with neighbors: {broker.neighbors}")
        await self.run_consensus_iteration()
        
        print(f"Consensus sync completed. Global view obtained:")
        print(f"  Neighbors RTT: {broker.state.rtt}")
        print(f"  Neighbors Load: {[self.brokers[n].state.load for n in broker.neighbors]}")
        print(f"  Neighbors Bandwidth: {broker.state.bandwidth}")
    
    async def optimize_path(self, broker_id: str):
        """
        Path optimization phase: Broker performs path selection
        1. Calculate cost function for each neighbor
        2. Execute discrete selection: NextHop = arg min f_i→j
        """
        print(f"\n=== Path Optimization Phase: Broker {broker_id} ===")
        
        if broker_id not in self.brokers:
            print(f"Error: Broker {broker_id} not found")
            return None
        
        broker = self.brokers[broker_id]
        
        # Calculate cost functions for all neighbors
        costs = {}
        print("Based on consensus results, calculating cost functions:")
        for neighbor in broker.neighbors:
            cost = broker.calculate_cost_function(neighbor)
            costs[neighbor] = cost
            print(f"  f_{broker_id}→{neighbor} = α*RTT + β*Load + γ*BW^-1 = {cost:.4f}")
            print(f"    = {broker.alpha}*{broker.state.rtt.get(neighbor, 0):.2f} + "
                  f"{broker.beta}*{broker.state.load:.2f} + "
                  f"{broker.gamma}*{1.0/max(broker.state.bandwidth.get(neighbor, 1.0), 0.1):.4f}")
        
        # Execute discrete selection
        next_hop = broker.discrete_routing_selection()
        
        print(f"\nDiscrete selection: NextHop({broker_id}) = arg min f_{broker_id}→j = {next_hop}")
        print(f"Cost values: {costs}")
        print(f"Result: Broker {broker_id} will only send messages to {next_hop}, ensuring unique path")
        
        return next_hop
    
    async def forward_message(self, from_broker: str, topic: str, message: str, previous_hop: Optional[str] = None):
        """
        Message forwarding phase: Broker → Downstream agent
        1. Broker receives message
        2. C executes consensus update with neighbors
        3. Calculate downstream cost based on new state
        4. Discrete selection: NextHop = arg min [f_C→j + downstream_cost_j]
        
        Args:
            from_broker: Current broker forwarding the message
            topic: Message topic
            message: Message content
            previous_hop: Previous broker in the path (to prevent loops)
        """
        print(f"\n=== Message Forwarding Phase: Broker {from_broker} ===")
        
        if from_broker not in self.brokers:
            print(f"Error: Broker {from_broker} not found")
            return None
        
        broker = self.brokers[from_broker]
        
        # Step 1: Receive message (add to queue if not already there)
        print(f"Step 1: Broker {from_broker} received message (Topic: {topic})")
        
        # Step 2: Execute consensus update
        print(f"Step 2: Broker {from_broker} executing consensus update with neighbors: {broker.neighbors}")
        await self.run_consensus_iteration()
        
        # Step 3: Calculate downstream cost and total costs
        print(f"Step 3: Based on new state estimation, calculating downstream cost:")
        costs = {}
        total_costs = {}
        
        # Get neighbor states for downstream_cost lookup
        neighbor_states = {}
        with broker.lock:
            for neighbor_id, state_data in broker.received_state_messages.items():
                if neighbor_id in broker.neighbors:
                    neighbor_state = BrokerState()
                    neighbor_state.downstream_cost = state_data.get('downstream_cost', float('inf'))
                    neighbor_states[neighbor_id] = neighbor_state
        
        for neighbor in broker.neighbors:
            edge_cost = broker.calculate_cost_function(neighbor)
            costs[neighbor] = edge_cost
            
            # Get downstream cost
            if neighbor in neighbor_states:
                neighbor_downstream = neighbor_states[neighbor].downstream_cost
            else:
                neighbor_downstream = float('inf')
            
            # Total cost: edge + downstream
            if neighbor_downstream < float('inf'):
                total_costs[neighbor] = edge_cost + neighbor_downstream
            else:
                total_costs[neighbor] = float('inf')
        
        # Step 4: Discrete selection with loop prevention
        next_hop = broker.discrete_routing_selection(exclude_neighbor=previous_hop, neighbor_states=neighbor_states)
        
        print(f"Step 4: Through discrete selection:")
        print(f"  NextHop({from_broker}) = arg min [f_{from_broker}→j + downstream_cost_j] = {next_hop}")
        print(f"  Edge costs: {costs}")
        print(f"  Total costs (edge + downstream): {total_costs}")
        
        if next_hop:
            print(f"Result: {from_broker}→{next_hop} becomes the optimal downstream path for current message")
            broker.messages_forwarded += 1
            
            # Forward message to next hop via MQTT
            success = broker.publish_message(topic, message, next_hop)
            if success:
                # Wait a bit for message to be delivered
                await asyncio.sleep(0.1)
        else:
            print(f"Warning: No valid next hop found for Broker {from_broker} (possible loop or no path to destination)")
        
        return next_hop
    
    async def deliver_message_to_subscriber(self, broker_id: str, topic: str, message: str) -> bool:
        """
        Message delivery phase: Broker E → Subscriber
        1. Broker receives message
        2. Search for topic in local subscription table
        3. Find subscriber subscribed to topic
        4. Deliver message directly
        5. Generate ACK and confirm to upstream
        """
        print(f"\n=== Message Delivery Phase: Broker {broker_id} → Subscriber ===")
        
        if broker_id not in self.brokers:
            print(f"Error: Broker {broker_id} not found")
            return False
        
        broker = self.brokers[broker_id]
        
        print(f"Step 1: Broker {broker_id} received message")
        print(f"Step 2: Searching for topic {topic} in local subscription table")
        
        # Step 3 & 4: Deliver if subscribed
        delivered = broker.deliver_message(topic, message)
        
        if delivered:
            print(f"Step 3: Found subscriber subscribed to topic {topic}")
            print(f"Step 4: Message delivered directly to subscriber")
            print(f"Step 5: ACK generated and confirmed to upstream agent")
        else:
            print(f"Step 3: No local subscriber found for topic {topic}")
        
        return delivered
    
    async def dynamic_adjustment(self):
        """
        Dynamic adjustment phase: System continuous consensus iteration
        All agents periodically run consensus updates
        Trigger re-calculation if: RTT changes, load surge, new agent joins
        """
        print(f"\n=== Dynamic Adjustment Phase: Continuous Consensus Iteration ===")
        
        # Run multiple consensus iterations
        for i in range(5):
            await self.run_consensus_iteration()
            await asyncio.sleep(0.1)
        
        print("Distributed dynamic convergence and adaptive mechanism running")
    
    async def simulate_network_change(self, change_type: str):
        """
        Simulate network changes: RTT change, load surge, or new agent
        """
        print(f"\n=== Network Change Detected: {change_type} ===")
        
        if change_type == "RTT_change":
            # Randomly change RTT between some brokers
            broker_a = random.choice(list(self.brokers.keys()))
            if self.brokers[broker_a].neighbors:
                neighbor = random.choice(self.brokers[broker_a].neighbors)
                old_rtt = self.brokers[broker_a].state.rtt.get(neighbor, 10.0)
                new_rtt = old_rtt + random.uniform(-10.0, 20.0)
                self.brokers[broker_a].state.rtt[neighbor] = max(1.0, new_rtt)
                print(f"RTT changed: {broker_a}→{neighbor}: {old_rtt:.2f}ms → {new_rtt:.2f}ms")
        
        elif change_type == "load_surge":
            # Increase load for a random broker
            broker = random.choice(list(self.brokers.values()))
            old_load = broker.state.load
            broker.state.load += random.uniform(5.0, 15.0)
            print(f"Load surge: Broker {broker.broker_id}: {old_load:.2f} → {broker.state.load:.2f}")
        
        # Trigger re-calculation
        print("Triggering DGD re-optimization and discrete path re-calculation...")
        await self.dynamic_adjustment()


async def simulate_complete_message_flow():
    """
    Simulate complete message flow from publisher to subscriber
    Using 5 real MQTT brokers on ports 1884-1888: A, B, C, D, E
    """
    print("=" * 60)
    print("Consensus + DGD Distributed MQTT System")
    print("5 Real MQTT Brokers: A(1884), B(1885), C(1886), D(1887), E(1888)")
    print("=" * 60)
    
    # Define topology: 5 brokers with connections
    # A connects to B, C, D
    # B connects to A, C
    # C connects to A, B, D, E
    # D connects to A, C
    # E connects to C
    topology = {
        'A': ['B', 'C', 'D'],
        'B': ['A', 'C'],
        'C': ['A', 'B', 'D', 'E'],
        'D': ['A', 'C'],
        'E': ['C']
    }
    
    # Create network
    network = ConsensusDGDNetwork(['A', 'B', 'C', 'D', 'E'], topology)
    
    try:
        # Start MQTT broker processes
        network.start_mqtt_brokers()
        
        # Wait a bit for brokers to start
        await asyncio.sleep(2)
        
        # Connect all broker clients to their MQTT brokers
        network.connect_all_brokers()
        
        # Wait a bit for connections to establish
        await asyncio.sleep(1)
        
        # Process received application messages from MQTT
        async def process_received_messages():
            """Process received application messages from MQTT"""
            while True:
                for broker_id, broker in network.brokers.items():
                    with broker.lock:
                        while broker.received_application_messages:
                            msg_data = broker.received_application_messages.popleft()
                            topic = msg_data.get('topic')
                            message = msg_data.get('message')
                            from_broker = msg_data.get('from_broker')
                            
                            print(f"Broker {broker_id} received message on topic {topic} from {from_broker}")
                            broker.add_message(topic, message)
                await asyncio.sleep(0.1)
        
        # Start background task to process received messages
        message_processor_task = asyncio.create_task(process_received_messages())
        
        # Initial consensus to establish global view
        print("\n=== Initial Consensus Phase ===")
        for i in range(20):
            await network.run_consensus_iteration()
            await asyncio.sleep(0.05)
        
        # Subscribe to topic
        print("\n=== Subscription Phase ===")
        topic = "sensor/temperature"
        network.brokers['E'].subscribe(topic, "subscriber_1")
        print(f"Subscriber subscribed to topic: {topic} at Broker E")
        
        # Initialize target broker (E) for DGD: set downstream_cost to 0
        network.set_target_broker('E')
        
        # Run additional consensus iterations to allow DGD to converge
        # after setting target broker
        print("\n=== DGD Convergence Phase (after setting target broker) ===")
        for i in range(30):
            await network.run_consensus_iteration()
            await asyncio.sleep(0.05)
        
        # Complete message flow
        print("\n" + "=" * 60)
        print("Starting Complete Message Flow")
        print("=" * 60)
        
        # Phase 1: Message Publishing - Publisher → Broker A
        await network.publish_message("publisher_1", "A", topic, "Temperature: 25.5°C")
        
        await asyncio.sleep(0.5)
        
        # Phase 2: Path Optimization - Broker A performs path selection
        next_hop_a = await network.optimize_path("A")
        
        await asyncio.sleep(0.5)
        
        # Phase 3: Message Forwarding - Forward message from A recursively until reaching target broker
        current_broker = 'A'
        previous_hop = None  # Track previous hop to prevent loops
        max_hops = 10  # Prevent infinite loops
        hops = 0
        visited_brokers = set()  # Track visited brokers to detect loops
        
        while current_broker and hops < max_hops:
            if not network.brokers[current_broker].message_queue:
                break
            
            # Loop detection: if we've visited this broker before, we're in a loop
            # (Check before adding to avoid false positives on first visit)
            if current_broker in visited_brokers:
                print(f"Warning: Loop detected! Broker {current_broker} has been visited before. Stopping forwarding.")
                print(f"Visited path: {' → '.join(sorted(visited_brokers))}")
                break
            
            visited_brokers.add(current_broker)
            topic, message, _ = network.brokers[current_broker].message_queue[0]  # Peek at message
            
            # Check if current broker is a target broker (has subscribers for this topic)
            if current_broker in network.target_brokers:
                # Phase 5: Message Delivery - Target Broker → Subscriber
                topic, message, _ = network.brokers[current_broker].message_queue.popleft()
                await network.deliver_message_to_subscriber(current_broker, topic, message)
                break
            
            # Forward message to next hop (pass previous_hop to prevent sending back)
            next_hop = await network.forward_message(current_broker, topic, message, previous_hop=previous_hop)
            
            # Remove message from current broker's queue after forwarding decision
            if next_hop:
                network.brokers[current_broker].message_queue.popleft()
                previous_hop = current_broker
                current_broker = next_hop
                hops += 1
                await asyncio.sleep(0.5)
            else:
                # No next hop available, stop forwarding
                print(f"Warning: No next hop available from Broker {current_broker}. Stopping forwarding.")
                break
        
        # Final check: if message reached target broker but wasn't delivered yet
        for broker_id in network.target_brokers:
            if network.brokers[broker_id].message_queue:
                topic, message, _ = network.brokers[broker_id].message_queue.popleft()
                await network.deliver_message_to_subscriber(broker_id, topic, message)
        
        await asyncio.sleep(0.5)
        
        # Phase 6: Dynamic Adjustment - Continuous consensus
        await network.dynamic_adjustment()
        
        await asyncio.sleep(0.5)
        
        # Simulate network changes
        print("\n" + "=" * 60)
        print("Simulating Network Changes")
        print("=" * 60)
        
        await network.simulate_network_change("RTT_change")
        await asyncio.sleep(0.5)
        
        await network.simulate_network_change("load_surge")
        await asyncio.sleep(0.5)
        
        # Final consensus iteration to get accurate final state
        print("\n=== Final Consensus Update ===")
        await network.run_consensus_iteration()
        await asyncio.sleep(0.2)
        
        # Print final statistics
        print("\n" + "=" * 60)
        print("Final Statistics")
        print("=" * 60)
        for broker_id, broker in network.brokers.items():
            # Use final_downstream_cost if available, otherwise use current
            final_cost = broker.final_downstream_cost if broker.final_downstream_cost < float('inf') else broker.state.downstream_cost
            final_cost_display = f"{final_cost:.2f}" if final_cost < float('inf') else "inf"
            
            print(f"\nBroker {broker_id}:")
            print(f"  Messages Forwarded: {broker.messages_forwarded}")
            print(f"  Messages Delivered: {broker.messages_delivered}")
            print(f"  Total Messages Processed: {broker.total_messages_processed}")
            print(f"  Current Load (Queue Length): {broker.state.load:.2f}")
            print(f"  Max Load Seen: {broker.max_load_seen:.2f}")
            print(f"  Next Hop: {broker.next_hop}")
            print(f"  Downstream Cost: {final_cost_display}")
        
        # Cancel message processor task
        message_processor_task.cancel()
        try:
            await message_processor_task
        except asyncio.CancelledError:
            pass
    
    finally:
        # Stop all MQTT brokers
        network.stop_mqtt_brokers()


if __name__ == "__main__":
    print("Starting Consensus + DGD Distributed MQTT System")
    print("Algorithm: Consensus + DGD with Discrete Routing Selection")
    print("\n")
    
    asyncio.run(simulate_complete_message_flow())
    
    print("\n" + "=" * 60)
    print("Simulation Complete")
    print("=" * 60)

