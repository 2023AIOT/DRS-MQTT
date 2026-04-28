"""
- Topic Security Association (SA)
- Key derivation (HKDF) -> per-level keys
- AES-GCM 端到端加密
- 子话题路由（broker 仅路由，不解密）
- Publisher (动态等级选择) 与 Subscriber (派生密钥并解密)

"""
from dataclasses import dataclass
from typing import Tuple, Optional, Callable, Dict, List
import os
import asyncio
import time
import re

try:
    import psutil
except Exception:
    psutil = None  # 若未安装，动态选择会退化为固定策略

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
import argparse
import subprocess
import tempfile
import socket
import sys
import json
try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None


@dataclass
class TopicSecurityAssociation:
    topic: str
    priority: str
    security_range: Tuple[int, int]
    master_key: bytes

    @staticmethod
    def generate_master_key(length: int = 32) -> bytes:
        return os.urandom(length)


class KeyManager:
    def __init__(self, master_key: bytes):
        if not isinstance(master_key, (bytes, bytearray)):
            raise TypeError("master_key must be bytes")
        self.master_key = bytes(master_key)

    def derive_level_key(self, level: int, info: Optional[bytes] = None, length: int = 32) -> bytes:
        level_info = (info or b"") + f"level:{level}".encode("utf-8")
        hkdf = HKDF(algorithm=hashes.SHA256(), length=length, salt=None, info=level_info)
        return hkdf.derive(self.master_key)


def encrypt_aes_gcm(key: bytes, plaintext: bytes, associated_data: Optional[bytes] = None) -> bytes:
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ct = aesgcm.encrypt(nonce, plaintext, associated_data)
    return nonce + ct


def decrypt_aes_gcm(key: bytes, payload: bytes, associated_data: Optional[bytes] = None) -> bytes:
    if len(payload) < 12 + 16:
        raise ValueError("payload too short")
    nonce = payload[:12]
    ct = payload[12:]
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, ct, associated_data)


# -----------------------
# In-memory Broker
# -----------------------
class Message:
    def __init__(self, topic: str, payload: bytes):
        self.topic = topic
        self.payload = payload


class InMemoryBroker:
    def __init__(self):
        self._subs: Dict[str, List[Callable[[Message], None]]] = {}
        self._lock = asyncio.Lock()

    async def publish(self, topic: str, payload: bytes) -> None:
        msg = Message(topic=topic, payload=payload)
        async with self._lock:
            for prefix, cbs in list(self._subs.items()):
                if topic.startswith(prefix):
                    for cb in list(cbs):
                        asyncio.get_event_loop().call_soon(cb, msg)

    async def subscribe(self, topic_prefix: str, callback: Callable[[Message], None]) -> None:
        async with self._lock:
            self._subs.setdefault(topic_prefix, []).append(callback)

    async def unsubscribe(self, topic_prefix: str, callback: Callable[[Message], None]) -> None:
        async with self._lock:
            if topic_prefix in self._subs:
                try:
                    self._subs[topic_prefix].remove(callback)
                except ValueError:
                    pass


# -----------------------
# Publisher
# -----------------------
class DynamicLevelSelector:
    def __init__(self, sa: TopicSecurityAssociation):
        self.sa = sa

    def select_level(self) -> int:
        lmin, lmax = self.sa.security_range
        if psutil is None:
            # fallback: return min for deterministic demo
            return lmin
        cpu = psutil.cpu_percent(interval=0.05)
        if cpu < 40:
            return lmin
        if cpu < 80:
            return min(lmax, max(lmin, (lmin + lmax) // 2))
        return lmax


class Publisher:
    def __init__(self, broker: InMemoryBroker, sa: TopicSecurityAssociation):
        self.broker = broker
        self.sa = sa
        self.km = KeyManager(sa.master_key)

    async def publish(self, payload: bytes, associated_data: Optional[bytes] = None) -> None:
        selector = DynamicLevelSelector(self.sa)
        level = selector.select_level()
        key = self.km.derive_level_key(level)
        ciphertext = encrypt_aes_gcm(key, payload, associated_data=associated_data)
        sub_topic = f"{self.sa.topic}/stl{level}"
        await self.broker.publish(sub_topic, ciphertext)

    async def periodic_publish(self, payload_factory: Callable[[], bytes], interval: float = 0.5, stop_after: Optional[float] = None):
        start = time.time()
        while True:
            payload = payload_factory()
            await self.publish(payload)
            await asyncio.sleep(interval)
            if stop_after is not None and (time.time() - start) >= stop_after:
                break


# -----------------------
# Subscriber
# -----------------------
_STL_RE = re.compile(r".*/stl(\d+)$")


class Subscriber:
    def __init__(self, broker: InMemoryBroker, sa: TopicSecurityAssociation, on_message: Optional[Callable[[bytes], None]] = None):
        self.broker = broker
        self.sa = sa
        self.km = KeyManager(sa.master_key)
        self.on_message = on_message or (lambda payload: None)

    async def start(self):
        prefix = f"{self.sa.topic}/stl"
        await self.broker.subscribe(prefix, self._on_broker_message)

    def _on_broker_message(self, msg: Message):
        topic = msg.topic
        m = _STL_RE.match(topic)
        if not m:
            return
        level = int(m.group(1))
        key = self.km.derive_level_key(level)
        try:
            plain = decrypt_aes_gcm(key, msg.payload)
        except Exception:
            return
        try:
            self.on_message(plain)
        except Exception:
            return


# -----------------------
# Demo
# -----------------------
async def run_demo(duration: float = 5.0):
    broker = InMemoryBroker()
    master = TopicSecurityAssociation.generate_master_key()
    sa = TopicSecurityAssociation(topic="sensors/temperature", priority="MEDIUM", security_range=(1, 4), master_key=master)

    def on_msg(plain: bytes):
        print(f"[subscriber] received plaintext: {plain.decode('utf-8', errors='replace')}")

    subscriber = Subscriber(broker, sa, on_message=on_msg)
    await subscriber.start()

    publisher = Publisher(broker, sa)

    counter = {"n": 0}
    def payload_factory():
        counter["n"] += 1
        return f"msg-{counter['n']}".encode("utf-8")

    task = asyncio.create_task(publisher.periodic_publish(payload_factory, interval=0.5, stop_after=duration))
    await task
    await asyncio.sleep(0.2)


async def _start_mosquitto_brokers(port_map: Dict[str, int]) -> Dict[str, subprocess.Popen]:
    """Start mosquitto instances for each broker id. Returns dict id->Popen."""
    processes: Dict[str, subprocess.Popen] = {}
    mosquitto_cmd = "mosquitto"
    # try to find mosquitto in common locations on Windows
    possible = [
        mosquitto_cmd,
        r"C:\Program Files\mosquitto\mosquitto.exe",
        r"C:\mosquitto\mosquitto.exe",
        "/usr/sbin/mosquitto",
        "/usr/local/sbin/mosquitto",
    ]
    found = None
    for p in possible:
        try:
            subprocess.run([p, "-h"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=1)
            found = p
            break
        except Exception:
            continue
    if found is None:
        print("[distributed] Mosquitto not found in PATH. Please install mosquitto or run in InMemory mode.")
        return processes

    for bid, port in port_map.items():
        # create temp config with listener port
        cfg = tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False)
        cfg.write(f"listener {port}\nallow_anonymous true\n")
        cfg.flush()
        cfg.close()
        try:
            if sys.platform.startswith("win") and found.lower().endswith(".exe"):
                proc = subprocess.Popen([found, "-c", cfg.name, "-p", str(port)], creationflags=subprocess.CREATE_NEW_CONSOLE)
            else:
                proc = subprocess.Popen([found, "-c", cfg.name, "-p", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes[bid] = proc
            # give broker time to start
            time.sleep(0.2)
            print(f"[distributed] Started mosquitto for {bid} on port {port} (pid={proc.pid})")
        except Exception as e:
            print(f"[distributed] Failed to start mosquitto for {bid} on port {port}: {e}")
    return processes


def _stop_mosquitto_brokers(processes: Dict[str, subprocess.Popen]):
    for bid, proc in processes.items():
        try:
            proc.terminate()
            proc.wait(timeout=2)
            print(f"[distributed] Stopped mosquitto for {bid}")
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


async def run_distributed_demo(duration: float = 5.0):
    """Run demo using real MQTT brokers (mosquitto) on ports 1884-1888 representing A-E."""
    if mqtt is None:
        print("[distributed] paho-mqtt not available. Install `paho-mqtt` to run distributed demo.")
        return

    port_map = {"A": 1884, "B": 1885, "C": 1886, "D": 1887, "E": 1888}
    processes = await _start_mosquitto_brokers(port_map)

    # shared SA
    master = TopicSecurityAssociation.generate_master_key()
    sa = TopicSecurityAssociation(topic="sensors/temperature", priority="MEDIUM", security_range=(1, 4), master_key=master)
    km = KeyManager(sa.master_key)

    # subscriber connected to broker E
    received = {"n": 0}

    def on_sub_message(client, userdata, msg):
        # msg.payload is bytes
        topic = msg.topic
        m = _STL_RE.match(topic)
        if not m:
            return
        level = int(m.group(1))
        key = km.derive_level_key(level)
        try:
            plain = decrypt_aes_gcm(key, msg.payload)
        except Exception:
            print("[distributed][subscriber] decryption failed or wrong key")
            return
        received["n"] += 1
        print(f"[distributed][subscriber] got: {plain.decode('utf-8', errors='replace')}")

    sub_client = mqtt.Client(client_id=f"dlst_sub_E_{int(time.time())}")
    sub_client.on_message = on_sub_message
    sub_client.connect("localhost", port_map["E"], 60)
    sub_client.loop_start()
    # subscribe to all stl levels
    sub_client.subscribe(f"{sa.topic}/stl+")

    # publisher connected to broker A
    pub_client = mqtt.Client(client_id=f"dlst_pub_A_{int(time.time())}")
    pub_client.connect("localhost", port_map["A"], 60)
    pub_client.loop_start()

    selector = DynamicLevelSelector(sa)

    start = time.time()
    counter = 0
    try:
        while True:
            level = selector.select_level()
            key = km.derive_level_key(level)
            counter += 1
            payload = f"msg-{counter}".encode("utf-8")
            ct = encrypt_aes_gcm(key, payload)
            topic = f"{sa.topic}/stl{level}"
            # publish raw bytes
            pub_client.publish(topic, ct, qos=1)
            await asyncio.sleep(0.5)
            if (time.time() - start) >= duration:
                break
    finally:
        try:
            pub_client.loop_stop()
            pub_client.disconnect()
        except Exception:
            pass
        try:
            sub_client.loop_stop()
            sub_client.disconnect()
        except Exception:
            pass
        # stop mosquitto processes
        _stop_mosquitto_brokers(processes)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DLST-MQTT demo (in-memory or distributed)")
    parser.add_argument("--distributed", action="store_true", help="use real mosquitto brokers on ports 1884-1888")
    parser.add_argument("--duration", type=float, default=5.0, help="duration seconds")
    args = parser.parse_args()
    if args.distributed:
        asyncio.run(run_distributed_demo(duration=args.duration))
    else:
        asyncio.run(run_demo(args.duration))

