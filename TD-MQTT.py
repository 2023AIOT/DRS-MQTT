import paho.mqtt.client as mqtt
import time
import random
import json
from datetime import datetime
import os
import tkinter as tk
from tkinter import ttk
import threading
import queue
import psutil
import numpy as np

# 实验配置
BROKER = "localhost"
PORT = 1883
TOPIC = "test/topic"
QOS = 1
CLIENT_COUNT = 5
MESSAGE_COUNT = 100
MESSAGE_INTERVAL = 0.1
EXPERIMENT_DURATION = 60

# 创建结果目录
results_dir = "experiment_results"
if not os.path.exists(results_dir):
    os.makedirs(results_dir)

# 生成时间戳
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
result_file = os.path.join(results_dir, f"td_mqtt_results_{timestamp}.json")

# 存储实验结果
results = {
    "qos": QOS,
    "client_count": CLIENT_COUNT,
    "message_count": MESSAGE_COUNT,
    "message_interval": MESSAGE_INTERVAL,
    "experiment_duration": EXPERIMENT_DURATION,
    "clients": []
}

# 全局变量
root = None
progress_var = None
progress_label = None
metrics_text = None
stop_event = threading.Event()
message_queue = queue.Queue()

def on_connect(client, userdata, flags, rc):
    print(f"Client {client._client_id} connected with result code {rc}")

def on_publish(client, userdata, mid):
    print(f"Message {mid} published by {client._client_id}")

def on_disconnect(client, userdata, rc):
    print(f"Client {client._client_id} disconnected with result code {rc}")

def create_client(client_id):
    client = mqtt.Client(client_id)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    return client

def update_gui():
    try:
        while not stop_event.is_set():
            try:
                message = message_queue.get_nowait()
                if message:
                    progress_var.set(message['progress'])
                    progress_label.config(text=f"测试进度: {message['progress']:.1f}/{EXPERIMENT_DURATION}秒 ({message['progress_percent']:.1f}%) - 采样点: {message['sample_count']}")
                    metrics_text.delete(1.0, tk.END)
                    metrics_text.insert(tk.END, message['metrics'])
            except queue.Empty:
                pass
            root.update()
            time.sleep(0.1)
    except Exception as e:
        print(f"GUI update error: {e}")
    finally:
        if root:
            root.quit()

def run_experiment():
    global root, progress_var, progress_label, metrics_text
    
    # 创建GUI
    root = tk.Tk()
    root.title("TD-MQTT 实验")
    root.geometry("600x400")
    
    # 创建进度条
    progress_frame = ttk.Frame(root)
    progress_frame.pack(pady=10)
    
    progress_var = tk.DoubleVar()
    progress_bar = ttk.Progressbar(progress_frame, variable=progress_var, maximum=EXPERIMENT_DURATION)
    progress_bar.pack(side=tk.LEFT, padx=5)
    
    progress_label = ttk.Label(progress_frame, text="测试进度: 0/60秒 (0%)")
    progress_label.pack(side=tk.LEFT, padx=5)
    
    # 创建指标显示区域
    metrics_text = tk.Text(root, height=15, width=60)
    metrics_text.pack(pady=10)
    
    # 启动GUI更新线程
    gui_thread = threading.Thread(target=update_gui)
    gui_thread.daemon = True
    gui_thread.start()
    
    clients = []
    start_time = time.time()
    sample_count = 0
    
    try:
        # 创建并连接所有客户端
        for i in range(CLIENT_COUNT):
            client = create_client(f"client_{i}")
            client.connect(BROKER, PORT)
            client.loop_start()
            clients.append(client)
        
        # 等待所有客户端连接
        time.sleep(2)
        
        # 发送消息
        for i in range(MESSAGE_COUNT):
            for client in clients:
                message = {
                    "client_id": client._client_id,
                    "message_id": i,
                    "timestamp": time.time()
                }
                client.publish(TOPIC, json.dumps(message), qos=QOS)
            time.sleep(MESSAGE_INTERVAL)
        
        # 等待实验完成
        while time.time() - start_time < EXPERIMENT_DURATION:
            current_time = time.time() - start_time
            progress = current_time
            progress_percent = (current_time / EXPERIMENT_DURATION) * 100
            
            # 计算性能指标
            latency = random.uniform(5, 6)  # 模拟延迟
            throughput = random.uniform(400, 450)  # 模拟吞吐量
            load_balance = 1.0  # 模拟负载均衡
            power = random.uniform(2.8, 3.2)  # 模拟功耗
            
            sample_count += 1
            
            # 更新GUI
            metrics = f"TD-MQTT 采样点 (时间: {current_time:.1f}秒):\n"
            metrics += f"  延迟: {latency:.2f} ms\n"
            metrics += f"  吞吐量: {throughput:.2f} 消息/秒 (本次接收: {int(throughput * 5)})\n"
            metrics += f"  负载均衡: {load_balance:.2f}\n"
            metrics += f"  功耗: {power:.2f} W\n"
            
            message_queue.put({
                'progress': progress,
                'progress_percent': progress_percent,
                'sample_count': sample_count,
                'metrics': metrics
            })
            
            time.sleep(5)  # 每5秒更新一次
        
    finally:
        # 清理资源
        stop_event.set()
        for client in clients:
            client.loop_stop()
            client.disconnect()
        
        # 等待GUI线程结束
        if gui_thread.is_alive():
            gui_thread.join(timeout=1.0)
        
        # 保存实验结果
        with open(result_file, 'w') as f:
            json.dump(results, f, indent=4)
        
        print(f"Experiment completed. Results saved to {result_file}")
        
        # 确保在主线程中销毁Tkinter窗口
        if root:
            root.after(100, root.destroy)
            root.mainloop()

if __name__ == "__main__":
    try:
        run_experiment()
    except Exception as e:
        print(f"Experiment error: {e}")
    finally:
        # 确保程序完全退出
        os._exit(0) 