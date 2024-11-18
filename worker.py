#!/usr/bin/env python3

from kafka import KafkaConsumer
import redis
import sys
import json
import threading
import time
import uuid

topic = sys.argv[1]
redis_client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
consumer = KafkaConsumer(
    topic,
    group_id="worker-group",
    value_deserializer=lambda m: json.loads(m.decode("ascii")),
    bootstrap_servers=["localhost:9092"]
)

worker_id = str(uuid.uuid4())

def send_heartbeat():
    while True:
        redis_client.set(f"worker:{worker_id}:heartbeat", "alive", ex=10)
        time.sleep(5)

heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
heartbeat_thread.start()

def process_task(task_data):
    task_id = task_data["task-id"]
    task = task_data["task"]
    args = task_data["args"]
    x, y = args[0], args[1]

    redis_client.hset(f"task:{task_id}", "status", "processing")

    try:
        if task == "add":
            result = x + y
        elif task == "sub":
            result = x - y
        elif task == "mul":
            result = x * y
        elif task == "div":
            result = x / y
        else:
            raise ValueError(f"Invalid operator: {task}")

        redis_client.hset(f"task:{task_id}", mapping={"status": "success", "result": result})
        print(f"Task {task_id} completed successfully: {result}")

    except Exception as e:
        redis_client.hset(f"task:{task_id}", mapping={"status": "failed", "result": str(e)})
        print(f"Task {task_id} failed: {str(e)}")

try:
    for message in consumer:
        if message.value == "stop":
            break

        task_data = message.value
        process_task(task_data)
except KeyboardInterrupt:
    print(f"Worker {worker_id} shutting down.")
finally:
    redis_client.delete(f"worker:{worker_id}:heartbeat")
    consumer.close()
