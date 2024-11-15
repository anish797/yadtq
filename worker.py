#!/usr/bin/env python3

from kafka import KafkaConsumer
import redis
import sys
import json

topic = sys.argv[1]

redis_client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)

consumer = KafkaConsumer(
    topic,
    group_id="worker-group",
    value_deserializer=lambda m: json.loads(m.decode("ascii")),
    bootstrap_servers=["localhost:9092"]
)

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

        redis_client.hset(f"task:{task_id}", "status", "success")
        redis_client.hset(f"task:{task_id}", "result", result)
        print(f"Task {task_id} completed successfully: {result}")

    except Exception as e:
        redis_client.hset(f"task:{task_id}", "status", "failed")
        redis_client.hset(f"task:{task_id}", "result", str(e))
        print(f"Task {task_id} failed: {str(e)}")

for message in consumer:
    if message.value == "stop":
        break

    task_data = message.value
    process_task(task_data)
    
consumer.close()
