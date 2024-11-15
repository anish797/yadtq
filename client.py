#!/usr/bin/env python3

from kafka import KafkaProducer
import redis
import sys
import json
import uuid

def generate_task_id():
    return str(uuid.uuid4())

topic = sys.argv[1]

redis_client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)

producer = KafkaProducer(
    value_serializer=lambda m: json.dumps(m).encode("ascii"),
    bootstrap_servers=["localhost:9092"]
)

for line in sys.stdin:
    line = line.strip().split(",")
    if line[0] != "EOF":
        task_id = generate_task_id()
        task_data = {
            "task-id": task_id,
            "task": line[0],
            "args": [int(line[1]), int(line[2])]
        }
        redis_client.hset(f"task:{task_id}", mapping={"status": "queued", "result": ""})
        producer.send(topic, task_data)
        print(f"Task {task_id} created and sent to Kafka.")
    elif line[0] == "EOF":
        partitions = 3
        for partition in range(partitions):
            producer.send(topic, "stop", partition=partition)
        break

producer.flush()
