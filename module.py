#!/usr/bin/env python3

import redis
import uuid
import json
import time
from kafka import KafkaProducer, KafkaConsumer

redis_client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)

producer = KafkaProducer(
    value_serializer=lambda m: json.dumps(m).encode("ascii"),
    bootstrap_servers=["localhost:9092"]
)

def clear_redis_tasks():
    task_keys = redis_client.keys("task:*")
    if task_keys:
        redis_client.delete(*task_keys)
        print("Cleared previous tasks from Redis.")
    else:
        print("No tasks to clear.")


def generate_task_id():
    return str(uuid.uuid4())

def send_task_to_kafka(topic, task_data):
    producer.send(topic, task_data)
    producer.flush()

def save_task_to_redis(task_id, status="queued", result=""):
    redis_client.hset(f"task:{task_id}", mapping={"status": status, "result": result})

def get_task_data_from_redis(task_key):
    return redis_client.hgetall(task_key)

def update_task_status(task_id, status, result=None):
    update_data = {"status": status}
    if result is not None:
        update_data["result"] = result
    redis_client.hset(f"task:{task_id}", mapping=update_data)

def get_failed_tasks():
    task_keys = redis_client.keys("task:*")
    failed_tasks = [
        (task_key, get_task_data_from_redis(task_key))
        for task_key in task_keys
        if get_task_data_from_redis(task_key).get("status") == "failed"
    ]
    return failed_tasks

def reassign_failed_tasks(topic):
    failed_tasks = get_failed_tasks()
    for task_key, task_data in failed_tasks:
        task_id = task_key.split(":")[1]
        task_data["task-id"] = task_id
        send_task_to_kafka(topic, task_data)
        print(f"Re-assigned failed task {task_id} to Kafka.")

def kafka_consumer(topic, group_id):
    return KafkaConsumer(
        topic,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("ascii")),
        bootstrap_servers=["localhost:9092"]
    )

def send_heartbeat(worker_id, interval=5, ttl=10):
    while True:
        redis_client.set(f"worker:{worker_id}:heartbeat", "alive", ex=ttl)
        time.sleep(interval)
#!/usr/bin/env python3

import redis
import uuid
import json
import time
from kafka import KafkaProducer, KafkaConsumer

redis_client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)

producer = KafkaProducer(
    value_serializer=lambda m: json.dumps(m).encode("ascii"),
    bootstrap_servers=["localhost:9092"]
)

def clear_redis_tasks():
    task_keys = redis_client.keys("task:*")
    if task_keys:
        redis_client.delete(*task_keys)
        print("Cleared previous tasks from Redis.")
    else:
        print("No tasks to clear.")


def generate_task_id():
    return str(uuid.uuid4())

def send_task_to_kafka(topic, task_data):
    producer.send(topic, task_data)
    producer.flush()

def save_task_to_redis(task_id, status="queued", result=""):
    redis_client.hset(f"task:{task_id}", mapping={"status": status, "result": result})

def get_task_data_from_redis(task_key):
    return redis_client.hgetall(task_key)

def update_task_status(task_id, status, result=None):
    update_data = {"status": status}
    if result is not None:
        update_data["result"] = result
    redis_client.hset(f"task:{task_id}", mapping=update_data)

def get_failed_tasks():
    task_keys = redis_client.keys("task:*")
    failed_tasks = [
        (task_key, get_task_data_from_redis(task_key))
        for task_key in task_keys
        if get_task_data_from_redis(task_key).get("status") == "failed"
    ]
    return failed_tasks

def reassign_failed_tasks(topic):
    failed_tasks = get_failed_tasks()
    for task_key, task_data in failed_tasks:
        task_id = task_key.split(":")[1]
        task_data["task-id"] = task_id
        send_task_to_kafka(topic, task_data)
        print(f"Re-assigned failed task {task_id} to Kafka.")

def kafka_consumer(topic, group_id):
    return KafkaConsumer(
        topic,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("ascii")),
        bootstrap_servers=["localhost:9092"]
    )

def send_heartbeat(worker_id, interval=5, ttl=10):
    while True:
        redis_client.set(f"worker:{worker_id}:heartbeat", "alive", ex=ttl)
        time.sleep(interval)
