#!/usr/bin/env python3

import sys
import threading
import time
import uuid
from module import (
    kafka_consumer,
    send_heartbeat,
    update_task_status,
)

topic = sys.argv[1]
worker_id = str(uuid.uuid4())

MAX_RETRIES = 3
RETRY_DELAY = 5

def process_task(task_data):
    task_id = task_data["task-id"]
    task = task_data["task"]
    args = task_data["args"]
    x, y = args[0], args[1]

    update_task_status(task_id, "processing")

    retries = 0
    while retries < MAX_RETRIES:
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
                raise ValueError(f"Invalid task: {task}")

            update_task_status(task_id, "success", result)
            print(f"Task {task_id} completed successfully: {result}")
            return
        except Exception as e:
            retries += 1
            print(f"Task {task_id} failed: {str(e)}. Retry {retries}/{MAX_RETRIES}")
            if retries < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                update_task_status(task_id, "failed", str(e))
                print(f"Task {task_id} permanently failed.")

def main():
    consumer = kafka_consumer(topic, group_id="worker-group")

    heartbeat_thread = threading.Thread(target=send_heartbeat, args=(worker_id,), daemon=True)
    heartbeat_thread.start()

    try:
        for message in consumer:
            if message.value == "stop":
                print(f"Worker {worker_id} shutting down.")
                break

            task_data = message.value
            process_task(task_data)
    except KeyboardInterrupt:
        print(f"Worker {worker_id} shutting down.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
