#!/usr/bin/env python3

import redis
import time

redis_client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)

def monitor_tasks():
    print("Monitoring tasks in real-time...\n")
    try:
        while True:
            task_keys = redis_client.keys("task:*")
            if not task_keys:
                print("No tasks found.")
            else:
                for task_key in task_keys:
                    task_data = redis_client.hgetall(task_key)
                    print(f"Task ID: {task_key.split(':')[1]}")
                    print(f"  Status: {task_data['status']}")
                    print(f"  Result: {task_data['result']}")
                    print("-" * 40)
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    task_keys = redis_client.keys("task:*")
    for key in task_keys:
        redis_client.delete(key)
        print(f"Deleted task: {key}")
    monitor_tasks()
