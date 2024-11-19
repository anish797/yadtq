#!/usr/bin/env python3

import sys
from module import generate_task_id, save_task_to_redis, send_task_to_kafka, clear_redis_tasks

topic = sys.argv[1]
workers = sys.argv[2]

def main():
    clear_redis_tasks()
    for line in sys.stdin:
        line = line.strip().split(",")
        if line[0] != "EOF":
            task_id = generate_task_id()
            task_data = {
                "task-id": task_id,
                "task": line[0],
                "args": [int(line[1]), int(line[2])]
            }
            save_task_to_redis(task_id)
            send_task_to_kafka(topic, task_data)
            print(f"Task {task_id} created and sent to Kafka.")
        elif line[0] == "EOF":
            for partition in range(int(workers)):
                send_task_to_kafka(topic, "stop")
            break

if __name__ == "__main__":
    main()
