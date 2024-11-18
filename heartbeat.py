#!/usr/bin/env python3

import redis
import time

redis_client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)

def monitor_workers():
    while True:
        worker_keys = redis_client.keys("worker:*:heartbeat")
        for worker_key in worker_keys:
            ttl = redis_client.ttl(worker_key)
            print(f"{worker_key} - TTL: {ttl} seconds")
        time.sleep(5)

print("started")
monitor_workers()
