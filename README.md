# yadtq

## steps to run

1. start kafka and create topic

   1. /usr/local/kafka/bin/kafka-topics.sh --create --topic topic1 --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092

   2. /usr/local/kafka/bin/kafka-topics.sh --create --topic topic2 --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

2. open 10 terminals/3 terminals and run the workers

3. task status - watch -n 2 "redis-cli KEYS 'task:\*' | xargs -n 1 -I{} sh -c 'echo {}; redis-cli HGETALL {}'"

4. worker status - watch -n 2 "redis-cli KEYS 'worker:\*:heartbeat' | xargs -n 1 -I{} sh -c 'echo {}; redis-cli TTL {}'"

5. for input_5k run cat input_5k.csv | ./client.py topic1 10

6. for failure input run cat input.csv | ./client.py topic2 3
