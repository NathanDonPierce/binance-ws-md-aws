## Kafka arbitration effectiveness

Using Kafka's offset order to rank sources captures the following latencies:
- network delivery to listener
- listener processing
- producer publishes to kafka

When Kafka offset is compared against latency measured as (listener timestamp - binance timestamp), the source rankings are not always aligned.

When comparing feeds in Grafana, the two methods can disagree on which feed to destroy:

<img width="1512" height="685" alt="kafka_arrival_vs_listener_timestamp" src="https://github.com/user-attachments/assets/5de2e8c4-b0a4-4603-8d8b-86ae55051d27" />
