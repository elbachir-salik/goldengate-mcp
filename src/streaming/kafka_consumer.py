"""
Kafka consumer — reads GoldenGate CDC events from configured topics.

Uses confluent-kafka-python.  If KAFKA_BROKERS is empty the consumer is
disabled and callers fall back to querying the Oracle replica directly.

Responsibilities:
    - Connect to Kafka with the configured consumer group
    - Deserialise GoldenGate Avro / JSON CDC envelope
    - Yield normalised event dicts to the get_realtime_events tool
    - Handle transient disconnects with exponential back-off (tenacity)
"""
