"""
Streaming layer — Kafka consumer for GoldenGate CDC events.

Gracefully degrades to Oracle replica polling if Kafka is not configured
(KAFKA_BROKERS env var is empty).
"""
