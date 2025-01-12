import json
from typing import Callable

from kafka import KafkaConsumer


class Consumer:
    """Consume messages from Kafka

    Attributes:
        consumer (KafkaConsumer): Kafka consumer
        topic (str): Topic to consume messages from

    Args:
        topic (str): Topic to consume messages from

    Keyword Args:
        **kwargs: Additional arguments to pass to KafkaConsumer
    """

    def __init__(self, topic: str, **kwargs) -> None:
        self.consumer = KafkaConsumer(
            **kwargs,
            group_id="spark-data-analysis",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        self.topic = topic

    def consume_stream(self, consumer_fn: Callable) -> None:
        """Consume messages from Kafka and call the consumer function

        Example:
            Create a consumer for the topic "task-usage" and print each message
            on the console.

            >>> consumer = Consumer("task-usage")
            >>> consumer.consume_stream(print)

        Args:
            consumer_fn (Callable): Function to call for each message
        """
        self.consumer.subscribe([self.topic])

        for msg in self.consumer:
            consumer_fn(msg)
