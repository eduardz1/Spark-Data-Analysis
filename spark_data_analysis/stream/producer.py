import json
import time
from typing import Callable, ParamSpec
from kafka import KafkaProducer
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

P = ParamSpec("P")


class Producer:
    """Produce messages to Kafka

    Attributes:
        producer (KafkaProducer): Kafka producer
        topic (str): Topic to produce messages to

    Args:
        topic (str): Topic to produce messages to

    Keyword Args:
        **kwargs: Additional arguments to pass to KafkaProducer
    """

    def __init__(self, topic: str, **kwargs) -> None:
        self.producer = KafkaProducer(
            **kwargs, value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.topic = topic

    def produce_stream(
        self,
        df_fn: Callable[P, DataFrame],
        sort_by: str | None,
        sleep_time: float = 0.1,
        *df_fn_args: P.args,
        **df_fn_kwargs: P.kwargs,
    ) -> None:
        """Produce messages to Kafka from a DataFrame

        Example:
            Create a producer for the topic "task-usage" and send each row of the
            DataFrame to Kafka, sorted by the "StartTime" column.

            >>> producer = Producer("task-usage")
            >>> producer.produce_stream(task_usage, "StartTime")

        Args:
            df_fn (Callable[P, DataFrame]): Function to create the DataFrame
            sort_by (str | None): Column to sort the DataFrame by, if any
            sleep_time (float, optional): Time to sleep after each message is
                sent to simulate a continuous stream. Defaults to 0.1.
        """
        df = df_fn(*df_fn_args, **df_fn_kwargs)

        if sort_by is not None:
            df.na.drop(subset=[sort_by])
            df = df.sort(col(sort_by))

        # "foreach" won't work because it would have to pickle the producer so
        # be careful with the size of the dataframe
        rows = df.collect()

        for row in rows:
            self.producer.send(self.topic, value=row.asDict())
            time.sleep(sleep_time)

        self.producer.flush()
        self.producer.close()
