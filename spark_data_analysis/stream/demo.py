import subprocess
from threading import Thread
from typing import Literal

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    from_json,
    from_unixtime,
    to_timestamp,
)
from retry import retry

from spark_data_analysis.datasets.clusterdata_2011_2 import (
    TASK_USAGE_SCHEMA,
    task_usage,
)
from spark_data_analysis.stream.producer import Producer

TOPIC = "task-usage"
BOOTSTRAP_SERVERS = "localhost:9092"


@retry(tries=5, delay=5)  # Retries in case docker has not finished starting Kafka
def recreate_topic(name: str, **kwargs):
    """Recreate a Kafka topic if it doesn't exist

    Args:
        name (str): Name of the topic
    """
    client = KafkaAdminClient(**kwargs)

    topic = NewTopic(name, num_partitions=1, replication_factor=1)

    try:
        client.create_topics([topic])
    except Exception as _:
        # TODO: should catch TopicAlreadyExistsError but the API is not available
        # yet so I'm resorting to catching all exceptions, not ideal
        pass
    finally:
        client.close()


def ensure_kafka_running():
    """Start Kafka using docker compose with a simple shell command

    Raises:
        subprocess.CalledProcessError: If the command fails
    """
    try:
        subprocess.run(
            ["docker", "compose", "up", "-d"],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print("Failed to start Kafka:", e)
        raise


def streaming_demo(
    ss: SparkSession,
    parts: int | Literal["full"],
    docker: bool = True,
):
    """Run the Spark Streaming simulation

    The simulation generates a stream of Task Usage data and calculates the
    average CPU usage every 5 seconds

    Args:
        ss (SparkSession): Spark session to use
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.
        docker (bool, optional): Whether to start Kafka using Docker. Defaults to True.
    """
    if docker:
        ensure_kafka_running()

    recreate_topic(TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS)

    producer = Producer(TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS)

    # Start producer in background
    producer_thread = Thread(
        target=producer.produce_stream,
        kwargs={"df_fn": task_usage, "ss": ss, "parts": parts, "sort_by": "StartTime"},
    )
    producer_thread.daemon = True
    producer_thread.start()

    df = (
        ss.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        df.select(
            from_json(col("value").cast("string"), TASK_USAGE_SCHEMA).alias("data")
        )
        .select("data.*")
        .withColumns(
            {
                "StartTime": to_timestamp(from_unixtime(col("StartTime"))),
                "EndTime": to_timestamp(from_unixtime(col("EndTime"))),
            }
        )
    )

    query = (
        parsed_df.agg(
            avg("CPURate").alias("AvgCPUUsage"),
        )
        .writeStream.outputMode("complete")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()
