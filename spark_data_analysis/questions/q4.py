from typing import Literal

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from rich.console import Console

from spark_data_analysis.datasets.clusterdata_2011_2 import (
    TaskEvents,
    task_events,
)
from spark_data_analysis.rich import table


def q4(ss: SparkSession, parts: int | Literal["full"]):
    """Question 4

    Answers the following questions:
    - Do tasks with a low scheduling class have a higher probability of being evicted?

    Args:
        ss (SparkSession): Spark session
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.
    """
    console = Console()

    te = task_events(ss, parts)

    evicted_tasks = te.filter(te.EventType == 2)
    evicted_tasks_per_class = (
        evicted_tasks.groupBy(TaskEvents.SCHEDULING_CLASS.value)
        .count()
        .alias("Evicted")
    )

    total_tasks_per_class = (
        te.groupBy(TaskEvents.SCHEDULING_CLASS.value).count().alias("Total")
    )

    eviction_probability = (
        evicted_tasks_per_class.join(
            total_tasks_per_class, TaskEvents.SCHEDULING_CLASS.value
        )
        .select(
            TaskEvents.SCHEDULING_CLASS.value,
            (col("Evicted.count") / col("Total.count")).alias("EvictionProbability"),
        )
        .orderBy(TaskEvents.SCHEDULING_CLASS.value)
    )

    eviction_prob_dict = {
        str(row[TaskEvents.SCHEDULING_CLASS.value]): row["EvictionProbability"]
        for row in eviction_probability.collect()
    }

    table(console, "Eviction Probability by Scheduling Class", eviction_prob_dict)
