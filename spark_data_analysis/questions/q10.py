from typing import Literal
from pyspark.sql import SparkSession
from rich.console import Console

from spark_data_analysis.datasets.clusterdata_2019 import InstanceUsage, instance_usage
from pyspark.sql.functions import col, avg, round

from spark_data_analysis.rich import table


def q10(ss: SparkSession, parts: int | Literal["full"]):
    """Question 10

    Answers the following questions:
    - What is page cache vs assigned memory ratio? What is the average number of
    memory accesses per instruction?

    Args:
        ss (SparkSession): Spark session
        parts (int | Literal["full"]): Number of parts to load. If
            "full", load all parts.
    """
    console = Console()

    iu = instance_usage(ss, parts)

    memory_ratio = (
        iu.withColumn(
            "cache_to_assigned_ratio",
            col(InstanceUsage.PAGE_CACHE_MEMORY.value)
            / col(InstanceUsage.ASSIGNED_MEMORY.value),
        )
        .agg(
            round(avg("cache_to_assigned_ratio"), 3).alias(
                "avg_cache_to_assigned_ratio"
            ),
            round(avg(InstanceUsage.MEMORY_ACCESSES_PER_INSTRUCTION.value), 3).alias(
                "avg_memory_accesses_per_instruction"
            ),
        )
        .collect()[0]
        .asDict()
    )

    table(console, "Memory Ratio", memory_ratio)
