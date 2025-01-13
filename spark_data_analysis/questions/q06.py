from typing import Literal

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, corr
from rich.console import Console

from spark_data_analysis.datasets.clusterdata_2011_2 import task_events, task_usage
from spark_data_analysis.rich import table


def q06(ss: SparkSession, parts: int | Literal["full"]):
    """Question 6

    Answers the following questions:
    - Are the tasks that request the more resources the one that consume the
        more resources?

    Args:
        ss (SparkSession): Spark session
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.
    """
    console = Console()

    te = task_events(ss, parts)
    tu = task_usage(ss, parts)

    id = ["JobID", "TaskIndex"]  # Unique identifier for a task

    # Given that tasks_usage saves the resource usage of a task in a given time
    # frame, we need to consider the aggregation of the actual resources used

    ru = (  # Resource Usage
        tu.groupBy(id).agg(
            avg(tu.CPURate).alias("CPUUsage"),
            avg(tu.CanonicalMemoryUsage).alias("MemoryUsage"),
            avg(tu.LocalDiskSpaceUsage).alias("DiskUsage"),
        )
    )

    # Given that tasks can be started, stopped and resterted, we need to
    # consider the aggregation of the requested resources

    rr = (  # Resource Request
        te.groupBy(id).agg(
            avg(te.CPURequest).alias("CPURequest"),
            avg(te.MemoryRequest).alias("MemoryRequest"),
            avg(te.DiskSpaceRequest).alias("DiskRequest"),
        )
    )

    results = (
        ru.join(rr, id)
        .agg(
            corr(ru.CPUUsage, rr.CPURequest).alias("CPU Correlation"),
            corr(ru.MemoryUsage, rr.MemoryRequest).alias("Memory Correlation"),
            corr(ru.DiskUsage, rr.DiskRequest).alias("Disk Correlation"),
        )
        .collect()[0]
        .asDict()
    )

    table(console, "Resource Usage vs Resource Request", results)
