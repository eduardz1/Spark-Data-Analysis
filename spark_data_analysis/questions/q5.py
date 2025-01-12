import os
from typing import Literal

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, avg, max, median, stddev
from rich.console import Console

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import (
    JobEvents,
    TaskEvents,
    job_events,
    task_events,
)
from spark_data_analysis.rich import table


def q5(ss: SparkSession, parts: int | Literal["full"]):
    """Question 5

    Answers the following questions:
    - In general, do tasks from the same job run on the same machine?

    Args:
        ss (SparkSession): Spark session
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.
    """
    console = Console()

    je = job_events(ss, parts)
    te = task_events(ss, parts)

    dm = (  # Different Machines
        je.join(te, [JobEvents.JOB_ID.value, TaskEvents.JOB_ID.value])
        .groupBy(JobEvents.JOB_ID.value)
        .agg(approx_count_distinct(te.MachineID).alias("DifferentMachines"))
    )

    if os.environ.get("SPARK_DATA_ANALYSIS_PLOT") == "true":
        import matplotlib.pyplot as plt

        plt.figure(figsize=(10, 2))

        dm.toPandas()["DifferentMachines"].plot.box(showfliers=False, vert=False)

        plt.ylabel("")
        ax = plt.gca()
        ax.axes.yaxis.set_visible(False)  # type: ignore
        plt.savefig(f"{IMGS_PATH}/different_machines_per_job.svg")
        plt.close()

    results = (
        dm.agg(
            avg("DifferentMachines").alias("Average"),
            max("DifferentMachines").alias("Max"),
            stddev("DifferentMachines").alias("Standard Deviation"),
            median("DifferentMachines").alias("Median"),
        )
        .collect()[0]
        .asDict()
    )

    table(console, "Different Machines per Job", results)
