import os
from typing import Literal

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    hour,
    max,
    min,
    to_timestamp,
    window,
)
from pyspark.sql.window import Window

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import TaskEvents, task_events


def q9(ss: SparkSession, parts: int | Literal["full"]):
    """Question 9

    Answers the following questions:
    - How do resource requests (CPU, memory) evolve over time for long-running jobs?

    Args:
        ss (SparkSession): Spark session
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.
    """
    te = task_events(ss, parts)

    resources = [  # The resources we are interested in
        TaskEvents.CPU_REQUEST.value,
        TaskEvents.MEMORY_REQUEST.value,
        TaskEvents.DISK_SPACE_REQUEST.value,
    ]

    te = te.na.drop(subset=resources)  # Drop rows with missing values
    te = te.na.drop(subset=[TaskEvents.TIME.value])  # Drop rows with missing time

    # Find long running jobs (those spanning at least 1 hour and having multiple events)
    long_running_jobs = (
        te.groupBy(TaskEvents.JOB_ID.value)
        .agg(
            (max(TaskEvents.TIME.value) - min(TaskEvents.TIME.value)).alias("Duration"),
        )
        .filter(col("Duration") > 3600)
    )

    # Join back to get resource requests over time
    resource_evolution = (
        te.join(
            long_running_jobs.select(TaskEvents.JOB_ID.value), TaskEvents.JOB_ID.value
        )
        # Normalize time to hours from start for each job
        .withColumn(
            "TimeFromStart",
            (
                to_timestamp(
                    col(TaskEvents.TIME.value)
                    - min(TaskEvents.TIME.value).over(
                        Window.partitionBy(TaskEvents.JOB_ID.value)
                    )
                )
            ),
        )
    )

    Window.partitionBy(TaskEvents.JOB_ID.value, window(col("TimeFromStart"), "1 hour"))

    hourly_averages = (
        resource_evolution.select(TaskEvents.JOB_ID.value, "TimeFromStart", *resources)
        .groupBy(TaskEvents.JOB_ID.value, window("TimeFromStart", "1 hour"))
        .agg(*[avg(col(resource)).alias(f"avg_{resource}") for resource in resources])
        .orderBy(TaskEvents.JOB_ID.value, "window")
    )

    final_evolution = hourly_averages.select(
        TaskEvents.JOB_ID.value,
        hour("window.start").alias("HourFromStart"),
        *[f"avg_{resource}" for resource in resources],
    )

    if os.environ.get("SPARK_DATA_ANALYSIS_PLOT") == "true":
        import matplotlib.pyplot as plt

        # Convert to pandas for easier plotting
        pdf = final_evolution.toPandas()

        plt.figure(figsize=(12, 4))

        for resource in resources:
            # Normalize the values to show relative changes
            avg_values = pdf.groupby("HourFromStart")[f"avg_{resource}"].mean()
            normalized = avg_values / avg_values.iloc[0]  # Divide by initial value
            plt.plot(normalized.index, normalized.values, label=resource, linewidth=2)

        plt.xlabel("Hours from Job Start")
        plt.ylabel("Relative Change (1.0 = initial value)")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"{IMGS_PATH}/normalized_resource_evolution.svg")
        plt.close()
