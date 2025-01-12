import os
from typing import Literal

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, corr, count, percentile_approx, sum
from pyspark.sql.window import Window

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import task_events, task_usage


def q7(ss: SparkSession, parts: int | Literal["full"]):
    """Question 7

    Answers the following questions:
    - Can we observe correlations between peaks of high resource consumption on
      some machines and task eviction events?

    Args:
        ss (SparkSession): Spark session
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.
    """
    te = task_events(ss, parts)
    tu = task_usage(ss, parts)

    id = ["JobID", "TaskIndex"]  # Unique identifier for a task

    peaks = (
        tu.withColumn(
            "Percentile95",  # I define a peak as a value that is above the 95th percentile
            percentile_approx(tu.MaximumMemoryUsage, 0.95).over(Window.partitionBy(id)),
        )
        .withColumn("IsPeak", tu.MaximumMemoryUsage > col("Percentile95"))
        .join(te, id)
    )

    rates = (
        peaks.groupBy("IsPeak")
        .agg(
            count("*").alias("TotalTasks"),
            sum((col("EventType") == 2).cast("int")).alias("EvictedTasks"),
        )
        .withColumn("EvictionRate", col("EvictedTasks") / col("TotalTasks"))
    ).collect()

    corrs = peaks.agg(
        corr(
            col("IsPeak").cast("double"), (col("EventType") == 2).cast("double")
        ).alias("Correlation")
    ).collect()[0]["Correlation"]

    if os.environ.get("SPARK_DATA_ANALYSIS_PLOT") == "true":
        import matplotlib.pyplot as plt

        plt.figure(figsize=(10, 4))
        labels = ["Normal Memory Usage", "Peaks in Memory Usage"]
        eviction_rates = [row["EvictionRate"] for row in rates]

        plt.bar(labels, eviction_rates)
        plt.title(f"Correlation: {corrs:.2f}")
        plt.ylabel("Eviction Rate")
        plt.ylim(0, max(eviction_rates) * 1.2)

        # Add percentage labels
        for i, rate in enumerate(eviction_rates):
            plt.text(i, rate, f"{rate:.1%}", ha="center", va="bottom")

        plt.savefig(f"{IMGS_PATH}/memory_eviction_rate_comparison.svg")
        plt.close()

    print(
        f"The correlation between high peak memory usage and task evition is {corrs:.2f}."
    )
