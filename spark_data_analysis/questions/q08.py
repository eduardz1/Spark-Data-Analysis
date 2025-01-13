import os
from typing import Literal

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, from_unixtime

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import (
    TaskUsage,
    task_usage,
)


def q08(ss: SparkSession, parts: int | Literal["full"]):
    """Question 8

    Answers the following questions:
    - What is the hourly cycles per instruction measure?

    Args:
        ss (SparkSession): Spark Session
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.
    """
    tu = task_usage(ss, parts).na.drop(subset=[TaskUsage.CYCLES_PER_INSTRUCTION.value])

    hourly_cpi = (
        tu.withColumn("hour", hour(from_unixtime(TaskUsage.START_TIME.value)))
        .groupBy("hour")
        .agg(avg(TaskUsage.CYCLES_PER_INSTRUCTION.value).alias("avg_cpi"))
        .orderBy("hour")
    )
    
    if os.environ.get("SPARK_DATA_ANALYSIS_PLOT") == "true":
        import matplotlib.pyplot as plt
        import seaborn as sns

        # Plot hourly CPI pattern
        hourly_data = hourly_cpi.toPandas()
        plt.figure(figsize=(12, 4))
        sns.lineplot(data=hourly_data, x="hour", y="avg_cpi")
        plt.title("Average CPI by Hour")
        plt.xlabel("Hour of Day")
        plt.ylabel("Average CPI")
        plt.savefig(f"{IMGS_PATH}/hourly_cpi.svg")
        plt.close()
