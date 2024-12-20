import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, corr, percentile_approx
from pyspark.sql.window import Window

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import task_events, task_usage


def q7(ss: SparkSession):
    te = task_events(ss)
    tu = task_usage(ss)

    id = ["JobID", "TaskIndex"]  # Unique identifier for a task

    percentile_95 = tu.withColumn(
        "Percentile95",  # I define a peak as a value that is above the 95th percentile
        percentile_approx(tu.MaximumMemoryUsage, 0.95).over(Window.partitionBy(id)),
    ).join(te, id)

    if os.environ.get("SPARK_DATA_ANALYSIS_PLOT") == "true":
        import matplotlib.pyplot as plt

        percentile_95.toPandas().plot.scatter(
            x="Percentile95", y="EventType", c="blue", alpha=0.5
        )
        plt.savefig(f"{IMGS_PATH}/peak_memory_usage_vs_task_eviction.svg")
        plt.close()

    result = (
        percentile_95.agg(
            corr(
                (tu.MaximumMemoryUsage > col("Percentile95")).cast("double"),
                (te.EventType == 2).cast("double"),
            ).alias("Correlation")
        )
        .collect()[0]
        .asDict()
    )

    print(
        f"The correlation between high peak memory usage and task evition is {result['Correlation']:.2f}."
    )
