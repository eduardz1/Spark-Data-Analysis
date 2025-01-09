import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, avg, max, stddev

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import (
    job_events,
    task_events,
)

def q4(ss: SparkSession):
    from spark_data_analysis.datasets.clusterdata_2011_2 import task_events
    from pyspark.sql.functions import col

    te = task_events(ss)

    # Filtra attività sfrattate
    evicted_tasks = te.filter(te.EventType == "EVICT")

    # Numero totale di attività per classe
    total_tasks_per_class = te.groupBy("SchedulingClass").count().alias("Total")

    # Numero di attività sfrattate per classe
    evicted_tasks_per_class = evicted_tasks.groupBy("SchedulingClass").count().alias("Evicted")

    # Calcolo probabilità di sfratto
    eviction_probability = (
        evicted_tasks_per_class.join(total_tasks_per_class, "SchedulingClass")
        .select(
            "SchedulingClass",
            (col("Evicted.count") / col("Total.count")).alias("EvictionProbability")
        )
        .orderBy("SchedulingClass")
    )

    print("Eviction Probability by Scheduling Class:")
    for row in eviction_probability.collect():
        print(f"Scheduling Class: {row['SchedulingClass']}, Probability: {row['EvictionProbability']:.2f}")
