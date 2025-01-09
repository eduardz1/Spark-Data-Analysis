import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, avg, max, stddev

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import (
    job_events,
    task_events,
)

def q2(ss: SparkSession):
    from pyspark.sql.functions import sum
    from spark_data_analysis.datasets.clusterdata_2011_2 import machine_events

    me = machine_events(ss)

    # Calcolo del tempo offline
    offline_events = me.filter(me.EventType == 1)
    total_offline_time = offline_events.agg(sum("Duration").alias("TotalOfflineTime")).collect()[0]["TotalOfflineTime"]

    # Potenza totale computazionale
    total_compute_capacity = me.select("CPU").rdd.map(lambda row: row["CPU"]).sum()

    # Percentuale persa
    percent_lost = (total_offline_time / total_compute_capacity) * 100
    print(f"Percentuale di potenza computazionale persa: {percent_lost:.2f}%")
