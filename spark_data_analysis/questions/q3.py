import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, avg, max, stddev

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import (
    job_events,
    task_events,
)

def q3(ss: SparkSession):
    from spark_data_analysis.datasets.clusterdata_2011_2 import job_events, task_events

    je = job_events(ss)
    te = task_events(ss)

    # Distribuzione dei lavori
    job_distribution = je.groupBy("SchedulingClass").count().orderBy("SchedulingClass")
    print("Job Distribution by Scheduling Class:")
    for row in job_distribution.collect():
        print(f"Scheduling Class: {row['SchedulingClass']}, Jobs: {row['count']}")

    # Distribuzione delle attività
    task_distribution = te.groupBy("SchedulingClass").count().orderBy("SchedulingClass")
    print("Task Distribution by Scheduling Class:")
    for row in task_distribution.collect():
        print(f"Scheduling Class: {row['SchedulingClass']}, Tasks: {row['count']}")
