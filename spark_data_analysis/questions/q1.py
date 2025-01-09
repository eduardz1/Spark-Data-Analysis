import os
from rich.console import Console

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, avg, max, stddev

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import (
    job_events,
    task_events,
    machine_events
)
def q1(ss: SparkSession):
    me = machine_events(ss)
    me.show()

def qt(ss: SparkSession):
    console = Console() #to print cose
    me = machine_events(ss)
    print('Ciao')
    # Calcolo della distribuzione
    cpu_distribution = me.groupBy("CPU").count().orderBy("CPU")
    print('Ciao2')
    results = cpu_distribution.collect()
    print("CPU Distribution:")
    for row in results:
        print(f"CPU: {row['CPU']}, Count: {row['count']}")

