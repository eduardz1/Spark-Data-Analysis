from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, avg, max, stddev

from spark_data_analysis.datasets.clusterdata_2011_2 import (
    job_events,
    task_events,
)


def q5(ss: SparkSession):
    je = job_events(ss)
    te = task_events(ss)

    results = (
        je.join(te, je.JobID == te.JobID)
        .groupBy(je.JobID)
        .agg(approx_count_distinct(te.MachineID).alias("DifferentMachines"))
        .agg(
            avg("DifferentMachines").alias("AvgDifferentMachines"),
            max("DifferentMachines").alias("MaxDifferentMachines"),
            stddev("DifferentMachines").alias("StdDevDifferentMachines"),
        )
        .collect()[0]
        .asDict()
    )

    print(
        "No, in general tasks from the same job run on different machines, "
        f"with the mean being {results['AvgDifferentMachines']:.2f} different "
        f"machines per job, the maximum being {results['MaxDifferentMachines']:.2f} "
        f"and the standard deviation being {results['StdDevDifferentMachines']:.2f}."
    )
