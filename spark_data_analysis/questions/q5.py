import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, avg, max, stddev

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import (
    job_events,
    task_events,
)


def q5(ss: SparkSession):
    je = job_events(ss)
    te = task_events(ss)

    dm = (  # Different Machines
        je.join(te, je.JobID == te.JobID)
        .groupBy(je.JobID)
        .agg(approx_count_distinct(te.MachineID).alias("DifferentMachines"))
    )

    if os.environ.get("SPARK_DATA_ANALYSIS_PLOT") == "true":
        import matplotlib.pyplot as plt

        # Filter out the outliers because they get very big, FIXME: why are the
        # numbers different fromthe aggregate results?
        # TODO: choose a figure size, currently it's too tall height-wise
        dm.toPandas()["DifferentMachines"].plot.box(showfliers=False, vert=False)
        plt.savefig(f"{IMGS_PATH}/different_machines_per_job.svg")
        plt.close()

    results = (
        dm.agg(
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
