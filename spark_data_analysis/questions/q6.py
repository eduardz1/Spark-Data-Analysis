from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, corr

from spark_data_analysis.datasets.clusterdata_2011_2 import task_events, task_usage


def q6(ss: SparkSession):
    te = task_events(ss)
    tu = task_usage(ss)

    id = ["JobID", "TaskIndex"]  # Unique identifier for a task

    # Given that tasks_usage saves the resource usage of a task in a given time
    # frame, we need to consider the aggregation of the actual resources used

    ru = (  # Resource Usage
        tu.groupBy(id).agg(
            avg(tu.CPURate).alias("CPUUsage"),
            avg(tu.CanonicalMemoryUsage).alias("MemoryUsage"),
            avg(tu.LocalDiskSpaceUsage).alias("DiskUsage"),
        )
    )

    # Given that tasks can be started, stopped and resterted, we need to
    # consider the aggregation of the requested resources

    rr = (  # Resource Request
        te.groupBy(id).agg(
            avg(te.CPURequest).alias("CPURequest"),
            avg(te.MemoryRequest).alias("MemoryRequest"),
            avg(te.DiskSpaceRequest).alias("DiskRequest"),
        )
    )

    result = (
        ru.join(rr, id)
        .agg(
            corr(ru.CPUUsage, rr.CPURequest).alias("CPUCorrelation"),
            corr(ru.MemoryUsage, rr.MemoryRequest).alias("MemoryCorrelation"),
            corr(ru.DiskUsage, rr.DiskRequest).alias("DiskCorrelation"),
        )
        .collect()[0]
        .asDict()
    )

    print(
        f"The correlation between the requested and actual CPU usage is {result['CPUCorrelation']:.2f}. "
        f"The correlation between the requested and actual memory usage is {result['MemoryCorrelation']:.2f}. "
        f"The correlation between the requested and actual disk usage is {result['DiskCorrelation']:.2f}."
    )
