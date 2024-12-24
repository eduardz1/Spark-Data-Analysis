#import "template.typ": template, eqcolumns

#show: template.with(
  title: [Spark Data Analysis],
  subtitle: [Report for the course of Large Scale Data Processing and Distributed Systems],
  authors: ("Eduard Occhipinti", "Domenico Di Stasio"),
)

= General Analyses

#text(
  fill: red,
  [TODO: ask about which IDs are LongType, how do you plot without converting to pandas DataFrame? You can't. Check if aggregations work with null types, check if reducing the dataset with a select of only the useful columns proves advantegous, check if it's correct to calculate correlation between booleans by casting them to double],
)

+ q1

+ q2
+ q3
+ q4
+ "#underline[In general, do tasks from the same job run on the same machine?]" No, in analyzing the distribution of tasks across machines for a given job (by combining the `job_events` and `task_events` datasets), we found that tasks from the same job are distributed across multiple machines. In general we found that, on average, tasks from the same job are distributed across 88 machines. With a maximum of 9170 different machines for a single job and a standard deviation of 623. Given the very high standard deviation, it makes more sense to look at the median, 4. #figure(image("imgs/different_machines_per_job.svg"), caption: [Distribution of the number of different machines for each job, notice that outliers were excluded because they tend to get particularly high.])
+ "#underline[Are the tasks that request the more resources the one that consume the more resources?]" In general, by calculating the correlation between resource usage and resource request, where the resource usage dataframe is calculated by computing the average of `CPURate`, `CanonicalMemoryUsage` and `LocalDiskSpaceUsage` for each task (uniquely identified by the tuple `(job_id, task_index)`) in the `task_usage` table, and the resource request dataframe is calculated by averaging the `CPURequest`, `MemoryRequest` and `DiskSpaceRequest` for each task in the `task_events` table, we found that:
  - The correlation between the CPU request and the CPU usage is moderate, at 0.4345.

  - The correlation between the memory request and the memory usage is high, at 0.9030.
  - The correlation between the disk space request and the disk space usage is low, at 0.1520.
+ "#underline[Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?]" There doesn´t seem to be any correlation between task eviction rate and spikes of memory usage. We define a "spike" as a value of "Maximum Memory Usage" greater than the $95^"th"$ percentile. The correlation is 0.06, when plotting the ratios of task eviction compared to the other event types, we even see a decrease in task evictions compared to "normal" tasks. #figure(
    image("imgs/memory_eviction_rate_comparison.svg"),
    caption: [Eviction rate comparison for tasks with normal memory usage compared to ones that have a peak in memory usage.],
  )

= Performance Tuning

If not otherwise specified, benchmarks are perfomed on a machine with the following specifications:

#align(center)[
  #table(
    columns: 6,
    table.header([CPU], [Memory], [OS], [Kernel], [PySpark], [Network]),
    [11th Gen Intel i5-11400F \ (12) \@ 4.400GHz],
    [46757MiB],
    [Ubuntu 20.10],
    [6.11.0-8-generic],
    [3.5.3],
    [1Gbps],
  )
]

== Caching

Given that we read the dataset directly from the server using the Google Cloud Storage API Connector, we decided to cache the `DataFrame` objects that result from each table. For example, the table `task_constraints` would be initialized in the following way:

```python
tc = task_constraints(spark_session)
```

Due to the function being defined as follows:

```python
from functools import cache

@cache
def job_events(ss: SparkSession, parts: int | Literal["full"] = "full") -> DataFrame:
    if parts == "full":
        parts = 500

    nums = [str(i).zfill(5) for i in range(parts)]
    nums = ",".join(nums)

    schema = (
        StructType()
        .add("Time", IntegerType(), False)
        .add("MissingInfo", IntegerType(), True)
        .add("JobID", IntegerType(), False)
        .add("EventType", IntegerType(), False)
        .add("User", StringType(), True)
        .add("SchedulingClass", IntegerType(), True)
        .add("JobName", StringType(), True)
        .add("LogicalJobName", StringType(), True)
    )
    return ss.read.schema(schema).csv(f"{BUCKET_NAME}/job_events/part-{{{nums}}}*")
```

This way, the `DataFrame` object is cached and reused in subsequent calls to the function, which is particularly useful in the context of multiple questions being run in the same script. This is different from `cache` in Spark in that it only stores the object and doesn't persist data in memory. The `cache` operation is reserved for the question level and should be used in a more targeted way.
