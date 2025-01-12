#import "template.typ": template, eqcolumns, codly
#import "@preview/gentle-clues:1.1.0": *

#show: template.with(
  title: [Spark Data Analysis],
  subtitle: [Report for the course of Large Scale Data Processing and Distributed Systems],
  authors: ("Eduard Occhipinti", "Domenico di Stasio"),
)

= Introduction

We decided to approach the project by writing a Python application that uses the PySpark library to analyze the Google Cluster dataset. The application runs by default locally but reads the dataset directly from the Google Cloud Storage API Connector.

In the `README.md` file, we provide instructions on how to run the application and the necessary configurations to set up the environment. The application is structured in such a way that each question is a separate module that can be run independently. The application also provides a `--plot` flag that can be used to generate plots for the questions that have a visual representation.

A summary of the commands provided by the application is as follows:

```bash
python -m spark_data_analysis --help
```

```
usage: python -m spark_data_analysis [-h] [-c] [-q] [-s SPARK_CONFIG] [-p [1-500]] {questions,streaming} ...

Either run the code for the analysis, separately for each question or run a simulation of a Spark Streaming environment. Optionally compile the report.

positional arguments:
  {questions,streaming}

options:
  -h, --help            show this help message and exit
  -c, --compile_pdf     compile the report in pdf format
  -q, --quiet           suppress additional information during code execution
  -s SPARK_CONFIG, --spark_config SPARK_CONFIG
                        path to the .ini Spark configuration file, by default it uses the local configuration
  -p [1-500], --parts [1-500]
                        number of parts to read from the Clusterdata 2011 dataset, by default it reads the full dataset
```

```bash
python -m spark_data_analysis questions --help
```

```
usage: python -m spark_data_analysis questions [-h] [--plot] (-a | -n [1-9] [[1-9] ...])

Run the Spark analysis

options:
  -h, --help            show this help message and exit
  --plot                plot the results of the analysis, by default it is disabled given the fact that accurate plots need to create huge pandas dataframes

questions:
  Choose which question to run

  -a                    run all questions
  -n [1-10] [[1-10] ...]  run specific Spark analysis parts by specifying one of more associated question numbers
```

```bash
python -m spark_data_analysis streaming --help
```

```
usage: python -m spark_data_analysis streaming [-h] [--no-docker]

Run the Spark Streaming simulation

options:
  -h, --help   show this help message and exit
  --no-docker  run the Spark Streaming simulation without Docker, in this case the program expects Kafka to be running
```

== Spark Configuration

To configure the connector we had to configure the `spark.jars` property to point to `https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar` and set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of the JSON file containing the service account key.

For the streaming demo (@streaming) we also had to set the properties:
- `fs.gs.impl` to `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem`
- `fs.AbstractFileSystem.gs.impl` to `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS`
For the streaming demo we also had to add the Kafka connetor by setting `spark.jars.packages` to `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`.

To configure the Spark environment one can also pass a `.ini` file with the desired configuration. The file should have the following structure:

```ini
[Spark]
key=value
...
```

and can be passed to the application with the `--spark_config` flag.

= General Analyses

First let's take a look at the general statistics of the dataset by anwsering a few questions:

+ "#underline[What is the distribution of the machines according to their CPU capacity?]" The distribution of the machines according to their CPU capacity is the following: #figure(
    image("imgs/cpu_dsitribution.svg"),
    caption: [Distribution of the machines according to their CPU capacity.],
  ) As we can notice the vast majority of CPUs is of type `0.5`.
+ "#underline[What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?]" The percentage of computational power lost due to maintenance is `-60.47%`

+ "#underline[What is the distribution of the number of jobs/tasks per scheduling class?]" We notice that the very few jobs are assigned to scheduling class `3` while they are distributed almost uniformely across the other three with a slight preference towards scheduling class `0`. When looking at the task distribution per scheduling class, we see again that almost no tasks are assigned to scheduling class `3` but this time the preference towards shceduling class `0` is much larger. The distributioncan be visualized in the following graph: #figure(image("imgs/job_task_distribution_by_schedulingclass.svg"), caption: [Distribution of the number of jobs per scheduling class.])
+ "#underline[Do tasks with a low scheduling class have a higher probability of being evicted?]" Based on the results, there is no clear correlation between a task's scheduling class and its likelihood of being evicted from memory. While we might intuitively expect tasks with lower priority (lower scheduling class) to be evicted more frequently, the given data does not support this assumption.#align(center)[
  #figure(
  table(
    columns: 5,
    table.header([],[0],[1],[2],[3],),
    [*Eviction Probability*],
    [3.84%],
    [6.21%],
    [2.70%],
    [5.02%]
  ), caption: [Eviction probability by Scheduling Class])]
+ "#underline[In general, do tasks from the same job run on the same machine?]" No, in analyzing the distribution of tasks across machines for a given job (by combining the `job_events` and `task_events` datasets), we found that tasks from the same job are distributed across multiple machines. In general we found that, on average, tasks from the same job are distributed across 88 machines. With a maximum of 9170 different machines for a single job and a standard deviation of 623. Given the very high standard deviation, it makes more sense to look at the median, 4. #figure(image("imgs/different_machines_per_job.svg"), caption: [Distribution of the number of different machines for each job, notice that outliers were excluded because they tend to get particularly high.])
+ "#underline[Are the tasks that request the more resources the one that consume the more resources?]" In general, by calculating the correlation between resource usage and resource request, where the resource usage dataframe is calculated by computing the average of `CPURate`, `CanonicalMemoryUsage` and `LocalDiskSpaceUsage` for each task (uniquely identified by the tuple `(job_id, task_index)`) in the `task_usage` table, and the resource request dataframe is calculated by averaging the `CPURequest`, `MemoryRequest` and `DiskSpaceRequest` for each task in the `task_events` table, we found that:
  - The correlation between the CPU request and the CPU usage is moderate, at 0.4345.

  - The correlation between the memory request and the memory usage is high, at 0.9030.
  - The correlation between the disk space request and the disk space usage is low, at 0.1520.
+ "#underline[Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?]" There doesn´t seem to be any correlation between task eviction rate and spikes of memory usage. We define a "spike" as a value of "Maximum Memory Usage" greater than the $95^"th"$ percentile. The correlation is 0.06, when plotting the ratios of task eviction compared to the other event types, we even see a decrease in task evictions compared to "normal" tasks. #figure(
    image("imgs/memory_eviction_rate_comparison.svg"),
    caption: [Eviction rate comparison for tasks with normal memory usage compared to ones that have a peak in memory usage.],
  )
+ "#underline[What is the hourly cycles per instruction measure?]" We can see the hourly cycles per instruction measure (CPI) in the following graph: #figure(image("imgs/hourly_cpi.svg"), caption: [Hourly cycles per instruction measure.])
+ "#underline[How do resource requests (CPU, memory) evolve over time for long-running jobs?]" We want to see if there is a trend in resource requests over time for long-running jobs, where a “long-running” job we defined as jobs taking more than an hour to complete. Curiously we see that CPU Requests spike in conjunction with Memory requests about one hour after the start of the job. We hypothesize that this is due to the jobs we selected requiring a long setup time. #figure(image("imgs/normalized_resource_evolution.svg"), caption: [Normalized resource requests over time for long-running jobs.])

= Performance Tuning

If not otherwise specified, benchmarks are performed on a machine with the following specifications:

#align(center)[
  #table(
    columns: 6,
    table.header([CPU], [Memory], [OS], [Kernel], [PySpark], [Network]),
    [11th Gen Intel i5-11400F \ (12) \@ 4.400GHz],
    [46757MiB],
    [Ubuntu 20.10],
    [6.11.0-8-generic],
    [3.5.4],
    [1Gbps],
  )
]

== Memory <memory>

Given that each question takes a long time to run, we will analyse performance differences on a question by question basis.

In the starting configuration question 6 (`python -m spark_data_analysis questions -n 6`) takes 1102.53 seconds (#sym.approx 18 minutes) to run. When trying to increase the memory by modifying the start configuration to `--executor-memory 16G --driver-memory 16G` the time to run the question doesn't change much and results in a run that takes 1045.60 seconds. The command used was `python -m spark_data_analysis --spark-config="spark.ini" questions -n 6 ` where the `spark.ini` file contains the following configuration:

```ini
[Spark]
spark.driver.memory=16g
spark.executor.memory=16g
```

== Caching

=== Python

Given that we read the dataset directly from the server using the Google Cloud Storage API Connector, we decided to cache the `DataFrame` objects that result from each table. For example, the table `task_constraints` would be initialized in the following way:

```python
tc = task_constraints(spark_session)
```

Due to the function being defined as follows:

#box[
  ```python
  from functools import cache

  @cache
  return ss.read.schema(JOB_EVENTS_SCHEMA).csv(
          f"{BUCKET_NAME}/job_events/{format_parts_file_regex(parts)}"
      )
  ```
]

This way, the `DataFrame` object is cached and reused in subsequent calls to the function, which is particularly useful in the context of multiple questions being run in the same script. This is different from `cache` in Spark in that it only stores the object and doesn't persist data in memory. The `cache` operation is reserved for the question level and should be used in a more targeted way.

=== Spark

We now try leveraging the Spark cache mechanism to speed up the execution of the queries. We first try with the $7^"th"$ query, where we can cache the results of the `peaks` _transformation_ for the two _actions_ below: the collection of the `rates` and of the `correlations`:

#codly(
  highlights: (
    (line: 13, start: 9, end: none, fill: yellow),
  ),
)
```python
def q7(ss: SparkSession):
    te = task_events(ss)
    tu = task_usage(ss)

    id = ["JobID", "TaskIndex"]  # Unique identifier for a task

    peaks = (
        tu.withColumn(
            "Percentile95",  # I define a peak as a value that is above the 95th percentile
            percentile_approx(tu.MaximumMemoryUsage, 0.95).over(Window.partitionBy(id)),
        )
        .withColumn("IsPeak", tu.MaximumMemoryUsage > col("Percentile95"))
        .join(te, id)
        .cache()
    )

    rates = (
        peaks.groupBy("IsPeak")
        .agg(
            count("*").alias("TotalTasks"),
            sum((col("EventType") == 2).cast("int")).alias("EvictedTasks"),
        )
        .withColumn("EvictionRate", col("EvictedTasks") / col("TotalTasks"))
    ).collect()

    corrs = peaks.agg(
        corr(
            col("IsPeak").cast("double"), (col("EventType") == 2).cast("double")
        ).alias("Correlation")
    ).collect()[0]["Correlation"]

    if os.environ.get("SPARK_DATA_ANALYSIS_PLOT") == "true":
        ... # Plotting code

    print(
        f"The correlation between high peak memory usage and task evition is {corrs:.2f}."
    )
```

#info[
  When working with Spark DataFrames, the `cache` function is an alias to the `persist(StorageLevel.MEMORY_AND_DISK)` function, however, when working with RDDs, the `cache` function is an alias to the `persist(StorageLevel.MEMORY_ONLY)`.
]

When trying to run this code without any modifications, it fails after some hours due to a `java.lang.OutOfMemoryError`. We try then to run it again with the configuration that we explored in @memory, doing so also leads to an out of memory error. Apparently, the `DataFrame` will take more than the 16GB of memory that we allocated to the executor and, given that Spark utilizes the `/tmp` directory by default to store intermediate data. To solve this problem we can specify a different directory for the temporary files to be stored in, by setting the `spark.local.dir` configuration in the `spark.ini` file.

```ini
[Spark]
spark.driver.memory=16g
spark.executor.memory=16g
spark.local.dir=.cache
```

This way, the temporary files will be stored in the `.cache` directory, which is located in the same directory as the script. The job now runs successfully, but the overhead of caching is very significant and it doesn't improve the performance of the job.

== Parallelism <parallelism>

Our next attempt to speed up Spark is by adding more executor instances, we will test the following configuration on questions 7:

```ini
[Spark]
spark.executor.instances=6
spark.executor.cores=2
spark.driver.memory=4g
spark.executor.memory=6g
```

With this configuration the time still doesn't improve, going from 2120.96 seconds to 2150.89 seconds.

== Dataframe vs RDD

Being Dataframes the standard way to work with Spark, all of our code is written with their usage in mind, however, we can try to analyze the difference in perfomance between dataframes and RDDs by taking the code from question 4. The original, dataframe-based implementation is as follows:

```python
def q4(ss: SparkSession, parts: int | Literal["full"]):
    console = Console()

    te = task_events(ss, parts)

    evicted_tasks = te.filter(te.EventType == 2)
    evicted_tasks_per_class = (
        evicted_tasks.groupBy("SchedulingClass").count().alias("Evicted")
    )

    total_tasks_per_class = te.groupBy("SchedulingClass").count().alias("Total")

    eviction_probability = (
        evicted_tasks_per_class.join(total_tasks_per_class, "SchedulingClass")
        .select(
            "SchedulingClass",
            (col("Evicted.count") / col("Total.count")).alias("EvictionProbability"),
        )
        .orderBy("SchedulingClass")
    )

    eviction_prob_dict = {
        str(row["SchedulingClass"]): row["EvictionProbability"]
        for row in eviction_probability.collect()
    }

    table(console, "Eviction Probability by Scheduling Class", eviction_prob_dict)
```

which can be compared to the RDD-based implementation:

```python
def q4_rdd(ss: SparkSession, parts: int | Literal["full"]):
    console = Console()

    te = task_events(ss, parts) # This time an RDD is returned

    evicted_tasks = te.filter(lambda x: x.EventType == 2)
    evicted_tasks_per_class = (
        evicted_tasks.map(lambda x: (x.SchedulingClass, 1)).reduceByKey(add)
    )

    total_tasks_per_class = te.map(lambda x: (x.SchedulingClass, 1)).reduceByKey(add)

    eviction_probability = (
        evicted_tasks_per_class.join(total_tasks_per_class)
        .map(lambda x: (x[0], x[1][0] / x[1][1]))
        .sortByKey()
    )

    eviction_prob_dict = eviction_probability.collectAsMap()
    eviction_prob_dict = {
        str(k): v for k, v in eviction_prob_dict.items()
    }

    table(console, "Eviction Probability by Scheduling Class", eviction_prob_dict)
```

The running time goes from around *100* seconds to more than *600*! As we wanted to demonstrate, the RDD-based implementation is significantly slower than the DataFrame-based one.

= Spark Streaming <streaming>

A spark streaming demo was written as suggested, it calculates the average CPU usage (column `CPURate`) of the tasks in the last 5 seconds and prints it to the console. The application does not require a Zookeeper instance running and will automatically attempt to start a Kafka broker through the official Docker image.

```docker
services:
  kafka:
    image: apache/kafka-native:latest
    ports:
      - "9092:9092"
```

This feature can be disabled with the `--no-docker` flag.

The producer runs as a daemon thread and sends a message every `0.1` seconds, to produce messages programmatically we utilize the `kafka-python-ng` library.

#info[You should expect the program to start after some seconds, give it some time, Spark has to first sort the data and then configure the streaming context, after it finishes setting everything up it will catch up with the data and you will see updates in real time.]

The demo can be started with the following command:

```bash
python -m spark_data_analysis -p 1 streaming
```

#tip[The flag `-p 1` is used to tell the application to use only one part of the dataset as there is no need to use the entire dataset for this demo.]

= Other Datasets

== Cluster Data 2019

In this section we will analyse the paper "Borg: the Next Generation" and apply our code to the new dataset.

=== Example of Usage of the Dataset

As an example of the usage of the dataset, we provide a tenth question which answers "#underline[What is page cache vs assigned memory ratio? What is the average number of memory accesses per instruction?]" which successfully runs on the new dataset and provides the following results #footnote[For the first 200 parts of the dataset]:

- *Page Cache vs Assigned Memory Ratio*: 0.0700
- *Average Number of Memory Accesses per Instruction*: 0.0130

The question uses the `spark_data_analysis.datasets.clusterdata_2019` module and analyses the "Instance Usage" table.

=== Borg Workload Evolution (2011-2019)

The analysis that we conducted on the paper articles reveals that Borg's workload experienced significant transformations between 2011 and 2019, highlighting key changes in job management, resource utilization, and system efficiency.

*Increased Job Submission Rate*
The job submission rate saw a remarkable 3.7x increase compared to 2011, while the number of tasks requiring scheduling grew 7x. Notably, these increases occurred without changes in cell size between 2011 and 2019. Despite this surge, the scheduler maintained consistent allocation times, demonstrating its robustness under higher workloads.

*Changes in Workload Mix*
The composition of workloads evolved, with a substantial shift from the "free" tier (low priority) to the "best-effort batch" tier (batch-queued jobs). Meanwhile, utilization for "production" tier jobs (high priority) remained stable. Workload distribution also varied significantly across different cells.

*Increased Resource Utilization*
Average utilization of CPU and memory increased, largely driven by higher consumption in the "best-effort batch" tier, which now accounts for 20% of cell capacity for both resources. This shift reflects the growing importance of batch workloads within Borg's infrastructure.

*Heavy-Tailed Job Size Distribution*
Resource consumption by jobs exhibited extreme variability, with 1% of jobs consuming over 99% of resources. This disparity required isolating smaller jobs from larger ones to maintain reasonable queue times and prevent resource bottlenecks.

*Resource Over-Commitment*
The use of statistical multiplexing led to a significant increase in resource over-commitment, where requested resource limits exceeded cell capacity. While CPU over-commitment was predominant in 2011, by 2019, memory over-allocation had risen to match CPU levels. This approach leverages the fact that most jobs do not fully utilize their requested resources.

*Introduction of Alloc Sets*
In 2019, alloc sets were introduced, enabling users to reserve resources for specific jobs. These accounted for 20% of total CPU and 18% of RAM, with 15% of jobs utilizing alloc sets, primarily in the "production" tier. This innovation improved resource predictability for critical workloads.

*Vertical Scaling with Autopilot*
Borg incorporated automatic vertical scaling through a system called Autopilot, which dynamically adjusts resource limits to close the gap between requested and actual usage. This feature enhanced resource efficiency while adapting to varying workload demands.

*Improved Resource Efficiency*
Borg achieved higher throughput on fixed machine capacity, allowing users to perform more work without a significant increase in actual machine utilization. This reflects ongoing improvements in system efficiency and workload management.

== Alibaba's Dataset Analysis and comparison with Google

From the analysis we conducted on the published papers, *Alibaba’s approach* to cluster management and machine learning workloads reveals several key features and innovations. *Alibaba Cloud* offers a *Machine Learning as a Service* (MLaaS) platform called PAI (Platform for Artificial Intelligence), which supports the entire machine learning pipeline and integrates frameworks like *TensorFlow* and *PyTorch*. A notable contribution is the release of a two-month cluster trace from a production environment with over 6,000 GPUs, showcasing a mix of *training* and *inference jobs* that span various machine learning algorithms. This trace stands out as one of the most comprehensive datasets in terms of *workload diversity* and *cluster scale*.

*Alibaba’s clusters* are heterogeneous, consisting of various *GPU generations* and resource configurations. The PAI platform implements *GPU sharing techniques* to optimize resource utilization, achieving up to *50% GPU savings* compared to non-sharing systems. GPU allocation is highly granular, with a minimum allocation unit of 0.01%, though at least 1% of memory and GPU time is reserved per task. However, GPU sharing introduces *fragmentation*, leaving some GPUs underutilized. To address this, Alibaba developed *Fragmentation Gradient Descent (FGD)*, which reduces fragmentation by up to 49% and enables the use of an additional 290 GPUs through improved job scheduling guided by a novel fragmentation metric.

Some tasks on Alibaba’s PAI platform exhibit low GPU utilization, creating *CPU bottlenecks* due to intensive data processing demands. The platform employs a *scheduling policy* that differentiates between high and low GPU utilization tasks to ensure efficient resource distribution.

When comparing Alibaba’s system to *Google’s Borg*, both companies operate large-scale computational infrastructures—Google with clusters hosting tens of thousands of machines and Alibaba managing over 6,000 GPUs in its PAI cluster. Both companies have also released public traces for research purposes. Alibaba’s trace emphasizes *GPU utilization* and includes diverse *machine learning workloads*, while Google’s focuses on general workload management in data centers.

Both companies manage heterogeneous clusters, though Alibaba’s GPU heterogeneity is more pronounced, with multiple generations of GPUs in operation. Regarding *resource sharing*, Google employs *alloc sets* for heavy workloads, whereas Alibaba leverages *GPU sharing* with fine-grained allocation units. *Optimization objectives* are aligned, as both focus on maximizing resource utilization and reducing job completion times. Google achieves this through *Autopilot’s autoscaling*, while Alibaba employs techniques like FGD and AntMan for dynamic scaling in machine learning workloads.

Overall, Google appears more focused on general workload management, while Alibaba concentrates on machine learning workloads, emphasizing GPU optimization and addressing fragmentation challenges.



