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
+ "#underline[In general, do tasks from the same job run on the same machine?]" No, in analyzing the distribution of tasks across machines for a given job (by combining the `job_events` and `task_events` datasets), we found that tasks from the same job are distributed across multiple machines. In general we found that, on average, tasks from the same job are distributed across 88 machines. With a maximum of 9170 different machines for a single job and a standard deviation of 623. #figure(image("imgs/different_machines_per_job.svg"), caption: [Distribution of the number of different machines for each job, notice that outliers were excluded because they tend to get particularly high.])
+ "#underline[Are the tasks that request the more resources the one that consume the more resources?]" In general, by calculating the correlation between resource usage and resource request, where the resource usage dataframe is calculated by computing the average of `CPURate`, `CanonicalMemoryUsage` and `LocalDiskSpaceUsage` for each task (uniquely identified by the tuple `(job_id, task_index)`) in the `task_usage` table, and the resource request dataframe is calculated by averaging the `CPURequest`, `MemoryRequest` and `DiskSpaceRequest` for each task in the `task_events` table, we found that:
  - The correlation between the CPU request and the CPU usage is moderate, at 0.5.

  - The correlation between the memory request and the memory usage is high, at 0.9.
  - There is no correlation between the disk request and the disk usage (0.05).
+ "#underline[Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?]"

= Performance Tuning
