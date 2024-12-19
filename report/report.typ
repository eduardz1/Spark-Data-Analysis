#import "template.typ": template, eqcolumns

#show: template.with(
  title: [Spark Data Analysis],
  subtitle: [Report for the course of Large Scale Data Processing and Distributed Systems],
  authors: ("Eduard Occhipinti", "Domenico Di Stasio"),
)

= General Analyses

+ q1

+ q2
+ q3
+ q4
+ "#underline[In general, do tasks from the same job run on the same machine?]" No, in analyzing the distribution of tasks across machines for a given job (by combining the `job_events` and `task_events` datasets), we found that tasks from the same job are distributed across multiple machines. In general we found that, on average, tasks from the same job are distributed across #text(fill: red, [TODO]) machines. With a maximum of #text(fill: red, [TODO]) different machines for a single job and a standard deviation of #text(fill: red, [TODO]).
