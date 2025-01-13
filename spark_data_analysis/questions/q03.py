import os
from typing import Literal

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from rich.console import Console

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import (
    JobEvents,
    TaskEvents,
    job_events,
    task_events,
)
from spark_data_analysis.rich import table


def q03(ss: SparkSession, parts: int | Literal["full"]):
    """Question 3

    Answers the following questions:
    - What is the distribution of the number of jobs/tasks per scheduling class?

    Args:
        ss (SparkSession): Spark session
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.
    """
    console = Console()

    je = job_events(ss, parts)
    te = task_events(ss, parts)

    job_distribution = (
        je.groupBy(JobEvents.SCHEDULING_CLASS.value)
        .count()
        .orderBy(JobEvents.SCHEDULING_CLASS.value)
    )

    job_distribution = {
        str(row.SchedulingClass): row["count"] for row in job_distribution.collect()
    }

    table(console, "Job Distribution by Scheduling Class", job_distribution)

    task_distribution = (
        te.groupBy(TaskEvents.SCHEDULING_CLASS.value)
        .count()
        .orderBy(TaskEvents.SCHEDULING_CLASS.value)
    )
    print("Task Distribution by Scheduling Class:")

    task_distribution = {
        str(row.SchedulingClass): row["count"] for row in task_distribution.collect()
    }

    table(console, "Task Distribution by Scheduling Class", task_distribution)

    if os.environ.get("SPARK_DATA_ANALYSIS_PLOT") == "true":
        plt.figure(figsize=(12, 4))

        # First plot: Job distribution
        plt.subplot(1, 2, 1)  # 1 row, 2 columns, first plot
        job_classes = list(job_distribution.keys())
        job_counts = list(job_distribution.values())
        plt.bar(job_classes, job_counts)
        plt.xlabel("Scheduling Class")
        plt.ylabel("Number of Jobs")
        plt.title("Job Distribution by Scheduling Class")

        # Percentage labels
        total_jobs = sum(job_counts)
        for i, count in enumerate(job_counts):
            plt.text(
                job_classes[i],  # type: ignore
                count,
                f"{count / total_jobs:.1%}",
                ha="center",
                va="bottom",
            )

        # Second plot: Task distribution
        plt.subplot(1, 2, 2)  # 1 row, 2 columns, second plot
        task_classes = list(task_distribution.keys())
        task_counts = list(task_distribution.values())
        plt.bar(task_classes, task_counts)
        plt.xlabel("Scheduling Class")
        plt.ylabel("Number of Tasks")
        plt.title("Task Distribution by Scheduling Class")

        # Percentage labels
        total_tasks = sum(task_counts)
        for i, count in enumerate(task_counts):
            plt.text(
                task_classes[i],  # type: ignore
                count,
                f"{count / total_tasks:.1%}",
                ha="center",
                va="bottom",
            )

        plt.tight_layout()
        plt.savefig(f"{IMGS_PATH}/job_task_distribution_by_schedulingclass.svg")
        plt.close()
