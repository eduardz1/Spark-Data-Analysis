import os
from typing import Literal

from pyspark.sql import SparkSession
from rich.console import Console

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import (
    MachineEvents,
    machine_events,
)
from spark_data_analysis.rich import table


def q01(ss: SparkSession, _: int | Literal["full"]):
    """Question 1

    Answers the following questions:
    - What is the distribution of the machines according to their CPU capacity?

    Args:
        ss (SparkSession): Spark session
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.
    """
    console = Console()

    me = machine_events(ss)

    # Calculate distribution of CPUs
    cpu_distribution = (
        me.groupBy(MachineEvents.CPUS.value).count().orderBy(MachineEvents.CPUS.value)
    )
    results = {str(row.CPUs): row["count"] for row in cpu_distribution.collect()}

    table(console, "CPU Distribution", results)

    if os.environ.get("SPARK_DATA_ANALYSIS_PLOT") == "true":
        import matplotlib.pyplot as plt

        plt.figure(figsize=(10, 4))

        cpu_types = list(results.keys())
        count_values = list(results.values())

        plt.bar(cpu_types, count_values)

        plt.xlabel("Number of CPUs")
        plt.ylabel("Count")

        # Add percentage labels
        total = sum(count_values)
        for i, count in enumerate(count_values):
            plt.text(
                cpu_types[i],  # type: ignore
                count,
                f"{count / total:.1%}",
                ha="center",
                va="bottom",
            )

        plt.tight_layout()
        plt.savefig(f"{IMGS_PATH}/cpu_dsitribution.svg")
        plt.close()
