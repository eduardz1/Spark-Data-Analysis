from typing import Literal

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when

from spark_data_analysis.datasets.clusterdata_2011_2 import (
    MachineEvents,
    machine_events,
)


def q02(ss: SparkSession, _: int | Literal["full"]):
    """Question 2

    Answers the following questions:
    - What is the percentage of computational power lost due to maintenance
      (a machine went offline and reconnected later)?

    Args:
        ss (SparkSession): Spark session
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.
    """
    me = machine_events(ss)

    # Create a copy of the DataFrame to obtain the preceding (lag) and succeeding (lead) rows
    me_prev = me.withColumnsRenamed(
        {
            MachineEvents.TIME.value: "PrevTime",
            MachineEvents.EVENT_TYPE.value: "PrevEventType",
        }
    )
    me_next = me.withColumnsRenamed(
        {
            MachineEvents.TIME.value: "NextTime",
            MachineEvents.EVENT_TYPE.value: "NextEventType",
        }
    )

    # Filter the rows to match the same machine
    me_prev = me_prev.select(
        MachineEvents.MACHINE_ID.value, "PrevTime", "PrevEventType"
    )
    me_next = me_next.select(
        MachineEvents.MACHINE_ID.value, "NextTime", "NextEventType"
    )

    # Perform a join to obtain the NextTime and PrevEventType for each original row
    me_with_lag = me.join(me_prev, on=MachineEvents.MACHINE_ID.value, how="left").join(
        me_next, on=MachineEvents.MACHINE_ID.value, how="left"
    )

    # OfflineDuration calculation
    offline_durations = me_with_lag.withColumn(
        "OfflineDuration",
        when(
            (col(MachineEvents.EVENT_TYPE.value) == 1) & (col("PrevEventType") == 0),
            col("NextTime") - col(MachineEvents.TIME.value),
        ).otherwise(0),
    )

    # Total offline time
    total_offline_time = offline_durations.agg(
        sum("OfflineDuration").alias("TotalOfflineTime")
    ).collect()[0]["TotalOfflineTime"]

    # Calculate the total computational power: Sum(CPUs * active time)
    active_durations = me_with_lag.withColumn(
        "ActiveDuration",
        when(
            (col(MachineEvents.EVENT_TYPE.value) == 0) & (col("NextTime").isNotNull()),
            col("NextTime") - col(MachineEvents.TIME.value),
        ).otherwise(0),
    )

    total_compute_capacity = (
        active_durations.withColumn(
            "ComputeContribution", col("ActiveDuration") * col(MachineEvents.CPUS.value)
        )
        .agg(sum("ComputeContribution").alias("TotalComputeCapacity"))
        .collect()[0]["TotalComputeCapacity"]
    )

    # Lost percentage
    if total_compute_capacity and total_offline_time:
        percent_lost = (total_offline_time / total_compute_capacity) * 100
        print(f"Percentage of lost computational power: {percent_lost:.2f}%")
    else:
        print("The available data does not allow us to determine the percentage lost.")
