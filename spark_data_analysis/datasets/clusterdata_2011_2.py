"""Clusterdata 2011-2 dataset.

This module contains the schema definitions for the Clusterdata 2011-2 dataset
from Google. This module also provides functions to load the dataset into a
DataFrame using Spark. The functions are cached to avoid loading the same data
multiple times and provide a way to specify the number of parts to load.
"""

from enum import Enum
from functools import cache
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructType,
)

BUCKET_NAME = "gs://clusterdata-2011-2"
NUM_PARTS = 500


def format_parts_file_regex(parts: int | Literal["full"]) -> str:
    """Format the number of parts to load into a file regex.

    Example:
        >>> format_parts_file_regex(5)
        "{part-00000*,part-00001*,part-00002*,part-00003*,part-00004*}"

    Args:
        parts (int | Literal[&quot;full&quot;]): Number of parts to load. If
            &quot;full&quot;, load all parts.

    Raises:
        ValueError: If the number of parts is greater than the maximum number of
            parts.

    Returns:
        str: The formatted file regex.
    """

    if isinstance(parts, int) and parts > NUM_PARTS:
        raise ValueError(
            f"Number of parts should be less than or equal to {NUM_PARTS}."
        )

    return (
        ""
        if parts == "full"
        else f"{{{','.join(f'part-{i:05d}*' for i in range(parts))}}}"
    )


class JobEvents(Enum):
    """Enum for the JobEvents dataset columns."""

    TIME = "Time"
    MISSING_INFO = "MissingInfo"
    JOB_ID = "JobID"
    EVENT_TYPE = "EventType"
    USER = "User"
    SCHEDULING_CLASS = "SchedulingClass"
    JOB_NAME = "JobName"
    LOGICAL_JOB_NAME = "LogicalJobName"


JOB_EVENTS_SCHEMA = (
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


@cache
def job_events(ss: SparkSession, parts: int | Literal["full"] = "full") -> DataFrame:
    return ss.read.schema(JOB_EVENTS_SCHEMA).csv(
        f"{BUCKET_NAME}/job_events/{format_parts_file_regex(parts)}"
    )


class MachineAttributes(Enum):
    """Enum for the MachineAttributes dataset columns."""

    TIME = "Time"
    MACHINE_ID = "MachineID"
    ATTRIBUTE_NAME = "AttributeName"
    ATTRIBUTE_VALUE = "AttributeValue"
    ATTRIBUTE_DELETED = "AttributeDeleted"


MACHINE_ATTRIBUTES_SCHEMA = (
    StructType()
    .add(MachineAttributes.TIME.value, IntegerType(), False)
    .add(MachineAttributes.MACHINE_ID.value, IntegerType(), False)
    .add(MachineAttributes.ATTRIBUTE_NAME.value, StringType(), False)
    .add(MachineAttributes.ATTRIBUTE_VALUE.value, StringType(), True)
    .add(MachineAttributes.ATTRIBUTE_DELETED.value, BooleanType(), False)
)


@cache
def machine_attributes(ss: SparkSession) -> DataFrame:
    return ss.read.schema(MACHINE_ATTRIBUTES_SCHEMA).csv(
        f"{BUCKET_NAME}/machine_attributes/"
    )


class MachineEvents(Enum):
    """Enum for the MachineEvents dataset columns."""

    TIME = "Time"
    MACHINE_ID = "MachineID"
    EVENT_TYPE = "EventType"
    PLATFORM_ID = "PlatformID"
    CPUS = "CPUs"
    MEMORY = "Memory"


MACHINE_EVENTS_SCHEMA = (
    StructType()
    .add(MachineEvents.TIME.value, IntegerType(), False)
    .add(MachineEvents.MACHINE_ID.value, IntegerType(), False)
    .add(MachineEvents.EVENT_TYPE.value, IntegerType(), False)
    .add(MachineEvents.PLATFORM_ID.value, StringType(), True)
    .add(MachineEvents.CPUS.value, FloatType(), True)
    .add(MachineEvents.MEMORY.value, FloatType(), True)
)


@cache
def machine_events(ss: SparkSession) -> DataFrame:
    return ss.read.schema(MACHINE_EVENTS_SCHEMA).csv(f"{BUCKET_NAME}/machine_events/")


class TaskConstraints(Enum):
    """Enum for the TaskConstraints dataset columns."""

    TIME = "Time"
    JOB_ID = "JobID"
    TASK_INDEX = "TaskIndex"
    COMPARISON_OPERATOR = "ComparisonOperator"
    ATTRIBUTE_NAME = "AttributeName"
    ATTRIBUTE_VALUE = "AttributeValue"


TASK_CONSTRAINTS_SCHEMA = (
    StructType()
    .add(TaskConstraints.TIME.value, IntegerType(), False)
    .add(TaskConstraints.JOB_ID.value, IntegerType(), False)
    .add(TaskConstraints.TASK_INDEX.value, IntegerType(), False)
    .add(TaskConstraints.COMPARISON_OPERATOR.value, IntegerType(), False)
    .add(TaskConstraints.ATTRIBUTE_NAME.value, StringType(), False)
    .add(TaskConstraints.ATTRIBUTE_VALUE.value, StringType(), True)
)


@cache
def task_constraints(
    ss: SparkSession, parts: int | Literal["full"] = "full"
) -> DataFrame:
    return ss.read.schema(TASK_CONSTRAINTS_SCHEMA).csv(
        f"{BUCKET_NAME}/task_constraints/{format_parts_file_regex(parts)}"
    )


class TaskEvents(Enum):
    """Enum for the TaskEvents dataset columns."""

    TIME = "Time"
    MISSING_INFO = "MissingInfo"
    JOB_ID = "JobID"
    TASK_INDEX = "TaskIndex"
    MACHINE_ID = "MachineID"
    EVENT_TYPE = "EventType"
    USER = "User"
    SCHEDULING_CLASS = "SchedulingClass"
    PRIORITY = "Priority"
    CPU_REQUEST = "CPURequest"
    MEMORY_REQUEST = "MemoryRequest"
    DISK_SPACE_REQUEST = "DiskSpaceRequest"


TASK_EVENTS_SCHEMA = (
    StructType()
    .add(TaskEvents.TIME.value, IntegerType(), False)
    .add(TaskEvents.MISSING_INFO.value, IntegerType(), True)
    .add(TaskEvents.JOB_ID.value, IntegerType(), False)
    .add(TaskEvents.TASK_INDEX.value, IntegerType(), False)
    .add(TaskEvents.MACHINE_ID.value, IntegerType(), True)
    .add(TaskEvents.EVENT_TYPE.value, IntegerType(), False)
    .add(TaskEvents.USER.value, StringType(), True)
    .add(TaskEvents.SCHEDULING_CLASS.value, IntegerType(), True)
    .add(TaskEvents.PRIORITY.value, IntegerType(), False)
    .add(TaskEvents.CPU_REQUEST.value, FloatType(), True)
    .add(TaskEvents.MEMORY_REQUEST.value, FloatType(), True)
    .add(TaskEvents.DISK_SPACE_REQUEST.value, FloatType(), True)
)


@cache
def task_events(ss: SparkSession, parts: int | Literal["full"] = "full") -> DataFrame:
    return ss.read.schema(TASK_EVENTS_SCHEMA).csv(
        f"{BUCKET_NAME}/task_events/{format_parts_file_regex(parts)}"
    )


class TaskUsage(Enum):
    """Enum for the TaskUsage dataset columns."""

    START_TIME = "StartTime"
    END_TIME = "EndTime"
    JOB_ID = "JobID"
    TASK_INDEX = "TaskIndex"
    MACHINE_ID = "MachineID"
    CPU_RATE = "CPURate"
    CANONICAL_MEMORY_USAGE = "CanonicalMemoryUsage"
    ASSIGNED_MEMORY_USAGE = "AssignedMemoryUsage"
    UNMAPPED_MEMORY_USAGE = "UnmappedMemoryUsage"
    TOTAL_PAGE_CACHE = "TotalPageCache"
    MAXIMUM_MEMORY_USAGE = "MaximumMemoryUsage"
    DISK_IO_TIME = "DiskIOTime"
    LOCAL_DISK_SPACE_USAGE = "LocalDiskSpaceUsage"
    MAXIMUM_CPU_RATE = "MaximumCPURate"
    MAXIMUM_DISK_IO_TIME = "MaximumDiskIOTime"
    CYCLES_PER_INSTRUCTION = "CyclesPerInstruction"
    MEMORY_ACCESS_PER_INSTRUCTION = "MemoryAccessPerInstruction"
    SAMPLE_PORTION = "SamplePortion"
    AGGREGATION_TYPE = "AggregationType"
    SAMPLED_CPU_USAGE = "SampledCPUUsage"


TASK_USAGE_SCHEMA = (
    StructType()
    .add(TaskUsage.START_TIME.value, IntegerType(), False)
    .add(TaskUsage.END_TIME.value, IntegerType(), False)
    .add(TaskUsage.JOB_ID.value, IntegerType(), False)
    .add(TaskUsage.TASK_INDEX.value, IntegerType(), False)
    .add(TaskUsage.MACHINE_ID.value, IntegerType(), False)
    .add(TaskUsage.CPU_RATE.value, FloatType(), True)
    .add(TaskUsage.CANONICAL_MEMORY_USAGE.value, FloatType(), True)
    .add(TaskUsage.ASSIGNED_MEMORY_USAGE.value, FloatType(), True)
    .add(TaskUsage.UNMAPPED_MEMORY_USAGE.value, FloatType(), True)
    .add(TaskUsage.TOTAL_PAGE_CACHE.value, FloatType(), True)
    .add(TaskUsage.MAXIMUM_MEMORY_USAGE.value, FloatType(), True)
    .add(TaskUsage.DISK_IO_TIME.value, FloatType(), True)
    .add(TaskUsage.LOCAL_DISK_SPACE_USAGE.value, FloatType(), True)
    .add(TaskUsage.MAXIMUM_CPU_RATE.value, FloatType(), True)
    .add(TaskUsage.MAXIMUM_DISK_IO_TIME.value, FloatType(), True)
    .add(TaskUsage.CYCLES_PER_INSTRUCTION.value, FloatType(), True)
    .add(TaskUsage.MEMORY_ACCESS_PER_INSTRUCTION.value, FloatType(), True)
    .add(TaskUsage.SAMPLE_PORTION.value, FloatType(), True)
    .add(TaskUsage.AGGREGATION_TYPE.value, BooleanType(), True)
    .add(TaskUsage.SAMPLED_CPU_USAGE.value, FloatType(), True)
)


@cache
def task_usage(ss: SparkSession, parts: int | Literal["full"] = "full") -> DataFrame:
    return ss.read.schema(TASK_USAGE_SCHEMA).csv(
        f"{BUCKET_NAME}/task_usage/{format_parts_file_regex(parts)}"
    )
