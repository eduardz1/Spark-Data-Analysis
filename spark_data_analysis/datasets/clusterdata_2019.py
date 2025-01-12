"""Clusterdata 2019 dataset.

This module contains the schema definitions for the Clusterdata 2019 dataset
from Google. This module also provides functions to load the dataset into a
DataFrame using Spark. The functions are cached to avoid loading the same data
multiple times and provide a way to specify the number of parts to load.
"""

from enum import Enum
from functools import cache
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructType,
)

CELL = "a"  # We only take into account cell a because of the size of the dataset
BUCKET_NAME = f"gs://clusterdata_2019_{CELL}"


def format_parts_file_regex(parts: int | Literal["full"]) -> str:
    return (
        "*" if parts == "full" else f"{{{','.join(f'{i:012d}' for i in range(parts))}}}"
    )


class CollectionEvents(Enum):
    """Enum for the CollectionEvents dataset columns."""

    TIME = "time"
    TYPE = "type"
    COLLECTION_ID = "collection_id"
    SCHEDULING_CLASS = "scheduling_class"
    MISSING_TYPE = "missing_type"
    COLLECTION_TYPE = "collection_type"
    PRIORITY = "priority"
    ALLOC_COLLECTION_ID = "alloc_collection_id"
    USER = "user"
    COLLECTION_NAME = "collection_name"
    COLLECTION_LOGICAL_NAME = "collection_logical_name"
    PARENT_COLLECTION_ID = "parent_collection_id"
    START_AFTER_COLLECTION_IDS = "start_after_collection_ids"
    MAX_PER_MACHINE = "max_per_machine"
    MAX_PER_SWITCH = "max_per_switch"
    VERTICAL_SCALING = "vertical_scaling"
    SCHEDULER = "scheduler"


COLLECTION_EVENTS_SCHEMA = (
    StructType()
    .add(CollectionEvents.TIME.value, IntegerType(), True)
    .add(CollectionEvents.TYPE.value, IntegerType(), True)
    .add(CollectionEvents.COLLECTION_ID.value, IntegerType(), True)
    .add(CollectionEvents.SCHEDULING_CLASS.value, IntegerType(), True)
    .add(CollectionEvents.MISSING_TYPE.value, IntegerType(), True)
    .add(CollectionEvents.COLLECTION_TYPE.value, IntegerType(), True)
    .add(CollectionEvents.PRIORITY.value, IntegerType(), True)
    .add(CollectionEvents.ALLOC_COLLECTION_ID.value, IntegerType(), True)
    .add(CollectionEvents.USER.value, StringType(), True)
    .add(CollectionEvents.COLLECTION_NAME.value, StringType(), True)
    .add(CollectionEvents.COLLECTION_LOGICAL_NAME.value, StringType(), True)
    .add(CollectionEvents.PARENT_COLLECTION_ID.value, IntegerType(), True)
    .add(
        CollectionEvents.START_AFTER_COLLECTION_IDS.value,
        ArrayType(IntegerType()),
        True,
    )
    .add(CollectionEvents.MAX_PER_MACHINE.value, IntegerType(), True)
    .add(CollectionEvents.MAX_PER_SWITCH.value, IntegerType(), True)
    .add(CollectionEvents.VERTICAL_SCALING.value, IntegerType(), True)
    .add(CollectionEvents.SCHEDULER.value, IntegerType(), True)
)


@cache
def collection_events(
    ss: SparkSession, parts: int | Literal["full"] = "full"
) -> DataFrame:
    return ss.read.schema(COLLECTION_EVENTS_SCHEMA).json(
        f"{BUCKET_NAME}/collection_events-{format_parts_file_regex(parts)}.json.gz"
    )


class InstanceEvents(Enum):
    """Enum for the InstanceEvents dataset columns."""

    TIME = "time"
    TYPE = "type"
    COLLECTION_ID = "collection_id"
    SCHEDULING_CLASS = "scheduling_class"
    MISSING_TYPE = "missing_type"
    COLLECTION_TYPE = "collection_type"
    PRIORITY = "priority"
    ALLOC_COLLECTION_ID = "alloc_collection_id"
    INSTANCE_INDEX = "instance_index"
    MACHINE_ID = "machine_id"
    ALLOC_INSTANCE_INDEX = "alloc_instance_index"
    RESOURCE_REQUEST = "resource_request"
    CONSTRAINT = "constraint"


class Constraint(Enum):
    """Enum for the Constraint dataset columns."""

    NAME = "name"
    VALUE = "value"
    RELATION = "relation"


class Resources(Enum):
    """Enum for the ResourceRequest dataset columns."""

    CPUS = "cpus"
    MEMORY = "memory"


INSTANCE_EVENTS_SCHEMA = (
    StructType()
    .add(InstanceEvents.TIME.value, IntegerType(), True)
    .add(InstanceEvents.TYPE.value, IntegerType(), True)
    .add(InstanceEvents.COLLECTION_ID.value, IntegerType(), True)
    .add(InstanceEvents.SCHEDULING_CLASS.value, IntegerType(), True)
    .add(InstanceEvents.MISSING_TYPE.value, IntegerType(), True)
    .add(InstanceEvents.COLLECTION_TYPE.value, IntegerType(), True)
    .add(InstanceEvents.PRIORITY.value, IntegerType(), True)
    .add(InstanceEvents.ALLOC_COLLECTION_ID.value, IntegerType(), True)
    .add(InstanceEvents.INSTANCE_INDEX.value, IntegerType(), True)
    .add(InstanceEvents.MACHINE_ID.value, IntegerType(), True)
    .add(InstanceEvents.ALLOC_INSTANCE_INDEX.value, IntegerType(), True)
    .add(
        InstanceEvents.RESOURCE_REQUEST.value,
        StructType()
        .add(Resources.CPUS.value, FloatType(), True)
        .add(Resources.MEMORY.value, FloatType(), True),
        True,
    )
    .add(
        InstanceEvents.CONSTRAINT.value,
        ArrayType(
            StructType()
            .add(Constraint.NAME.value, StringType(), True)
            .add(Constraint.VALUE.value, StringType(), True)
            .add(Constraint.RELATION.value, IntegerType(), True),
            True,
        ),
        True,
    )
)


@cache
def instance_events(
    ss: SparkSession, parts: int | Literal["full"] = "full"
) -> DataFrame:
    return ss.read.schema(INSTANCE_EVENTS_SCHEMA).json(
        f"{BUCKET_NAME}/instance_events-{format_parts_file_regex(parts)}.json.gz"
    )


class InstanceUsage(Enum):
    """Enum for the InstanceUsage dataset columns."""

    START_TIME = "start_time"
    END_TIME = "end_time"
    COLLECTION_ID = "collection_id"
    INSTANCE_INDEX = "instance_index"
    MACHINE_ID = "machine_id"
    ALLOC_COLLECTION_ID = "alloc_collection_id"
    ALLOC_INSTANCE_INDEX = "alloc_instance_index"
    COLLECTION_TYPE = "collection_type"
    AVERAGE_USAGE = "average_usage"
    MAXIMUM_USAGE = "maximum_usage"
    RANDOM_SAMPLE_USAGE = "random_sample_usage"
    ASSIGNED_MEMORY = "assigned_memory"
    PAGE_CACHE_MEMORY = "page_cache_memory"
    CYCLES_PER_INSTRUCTION = "cycles_per_instruction"
    MEMORY_ACCESSES_PER_INSTRUCTION = "memory_accesses_per_instruction"
    SAMPLE_RATE = "sample_rate"
    CPU_USAGE_DISTRIBUTION = "cpu_usage_distribution"
    TAIL_CPU_USAGE_DISTRIBUTION = "tail_cpu_usage_distribution"


INSTANCE_USAGE = (
    StructType()
    .add(InstanceUsage.START_TIME.value, IntegerType(), True)
    .add(InstanceUsage.END_TIME.value, IntegerType(), True)
    .add(InstanceUsage.COLLECTION_ID.value, IntegerType(), True)
    .add(InstanceUsage.INSTANCE_INDEX.value, IntegerType(), True)
    .add(InstanceUsage.MACHINE_ID.value, IntegerType(), True)
    .add(InstanceUsage.ALLOC_COLLECTION_ID.value, IntegerType(), True)
    .add(InstanceUsage.ALLOC_INSTANCE_INDEX.value, IntegerType(), True)
    .add(InstanceUsage.COLLECTION_TYPE.value, IntegerType(), True)
    .add(
        InstanceUsage.AVERAGE_USAGE.value,
        StructType()
        .add(Resources.CPUS.value, FloatType(), True)
        .add(Resources.MEMORY.value, FloatType(), True),
        True,
    )
    .add(
        InstanceUsage.MAXIMUM_USAGE.value,
        StructType()
        .add(Resources.CPUS.value, FloatType(), True)
        .add(Resources.MEMORY.value, FloatType(), True),
        True,
    )
    .add(
        InstanceUsage.RANDOM_SAMPLE_USAGE.value,
        StructType()
        .add(Resources.CPUS.value, FloatType(), True)
        .add(Resources.MEMORY.value, FloatType(), True),
        True,
    )
    .add(InstanceUsage.ASSIGNED_MEMORY.value, FloatType(), True)
    .add(InstanceUsage.PAGE_CACHE_MEMORY.value, FloatType(), True)
    .add(InstanceUsage.CYCLES_PER_INSTRUCTION.value, FloatType(), True)
    .add(InstanceUsage.MEMORY_ACCESSES_PER_INSTRUCTION.value, FloatType(), True)
    .add(InstanceUsage.SAMPLE_RATE.value, FloatType(), True)
    .add(InstanceUsage.CPU_USAGE_DISTRIBUTION.value, ArrayType(FloatType()), True)
    .add(InstanceUsage.TAIL_CPU_USAGE_DISTRIBUTION.value, ArrayType(FloatType()), True)
)


@cache
def instance_usage(
    ss: SparkSession, parts: int | Literal["full"] = "full"
) -> DataFrame:
    return ss.read.schema(INSTANCE_USAGE).json(
        f"{BUCKET_NAME}/instance_usage-{format_parts_file_regex(parts)}.json.gz"
    )


class MachineAttributes(Enum):
    """Enum for the MachineAttributes dataset columns."""

    TIME = "time"
    MACHINE_ID = "machine_id"
    NAME = "name"
    VALUE = "value"
    DELETED = "deleted"


MACHINE_ATTRIBUTES_SCHEMA = (
    StructType()
    .add(MachineAttributes.TIME.value, IntegerType(), True)
    .add(MachineAttributes.MACHINE_ID.value, IntegerType(), True)
    .add(MachineAttributes.NAME.value, StringType(), True)
    .add(MachineAttributes.VALUE.value, StringType(), True)
    .add(MachineAttributes.DELETED.value, BooleanType(), True)
)


@cache
def machine_attributes(
    ss: SparkSession, parts: int | Literal["full"] = "full"
) -> DataFrame:
    return ss.read.schema(MACHINE_ATTRIBUTES_SCHEMA).json(
        f"{BUCKET_NAME}/machine_attributes-{format_parts_file_regex(parts)}.json.gz"
    )


class MachineEvents(Enum):
    """Enum for the MachineEvents dataset columns."""

    TIME = "time"
    MACHINE_ID = "machine_id"
    TYPE = "type"
    SWITCH_ID = "switch_id"
    CAPACITY = "capacity"
    PLATFORM_ID = "platform_id"
    MISSING_DATA_REASON = "missing_data_reason"


MACHINE_EVENTS_SCHEMA = (
    StructType()
    .add(MachineEvents.TIME.value, IntegerType(), True)
    .add(MachineEvents.MACHINE_ID.value, IntegerType(), True)
    .add(MachineEvents.TYPE.value, IntegerType(), True)
    .add(MachineEvents.SWITCH_ID.value, StringType(), True)
    .add(
        MachineEvents.CAPACITY.value,
        StructType()
        .add(Resources.CPUS.value, FloatType(), True)
        .add(Resources.MEMORY.value, FloatType(), True),
        True,
    )
    .add(MachineEvents.PLATFORM_ID.value, StringType(), True)
    .add(MachineEvents.MISSING_DATA_REASON.value, IntegerType(), True)
)


@cache
def machine_events(
    ss: SparkSession, parts: int | Literal["full"] = "full"
) -> DataFrame:
    return ss.read.schema(MACHINE_EVENTS_SCHEMA).json(
        f"{BUCKET_NAME}/machine_events-{format_parts_file_regex(parts)}.json.gz"
    )
