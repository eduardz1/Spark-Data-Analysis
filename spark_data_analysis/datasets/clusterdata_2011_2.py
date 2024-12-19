from functools import cache
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructType,
    FloatType,
)

BUCKET_NAME = "gs://clusterdata-2011-2"


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


@cache
def machine_attributes(ss: SparkSession) -> DataFrame:
    schema = (
        StructType()
        .add("Time", IntegerType(), False)
        .add("MachineID", IntegerType(), False)
        .add("AttributeName", StringType(), False)
        .add("AttributeValue", StringType(), True)
        .add("AttributeDeleted", BooleanType(), False)
    )
    return ss.read.schema(schema).csv(f"{BUCKET_NAME}/machine_attributes/*")


@cache
def machine_events(ss: SparkSession) -> DataFrame:
    schema = (
        StructType()
        .add("Time", IntegerType(), False)
        .add("MachineID", IntegerType(), False)
        .add("EventType", IntegerType(), False)
        .add("PlatformID", StringType(), True)
        .add("CPUs", FloatType(), True)
        .add("Memory", FloatType(), True)
    )
    return ss.read.schema(schema).csv(f"{BUCKET_NAME}/machine_events/*")


@cache
def task_constraints(
    ss: SparkSession, parts: int | Literal["full"] = "full"
) -> DataFrame:
    if parts == "full":
        parts = 500

    nums = [str(i).zfill(5) for i in range(parts)]
    nums = ",".join(nums)

    schema = (
        StructType()
        .add("Time", IntegerType(), False)
        .add("JobID", IntegerType(), False)
        .add("TaskIndex", IntegerType(), False)
        .add("ComparisonOperator", IntegerType(), False)
        .add("AttributeName", StringType(), False)
        .add("AttributeValue", StringType(), True)
    )
    return ss.read.schema(schema).csv(
        f"{BUCKET_NAME}/task_constraints/part-{{{nums}}}*"
    )


@cache
def task_events(ss: SparkSession, parts: int | Literal["full"] = "full") -> DataFrame:
    if parts == "full":
        parts = 500

    nums = [str(i).zfill(5) for i in range(parts)]
    nums = ",".join(nums)

    schema = (
        StructType()
        .add("Time", IntegerType(), False)
        .add("MissingInfo", IntegerType(), True)
        .add("JobID", IntegerType(), False)
        .add("TaskIndex", IntegerType(), False)
        .add("MachineID", IntegerType(), True)
        .add("EventType", IntegerType(), False)
        .add("User", StringType(), True)
        .add("SchedulingClass", IntegerType(), True)
        .add("Priority", IntegerType(), False)
        .add("CPURequest", FloatType(), True)
        .add("MemoryRequest", FloatType(), True)
        .add("DiskSpaceRequest", FloatType(), True)
    )
    return ss.read.schema(schema).csv(f"{BUCKET_NAME}/task_events/part-{{{nums}}}*")


@cache
def task_usage(ss: SparkSession, parts: int | Literal["full"] = "full") -> DataFrame:
    if parts == "full":
        parts = 500

    nums = [str(i).zfill(5) for i in range(parts)]
    nums = ",".join(nums)

    schema = (
        StructType()
        .add("StartTime", IntegerType(), False)
        .add("EndTime", IntegerType(), False)
        .add("JobID", IntegerType(), False)
        .add("TaskIndex", IntegerType(), False)
        .add("MachineID", IntegerType(), False)
        .add("CPURate", FloatType(), True)
        .add("CanonicalMemoryUsage", FloatType(), True)
        .add("AssignedMemoryUsage", FloatType(), True)
        .add("UnmappedMemoryUsage", FloatType(), True)
        .add("TotalPageCache", FloatType(), True)
        .add("MaximumMemoryUsage", FloatType(), True)
        .add("DiskIOTime", FloatType(), True)
        .add("LocalDiskSpaceUsage", FloatType(), True)
        .add("MaximumCPURate", FloatType(), True)
        .add("MaximumDiskIOTime", FloatType(), True)
        .add("CyclesPerInstruction", FloatType(), True)
        .add("MemoryAccessPerInstruction", FloatType(), True)
        .add("SamplePortion", FloatType(), True)
        .add("AggregationType", BooleanType(), True)
        .add("SampledCPUUsage", FloatType(), True)
    )
    return ss.read.schema(schema).csv(f"{BUCKET_NAME}/task_usage/part-{{{nums}}}*")
