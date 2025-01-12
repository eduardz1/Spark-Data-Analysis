"""Spark streaming demo with Kafka.

This demo uses Kafka to simulate a stream coming from the "Cluster Data 2011"
Google dataset. The stream is produced from the "task_usage" table and consumed
by Spark Structured Streaming. The stream is then processed to calculate the
average CPU usage per task in a 5 seconds window and printed to the console.
"""
