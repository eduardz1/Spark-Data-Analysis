import argparse
import configparser
import contextlib
import importlib
import os
import time
from typing import Mapping

import typst
from pyspark.sql import SparkSession
from rich.console import Console
from rich.status import Status
from rich.traceback import install

from spark_data_analysis.constants import QUESTION_TITLES, TYPST_PATH


def parse_spark_config(path: str) -> dict:
    """Parse the Spark configuration file.

    Args:
        path (str): Path to the Spark configuration file

    Returns:
        dict: Dictionary with the Spark configuration
    """
    config = configparser.ConfigParser()
    config.read(path)
    return dict(config["Spark"])


def parse_args() -> argparse.Namespace:
    """Parse the command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        prog="python -m spark_data_analysis",
        description=(
            "Either run the code for the analysis, separately for each question "
            "or run a simulation of a Spark Streaming environment. "
            "Optionally compile the report."
        ),
    )
    parser.add_argument(
        "-c",
        "--compile_pdf",
        action="store_true",
        help="compile the report in pdf format",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="suppress additional information during code execution",
    )
    parser.add_argument(
        "-s",
        "--spark_config",
        type=str,
        help=(
            "path to the .ini Spark configuration file, by default it uses the "
            "local configuration"
        ),
    )
    parser.add_argument(
        "-p",
        "--parts",
        type=int,
        metavar="[1-500]",
        choices=range(1, 501),
        help=(
            "number of parts to read from the Clusterdata 2011 dataset, by "
            "default it reads the full dataset"
        ),
    )
    subparsers = parser.add_subparsers(dest="subcommands")

    parser_q = subparsers.add_parser("questions", description="Run the Spark analysis")
    parser_q.add_argument(
        "--plot",
        action="store_true",
        help=(
            "plot the results of the analysis, by default it is disabled given "
            "the fact that accurate plots need to create huge pandas dataframes"
        ),
    )
    group = parser_q.add_argument_group("questions", "Choose which question to run")
    exclusive_group_q = group.add_mutually_exclusive_group(required=True)
    exclusive_group_q.add_argument(
        "-a",
        action="store_true",
        help="run all questions",
    )
    exclusive_group_q.add_argument(
        "-n",
        choices=range(1, 10),
        metavar="[1-9]",
        type=int,
        nargs="+",
        help=(
            "run specific Spark analysis parts by specifying one of more "
            "associated question numbers"
        ),
    )

    parser_s = subparsers.add_parser(
        "streaming", description="Run the Spark Streaming simulation"
    )
    parser_s.add_argument(
        "--no-docker",
        action="store_true",
        help=(
            "run the Spark Streaming simulation without Docker, in this case "
            "the program expects Kafka to be running"
        ),
    )

    args = parser.parse_args()

    if args.subcommands == "questions":
        if args.a:
            args.n = range(1, 10)

    return args


def main():
    console = Console()
    install(show_locals=True)
    args = parse_args()

    config: Mapping = {
        "spark.jars": "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"
    }

    if args.spark_config:
        config.update(parse_spark_config(args.spark_config))

    if args.subcommands == "streaming":
        config.update(
            {
                "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                "fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            }
        )

    ss = (
        SparkSession.builder.master("local[*]")  # type: ignore
        .appName("SparkDataAnalysis")
        .config(map=config)
        .getOrCreate()
    )
    if args.quiet:
        ss.sparkContext.setLogLevel("OFF")

    ss.conf.set("spark.sql.repl.eagerEval.enabled", True)

    def run():
        if args.subcommands == "streaming":
            from spark_data_analysis.stream.demo import streaming_demo

            streaming_demo(ss, args.parts or "full", not args.no_docker)
        else:
            os.environ["SPARK_DATA_ANALYSIS_PLOT"] = "true" if args.plot else "false"

            for n in args.n:
                module = importlib.import_module(f"spark_data_analysis.questions.q{n}")
                func = getattr(module, f"q{n}")
                console.log(f"{n} - [bold red]{QUESTION_TITLES[n]} [/bold red]")
                start = time.perf_counter()
                func(ss, args.parts or "full")
                end = time.perf_counter()
                console.log(f"Execution time: {end - start:.2f}s")

        if args.compile_pdf:
            status = Status("Compiling the report...")
            status.start()
            typst.compile(TYPST_PATH, output=TYPST_PATH.replace(".typ", ".pdf"))
            status.stop()

    if args.quiet:
        # Suppress output by redirecting stdout to /dev/null
        with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
            run()
    else:
        run()


if __name__ == "__main__":
    main()
