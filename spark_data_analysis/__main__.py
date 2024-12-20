import argparse
import configparser
import contextlib
import importlib
import os

import typst
from pyspark.sql import SparkSession
from rich.console import Console
from rich.status import Status
from rich.traceback import install

from spark_data_analysis.constants import QUESTION_TITLES, TYPST_PATH


def parse_spark_config(path: str) -> dict:
    config = configparser.ConfigParser()
    config.read(path)
    return dict(config["Spark"])


def parse_args():
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
        "--quiet",
        action="store_true",
        help="suppress additional information during code execution",
    )
    parser.add_argument(
        "--spark_config",
        type=str,
        help=(
            "path to the .ini Spark configuration file, by default it uses the "
            "local configuration"
        ),
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help=(
            "plot the results of the analysis, by default it is disabled given "
            "the fact that accurate plots need to create huge pandas dataframes"
        ),
    )
    group = parser.add_argument_group("questions", "Choose which question to run")

    exclusive_group = group.add_mutually_exclusive_group(required=True)
    exclusive_group.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="run all questions",
    )
    exclusive_group.add_argument(
        "-q",
        "--questions",
        choices=range(1, 10),
        type=int,
        nargs="+",
        help=(
            "run specific Spark analysis parts by specifying one of more "
            "associated question numbers"
        ),
    )
    exclusive_group.add_argument(
        "-s",
        "--streaming",
        action="store_true",
        help="run the Spark Streaming simulation",
    )

    args = parser.parse_args()

    if args.all:
        args.questions = range(1, 10)

    return args


def main():
    console = Console()
    install(show_locals=True)
    args = parse_args()

    os.environ["SPARK_DATA_ANALYSIS_PLOT"] = "true" if args.plot else "false"

    ss = (
        SparkSession.builder.master("local")  # type: ignore
        .appName("SparkDataAnalysis")
        .config(
            "spark.jars",
            "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
        )
        .getOrCreate()
    )
    ss.conf.set("spark.sql.repl.eagerEval.enabled", True)

    if args.spark_config:
        for k, v in parse_spark_config(args.spark_config).items():
            ss.conf.set(k, v)

    def run():
        for q in args.questions:
            module = importlib.import_module(f"spark_data_analysis.questions.q{q}")
            func = getattr(module, f"q{q}")
            console.log(f"{q} - [bold red]{QUESTION_TITLES[q]} [/bold red]")
            func(ss)

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
