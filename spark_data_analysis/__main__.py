import argparse
import contextlib
import os
from pyspark.sql import SparkSession

from rich.console import Console
from rich.status import Status
from rich.traceback import install

from spark_data_analysis.questions.q1 import q1
from spark_data_analysis.questions.q2 import q2
from spark_data_analysis.questions.q3 import q3
from spark_data_analysis.questions.q4 import q4
from spark_data_analysis.questions.q5 import q5
from spark_data_analysis.questions.q6 import q6
from spark_data_analysis.questions.q7 import q7
from spark_data_analysis.questions.q8 import q8
from spark_data_analysis.questions.q9 import q9

import typst

TYPST_PATH = "report/report.typ"

questions_config = {
    "q1": {
        "enabled": False,
        "function": q1,
        "title": "Distribution of the machines according to their CPU capacity",
    },
    "q2": {
        "enabled": False,
        "function": q2,
        "title": "Percentage of computational power lost due to maintenance",
    },
    "q3": {
        "enabled": False,
        "function": q3,
        "title": "Distribution of the number of jobs per scheduling class",
    },
    "q4": {
        "enabled": False,
        "function": q4,
        "title": "Probability of eviction of low-scheduling classes",
    },
    "q5": {
        "enabled": False,
        "function": q5,
        "title": "Distribution of tasks from the same job across machines",
    },
    "q6": {
        "enabled": False,
        "function": q6,
        "title": "Resource consumption compared to requested resources",
    },
    "q7": {
        "enabled": False,
        "function": q7,
        "title": "Correlation of resource consumption peaks and evictions",
    },
    "q8": {
        "enabled": False,
        "function": q8,
        "title": "TODO",
    },
    "q9": {
        "enabled": False,
        "function": q9,
        "title": "TODO",
    },
}


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
        for q_key in questions_config.keys():
            questions_config[q_key]["enabled"] = True
    else:
        for q in args.questions:
            # Converts 1 to "q1", 2 to "q2", etc.
            q_key = f"q{str(q)}"
            if q_key in questions_config:
                questions_config[q_key]["enabled"] = True

    return args


def main():
    console = Console()
    install(show_locals=True)
    args = parse_args()

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

    def run():
        for q_id, q_info in questions_config.items():
            if q_info["enabled"]:
                console.log(f"{q_id} - [bold red]{q_info['title']} [/bold red]")
                q_info["function"](ss)

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
