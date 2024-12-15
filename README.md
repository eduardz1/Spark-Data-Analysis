# Spark-Data-Analysis

To only run the Python code, create a virtual environment (recommended) with `python -m venv .venv` and activate it with `source .venv/bin/activate` (or in Windows `.\.venv\Scripts\activate`). Then install the requirements with `pip install -r requirements.txt`. Afterward, you can show the list of available commands with:

```bash
python -m spark_data_analysis --help
```

## Commands

### Questions

Each question can be run separately, for example the following command will run the first two questions:

```bash
python -m spark_data_analysis --questions 1 2
```

To run all questions, use the following command:

```bash
python -m spark_data_analysis --all
```

### Spark Streaming

You can either choose to run the Spark Streaming application or the questions, to run the Spark Streaming demo use the following command:

```bash
python -m spark_data_analysis --streaming
```

### Compile the report

To compile the report, use the following command:

```bash
python -m spark_data_analysis --report
```
