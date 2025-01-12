# Spark-Data-Analysis

## Preliminaries

Before being able to run the program you need to (1) Install the `gcloud` CLI from Google Cloud Services. (2) Initialize it. (3) Set up a default application login.

### Installing the `gcloud` CLI

On Debian based systems first add the Google Cloud SDK distribution URI as package source

```bash
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
```

Import the public key

```bash
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - 
```

Update the package list

```bash
sudo apt update
```

and install the SDK

```bash
sudo apt install google-cloud-sdk
```

#### Initializing the CLI

To initialize it run

```bash
gcloud init
```

Follow the steps, you will be prompted to log-in with your google account from the browser and select or create a new project.

#### Setting up a default application login

Run the command

```bash
gcloud auth application-default login
```

and add `GOOGLE_APPLICATION_CRDENTIALS=/home/{username}/.config/gcloud/application_default_credentials.json` to your environment variables

### Creating a new virtual environment

Create a virtual environment (recommended) with `python -m venv .venv` and activate it with `source .venv/bin/activate` (or in Windows `.\.venv\Scripts\activate`). Then install the requirements with `pip install -r requirements.txt`.

## Commands

To get a list of available commands run

```bash
python -m spark_data_analysis --help
```

### Questions

Each question can be run separately, for example the following command will run the first two questions:

```bash
python -m spark_data_analysis questions -n 1 2
```

To run all questions, use the following command:

```bash
python -m spark_data_analysis questions -all
```

### Spark Streaming

You can either choose to run the Spark Streaming application or the questions, to run the Spark Streaming demo use the following command:

```bash
python -m spark_data_analysis -p 1 streaming
```

`-p 1` tells the program to use only one part of the dataset.

#### Kafka and Zookeeper

To run the Spark Streaming application you need to have Kafka and Zookeeper running. For this you can use the `docker-compose.yml` file provided in the `docker` directory. Starting from [Kafka 3.3](https://www.confluent.io/blog/apache-kafka-3-3-0-new-features-and-updates/) Zookeeper is no longer a requirement for running a Kafka broker so we are omitting it in our compose file, we're also using the native (faster even if less feature rich) version of Kafka. You can start the services manually with the following command:

```bash
docker compose up -d
```

But by default the demo will automatically start the services for you unless you specify the `--no-docker` flag.

```bash
python -m spark_data_analysis streaming --no-docker
```

### Compile the report

To compile the report, use the following command:

```bash
python -m spark_data_analysis --report
```
