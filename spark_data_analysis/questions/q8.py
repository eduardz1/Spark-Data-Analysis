from typing import Literal

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum

from spark_data_analysis.constants import IMGS_PATH
from spark_data_analysis.datasets.clusterdata_2011_2 import task_events


def q8(ss: SparkSession, parts: int | Literal["full"]):
    te = task_events(ss, parts)
    print("Dataset task_events caricato.")
    print(
        f"Numero totale di righe nel dataset: {te.count()}"
    )  # Debug: Controllo del numero di righe iniziali

    # Filtra le righe dove EventType è 0 (Submit)
    submit_tasks = te.filter(col("EventType") == 0)
    print("Filtrate le righe con EventType == 0 (Submit).")
    print(
        f"Numero di righe dopo il filtro: {submit_tasks.count()}"
    )  # Debug: Controllo delle righe filtrate

    # Raggruppa per SchedulingClass e calcola il numero di attività inviate
    tasks_per_class = (
        submit_tasks.groupBy("SchedulingClass")
        .agg(
            spark_sum("CPURequest").alias("TotalCPUs"),
            spark_sum("MemoryRequest").alias("TotalMemory"),
        )
        .orderBy("SchedulingClass")
    )
    print("Calcolata la somma di CPUs e Memory per SchedulingClass.")
    print(
        f"Numero di righe nel risultato aggregato: {tasks_per_class.count()}"
    )  # Debug: Numero di righe aggregate
    tasks_per_class.show()  # Debug: Mostra il risultato aggregato

    # Stampa i risultati in console
    print("Distribuzione delle risorse (CPUs e Memory) per SchedulingClass:")
    for row in tasks_per_class.collect():
        print(
            f"Scheduling Class: {row['SchedulingClass']}, Total CPUs: {row['TotalCPUs']}, Total Memory: {row['TotalMemory']}"
        )

    # Aggiungi grafici con matplotlib
    import matplotlib.pyplot as plt

    # Trasforma i risultati in un formato utilizzabile con matplotlib
    results = tasks_per_class.collect()
    scheduling_classes = [str(row["SchedulingClass"]) for row in results]
    total_cpus = [row["TotalCPUs"] for row in results]
    total_memory = [row["TotalMemory"] for row in results]

    print("Generazione dei grafici con matplotlib.")
    # Grafico per CPUs
    plt.figure(figsize=(10, 6))
    plt.bar(scheduling_classes, total_cpus, color="skyblue")
    plt.title("CPUs distribution for each scheduling class")
    plt.xlabel("Scheduling Class")
    plt.ylabel("Total CPUs")
    plt.savefig(f"{IMGS_PATH}/cpu_dist_schedulingclass.svg")

    # Grafico per Memory
    plt.figure(figsize=(10, 6))
    plt.bar(scheduling_classes, total_memory, color="salmon")
    plt.title("Memory distribution for each scheduling class")
    plt.xlabel("Scheduling Class")
    plt.ylabel("Total Memory")
    plt.savefig(f"{IMGS_PATH}/mem_dist_schedulingclass.svg")
    plt.close()
