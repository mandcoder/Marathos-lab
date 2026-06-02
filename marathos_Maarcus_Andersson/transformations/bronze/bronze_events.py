from pyspark import pipelines as dp
from pyspark.sql.functions import (
    current_timestamp,
    col
)
from utils.table_config import DEFAULT_TABLE_PROPERTIES

# Base directory where raw CSV files are stored in Unity Catalog volume
BASE_DIR = "/Volumes/marathos/default/raw/events"

@dp.table(
    name="marathos.bronze.raw_ultra_marathons",
    comment="Raw ultra marathon data, ingested with Auto Loader as a streaming source",
    table_properties=DEFAULT_TABLE_PROPERTIES
)

def raw_ultra_marathons():
  
    # Read raw CSV files incrementally using Auto Loader.
    # WHY:
    # - readStream makes this a streaming ingestion.
    # - cloudFiles lets Databricks detect and process new files automatically.
    df = (
    spark.readStream
    .format("cloudFiles")                 # Use Auto Loader for incremental ingestion
    .option("cloudFiles.format", "csv")   # Filformat som Auto Loader ska leta efter
    .option("header", "true")             # Första raden i CSV är kolumnnamn
    .option("cloudFiles.schemaLocation", "/Volumes/marathos/default/raw/events/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # Lägger till nya kolumner när nya filer har ett annat schema
    # WHY: hanterar filer med olika kolumnnamn i samma mapp
    # utan detta kraschar pipelinen eller sätter null för okända kolumner
    .load(BASE_DIR)                       # Mapp att läsa CSV-filer från
)

    # Ingestion metadata columns.
    df = (
        df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "source_file",
            col("_metadata.file_path")
        )
    )
    return df