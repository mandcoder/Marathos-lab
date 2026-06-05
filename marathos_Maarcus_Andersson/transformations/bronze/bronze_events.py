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
  
    # Read raw CSV file incrementally using Auto Loader.
    df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")  
    .option("header", "true")            
    .option("cloudFiles.schemaLocation", "/Volumes/marathos/default/raw/events/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(BASE_DIR)                       
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