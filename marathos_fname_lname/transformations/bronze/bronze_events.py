from pyspark import pipelines as dp

from pyspark.sql.functions import (
    current_timestamp,
    col
)

BASE_DIR = "/Volumes/marathos/default/raw/TWO_CENTURIES_OF_UM_RACES.csv"

bronze_schema = (
    spark.read.format ("csv")
    .options(header=True, inferSchema=True)
    .load(BASE_DIR).schema
)

@dp.table(
    name="bronze_events",
    comment="Raw ultra marathon event data loaded from CSV file",
    table_properties={
        "delta.columnMapping.mode": "name"
    }
)
def bronze_events():

    # Read raw CSV file
    df = (
        spark.read
        .option("header", True)
        .schema(bronze_schema)
        .csv("/Volumes/marathos/default/raw/TWO_CENTURIES_OF_UM_RACES.csv")
    )

    # Add ingestion metadata columns
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