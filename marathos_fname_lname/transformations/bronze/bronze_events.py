from pyspark import pipelines as dp

from pyspark.sql.functions import (
    current_timestamp,
    col
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

# Define raw Bronze schema
# All columns are stored as strings due to messy data discovered during EDA
bronze_schema = StructType([
    StructField("year_of_event", StringType(), True),
    StructField("event_dates", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_distance_length", StringType(), True),
    StructField("event_number_of_finishers", StringType(), True),
    StructField("athlete_performance", StringType(), True),
    StructField("athlete_club", StringType(), True),
    StructField("athlete_country", StringType(), True),
    StructField("athlete_year_of_birth", StringType(), True),
    StructField("athlete_gender", StringType(), True),
    StructField("athlete_age_category", StringType(), True),
    StructField("athlete_average_speed", StringType(), True),
    StructField("athlete_id", StringType(), True),
])


@dp.table(
    name="bronze_events",
    comment="Raw ultra marathon event data loaded from CSV file"
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