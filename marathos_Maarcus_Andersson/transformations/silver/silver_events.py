# To understand regex i useed Claude 

from pyspark import pipelines as dp
from pyspark.sql.functions import(
    col, sha2, concat_ws, trim, regexp_replace
)
from pyspark.sql.types import DecimalType
from utils.column_cleaner import rename_columns_to_snake_case
from utils.table_config import DEFAULT_TABLE_PROPERTIES
from utils.silver_transformations import (
    process_event_name,
    classify_event_unit,
    extract_event_distances,
    extract_event_dates,
    validate_performance,
    filter_invalid_rows,
    standardize_countries,
    create_event_id
)

@dp.table(
    name="marathos.silver.marathon_results",
    comment="Cleaned ultra marathon event data from bronze layer",
    table_properties = DEFAULT_TABLE_PROPERTIES
)


def cleaned_ultra_marathons():
    """
    Cleaning bronze layer into Silver OBT.
    Applies transformations and validation to prepare data for the gold layer
    """

    #  Read data from Bronze table as stream 
    df = spark.sql("FROM STREAM marathos.bronze.raw_ultra_marathons")

    # Rename columns to snake case
    df = rename_columns_to_snake_case(df)

    # Rename columns for clarity
    df = df.withColumnRenamed("event_distance_length", "event_distance")
    df = df.withColumnRenamed("athlete_country", "athlete_country_code")

    # Extract host_country_code and clean event_name
    df = process_event_name(df)
        
    # Classify event unit type based on event_distances
    df = classify_event_unit(df)

    # Extract numeric distance value and convert to km
    df = extract_event_distances(df)
  
    # Extract start and end dates from event_dates
    df = extract_event_dates(df)

    # Validate athlete performance against event unit type
    df = validate_performance(df)

    # Remove invalid rows
    df = filter_invalid_rows(df)

        # Clean athlete_performance by removing unit suffixes
    df = df.withColumn(
        "athlete_performance",
        trim(regexp_replace(col("athlete_performance"), r"\s*(h|km|mi)$", ""))
    )

    # Create event_id using SHA256 hash based on event_name and year_of_event
    df = create_event_id(df)

    # Remove duplicate rows
    df = df.dropDuplicates(["event_id", "athlete_id", "athlete_performance"])

    # Drop column with no business value
    df = df.drop("athlete_club")

    # Cast datatypes
    df = df.withColumn("athlete_year_of_birth", col("athlete_year_of_birth").cast("integer"))
    df = df.withColumn("year_of_event", col("year_of_event").cast("integer"))

    # Remove invalid year_of_event
    df = df.filter(col("year_of_event") >= 1900)

    # Standardize country codes
    df = standardize_countries(df)

    return df
