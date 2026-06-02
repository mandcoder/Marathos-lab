# To understand regex i useed Claude 

from pyspark import pipelines as dp
from pyspark.sql.types import DecimalType 
from pyspark.sql.functions import (
    col, lower, when, sha2,
    concat_ws, upper, trim,
    regexp_extract, regexp_replace,
    to_date, lpad)

from utils.column_cleaner import rename_columns_to_snake_case
from utils.table_config import DEFAULT_TABLE_PROPERTIES

# Create Silver table from Bronze layer
@dp.table(
    name="marathos.silver.marathon_results",
    comment="Cleaned ultra marathon event data from bronze layer",
    table_properties = DEFAULT_TABLE_PROPERTIES
)


def cleaned_ultra_marathons():
    """
    Cleaning bronze layer
    """

    df = spark.sql("FROM STREAM marathos.bronze.raw_ultra_marathons")

    df = rename_columns_to_snake_case(df)

    df = df.withColumnRenamed("event_distance_length", "event_distance")
    df = df.withColumnRenamed("athlete_country", "athlete_country_code")


    # Extract host_country_code BEFORE modifying "event_name"
    df = df.withColumn(
        "host_country_code",
        upper(regexp_extract(col("event_name"), r"\((.*?)\)", 1))            
    )

    # Clean event_name by removing country code in parentheses
    df = df.withColumn(
        "event_name",
        trim(regexp_extract(col("event_name"), r"^(.*?)\(", 1))
    
    )
       
    # Classify event unit type based on event_distance
    # km = kilometer races
    # mi = mile races
    # h = timed races
    # d = day races (is invalid)

    df = df.withColumn("event_unit_type",
        when(
            lower(col("event_distance")).contains("km"),
            "km"
        )
        .when(
            lower(col("event_distance")).contains("mi"),
            "mi"
        )
        .when(
            lower(col("event_distance")).contains("h"),
            "h"
        )
        .when(
            lower(col("event_distance")).contains("d"),
            "d"
        )
        .otherwise("unknown")
    )

    # Extract numeric value from event_distance
    df = df.withColumn(
        "event_distance_value",
        regexp_extract(col("event_distance"), r"(\d+\.?\d*)", 1).cast("float")
    )

    # Convert distance to km for all events
    df = df.withColumn(
        "event_distance_km",
        when(col("event_unit_type") == 'mi',
             col("event_distance_value") * 1.60934)
        .when(col("event_unit_type") == 'km',
              col("event_distance_value"))
        .otherwise(None)
    )
    
    df = df.withColumn("event_distance_value", col("event_distance_value").cast(DecimalType(10, 1)))
    df = df.withColumn("event_distance_km", col("event_distance_km").cast(DecimalType(10, 1)))

    # Clean athlete_performance by removing unit suffixes
    # This ensures consistent format for hashing in fct_results
    df = df.withColumn("athlete_performance", trim(regexp_replace(col("athlete_performance"), r"\s*(h|km|mi)$", "")))

    # Extract start day
    start_day = regexp_extract(col("event_dates"), r"^(\d+)\.", 1)

    # Extract end day if there are multi-day event
    end_day = when(
        col("event_dates").contains("-"),
        regexp_extract(col("event_dates"), r"-(\d+)\.", 1)
    ).otherwise(start_day)
    
    # Extract month and year
    month = regexp_extract(col("event_dates"), r"\.(\d{2})\.", 1)
    year = regexp_extract(col("event_dates"), r"(\d{4})$", 1)

    # Create columns for event/_start_date and _end_date
    df = df.withColumn(
        "event_start_date",
        to_date(concat_ws("-", year, month, lpad(start_day, 2, "0")), "yyyy-MM-dd")
    ).withColumn(
        "event_end_date",
        to_date(concat_ws("-", year, month, lpad(end_day, 2, "0")), "yyyy-MM-dd")
    )

    # Drop original event_dates
    df = df.drop("event_dates")
    

    # event_distance_value + event_unit_type replaces event_distance.
    df = df.drop("event_distance")


    # I validate athelete performance against event unit type
    df = df.withColumn(
        "performance_validity",
        when(
            (
                (col("event_unit_type") == "km")
                | (col("event_unit_type") == "mi")
            )
            & (col("athlete_performance").contains(":")),
            "valid"
        )
        .when(
            (col("event_unit_type") == "h")
            & (
                lower(col("athlete_performance")).contains("km")
                | lower(col("athlete_performance")).contains("mi")
            ),
            "valid"
        )
        .otherwise("invalid")
        )

    # Remove invalid rows from Silver
    # Remove:
    #      - invalid performance rows
    #      - unknown event types
    #      - races with d
    df = df.filter(
        (col("performance_validity") == "valid")  
        & (col("event_unit_type") != "unknown")
        & (col("event_unit_type") != "d")
    )
    
    # Create event_id using SHA256 hash based on event_name and event_year
    df = df.withColumn(
        "event_id",
        sha2(concat_ws("_", col("event_name"), col ("year_of_event")), 256 )
    )

    # Remove duplicates
    df = df.dropDuplicates(["event_id","athlete_id", "athlete_performance"])
    
    # Drop columns
    df = df.drop("athlete_club", "performance_validity")

    df = df.withColumn("athlete_year_of_birth", col("athlete_year_of_birth").cast("integer"))
    df = df.withColumn("year_of_event", col("year_of_event").cast("integer"))

    # Remove invalid year_of_event
    # WHY: ultra-marathon as organized sport started in the 1900s
    df = df.filter(col("year_of_event") >= 1900)

    # Standardize country codes to uppercase
    df = df.withColumn("athlete_country_code", upper(trim(col("athlete_country_code"))))

    # Remove invalid or unknown country codes
    df = df.filter(
        col("athlete_country_code").isNotNull()
        & (col("athlete_country_code") != "XXX")
        & (col("athlete_country_code") != "ACT"))
    
    COUNTRY_CODE_REPLACEMENT = {
        "DAN": "DEN",
        "IRE": "IRL",
        "GRB": "GBR",
        "SVE": "SWE",
        "MAD": "MDG"
    }

    df = df.replace(COUNTRY_CODE_REPLACEMENT, subset=["athlete_country_code"])
    
    return df