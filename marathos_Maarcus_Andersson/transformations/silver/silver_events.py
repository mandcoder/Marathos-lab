# To understand regex i useed Claude 

from pyspark import pipelines as dp
from pyspark.sql.types import DecimalType 
from pyspark.sql.functions import (col, lower, when, sha2, 
                                    concat_ws, upper, trim, regexp_extract, to_date, lpad)
from utils.column_cleaner import rename_columns_to_snake_case
from utils.table_config import DEFAULT_TABLE_PROPERTIES

# Create Silver table from Bronze layer
# This layer will contain cleaned and validated marathon data

@dp.table(
    name="marathos.silver.marathon_results",
    comment="Cleaned ultra marathon event data from bronze layer",
    table_properties = DEFAULT_TABLE_PROPERTIES
)

def cleaned_ultra_marathons():

    df = spark.sql("FROM STREAM marathos.bronze.raw_ultra_marathons")

    df = rename_columns_to_snake_case(df)

    df = df.withColumnRenamed("event_distance_length", "event_distance")
    df = df.withColumnRenamed("athlete_country", "athlete_country_code")


    # Extract host_country_code BEFORE modifying "event_name"
    # WHY: we need the original event_name with parantheses to extract country code
    # e.g "Marathos Iberia Ultra (ESP)" -> host_country_code = "ESP"
    df = df.withColumn(
        "host_country_code",
        upper(regexp_extract(col("event_name"), r"\((.*?)\)", 1))
        # \(     → find literal "("
        # (.*?)  → capture everything inside parentheses e.g. "ESP"
        # \)     → find literal ")"
        # , 1    → return capture group 1 = "ESP"
        # upper  → ensure uppercase e.g. "esp" → "ESP"
            
    )

    # Clean event_name by removing country code in parentheses
    # WHY: event_name should only contain the race name, not the country code
    # e.g. "Marathos Iberia Ultra (ESP)" → "Marathos Iberia Ultra"

    df = df.withColumn(
        "event_name",
        trim(regexp_extract(col("event_name"), r"^(.*?)\(", 1))
        # ^      → start from beginning of string
        # (.*?)  → capture everything up to...
        # \(     → the literal "("
        # , 1    → return capture group 1 = "Marathos Iberia Ultra "
        # trim   → remove trailing whitespace
    )
       
    # Classify event unit type based on event_distance
    # km = kilometer races
    # mi = mile races
    # h = timed races
    # d = day races (is invalid)

    df = df.withColumn(
        "event_unit_type",
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
    # WHY: event_distance contain mixed values like "80.9km", "25h"
    # I Want to separate the numeric value from the unit
    df = df.withColumn(
        "event_distance_value",
        regexp_extract(col("event_distance"), r"(\d+\.?\d*)", 1).cast("float")
    )

    # Convert distance to km for all events
    # WHY: standardize distance for global stakeholders
    # mi races are converted to km for easy comparison
    # h races are have no distance, set to NULL
    # 1 mile = 1.60934 km
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

    # Extract start and end dates from event_dates
    # WHY: event_dates contains mixed format:
    #  Single day: "08.12.2018"
    #  Multi day:  "10.-12.09.2021"

    # Extract start day
    start_day = regexp_extract(col("event_dates"), r"^(\d+)\.", 1)

    # Extract end day if there are multi day event
    end_day = when(
        col("event_dates").contains("-"),
        regexp_extract(col("event_dates"), r"-(\d+)\.", 1)
    ).otherwise(start_day)
    
    # Extract month and year (always at the end)
    month = regexp_extract(col("event_dates"), r"\.(\d{2})\.", 1)
    year = regexp_extract(col("event_dates"), r"(\d{4})$", 1)

    # Create start and end date columns
    # WHY: convert to proper date format YYYY-MM-DD for downstream analysis
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
    # Create event_id using SHA256 hash based on event_name
    # WHY: dense_rank() does not work in streaming,
    # hash is deterministic and works in streaming
    df = df.withColumn(
        "event_id",
        sha2(concat_ws("_", col("event_name")), 256 )
    )

    df = df.drop("athlete_club", "performance_validity")
    df = df.withColumn("athlete_year_of_birth", col("athlete_year_of_birth").cast("integer"))

    df = df.withColumn("year_of_event", col("year_of_event").cast("integer"))

    # Remove invalid year_of_event
    # WHY: ultra-marathon as organized sport started in the 1900s
    # rows before 1900 are considered data quality issues
    df = df.filter(col("year_of_event") >= 1900)

    # Standardize country codes to uppercase
    df = df.withColumn("athlete_country_code", upper(trim(col("athlete_country_code"))))

    # Remove invalid or unknown country codes
    # WHY:
    #   - null: missing country data, cannot be used for geographic
    # analysis
    # XXX: IOC placeholder for unknown country, not meaningful
    # ACT: Australian Capital Territory (state), not a country code
    
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