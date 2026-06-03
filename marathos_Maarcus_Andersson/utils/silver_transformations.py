from pyspark.sql.functions import (
    upper,regexp_extract, col, trim, when, lower,lpad, to_date, concat_ws, sha2)

from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType

def process_event_name(df: DataFrame) -> DataFrame:
    """
    Extract host country code and clean event_name.
    
    Example:
    "Marathos Iberia Ultra (ESP)" 
    -> event_name = "Marathos Iberia Ultra"
    -> host_country_code = "ESP"
    
    WHY: both operations must be done together since
    host_country_code must be extracted BEFORE event_name is cleaned
    """

    df = df.withColumn(
        "host_country_code",
        upper(regexp_extract(col("event_name"), r"\((.*?)\)", 1))
    )

    df = df.withColumn(
        "event_name",
        trim(regexp_extract(col("event_name"), r"^(.*?)\(", 1))
    )
    
    return df

def classify_event_unit(df: DataFrame) -> DataFrame:
    """
    Classify event unit type based on event_distance column.

    km  = kilometer races
    mi  = mile races
    h   = timed races
    d   = day races (invalid)
    """
    return df.withColumn(
        "event_unit_type",
        when(lower(col("event_distance")).contains("km"),"km")
        .when(lower(col("event_distance")).contains("mi"),"mi")
        .when(lower(col("event_distance")).contains("h"),"h")
        .when(lower(col("event_distance")).contains("d"),"d")
        .otherwise("unknown")
    )

def extract_event_distances(df: DataFrame) -> DataFrame:
    """
    Extract numeric distance value and convert to km.

    Example:
    "100km" -> event_distance_value = 100.0, event_distance_km = 100.0
    "100mi" -> event_distance_value = 100.0, event_distance_km = 160.9
    "25h"   -> event_distance_value = 25.0,  event_distance_km = NULL
    """

    df = df.withColumn(
        "event_distance_value",
        regexp_extract(col("event_distance"), r"(\d+\.?\d*)", 1).cast("float")
    )

    # Convert distance to km for standardization
    df = df.withColumn(
        "event_distance_km",
        when(col("event_unit_type") == 'mi', col("event_distance_value") * 1.60934)
        .when(col("event_unit_type") == 'km', col("event_distance_value"))
        .otherwise(None)
    )
    # Cast to DecimalType for consistent precision in layers
    df = df.withColumn("event_distance_value", col("event_distance_value").cast(DecimalType(10, 1)))
    df = df.withColumn("event_distance_km", col("event_distance_km").cast(DecimalType(10, 1)))

    # Drop original event_distance
    return df.drop("event_distance")



def extract_event_dates(df: DataFrame) -> DataFrame:
    """
    Extract start and end dates from event_dates column.

    Handles two formats:
    Single day: "08.12.2018"       -> start = end = 2018-12-08
    Multi day:  "10.-12.09.2021"   -> start = 2021-09-10, end = 2021-09-12
    """

    start_day = regexp_extract(col("event_dates"), r"^(\d+)\.", 1)
    
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
    return df.drop("event_dates")

def validate_performance(df: DataFrame) -> DataFrame:
    """
    Validate athlete_performance against event unit type.

    distance races: performance should be in HH:MM:SS format
    time races: performance should contain km or mi.
    """

    df = df.withColumn(
    "performance_validity",
    when(
        ((col("event_unit_type") == "km") | (col("event_unit_type") == "mi"))
        & (col("athlete_performance").contains(":")),
        "valid"
    )
    .when(
        (col("event_unit_type") == "h") & (lower(col("athlete_performance")).contains("km")
            | lower(col("athlete_performance")).contains("mi")
        ),
        "valid"
    )
    .otherwise("invalid")
    )
    return df

def filter_invalid_rows(df: DataFrame) -> DataFrame:
    """
    Remove invalid rows from Silver.
    - invalid performance rows
    - unknown event types
    - day races (d)
    """
    df = df.filter(
    (col("performance_validity") == "valid")  
    & (col("event_unit_type") != "unknown")
    & (col("event_unit_type") != "d")
    )
    return df.drop("performance_validity")

def standardize_countries(df: DataFrame) -> DataFrame:
    """
    - Convert to uppercase
    - Remove invalid codes (NULL, XXX, ACT)
    - Replace duplicates codes with standard IOC codes
    """

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

def create_event_id(df: DataFrame) -> DataFrame:
    """
    Create a unique event_id using SHA256 hash.
    """

    return df.withColumn(
        "event_id",
        sha2(concat_ws("_", col("event_name"), col("year_of_event")), 256)
    )

    







    

