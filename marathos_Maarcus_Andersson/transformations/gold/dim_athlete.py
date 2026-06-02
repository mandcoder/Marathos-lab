from pyspark import pipelines as dp
from pyspark.sql.functions import col
from utils.table_config import DEFAULT_TABLE_PROPERTIES

# I Create Gold dimension table for athlete
# One row per unique athlete
@dp.table(
    name="marathos.gold.dim_athlete",
    comment="Athlete dimension table from marathon_events",
    table_properties=DEFAULT_TABLE_PROPERTIES
)

def dim_athlete():

    # Read cleaned Silver OBT
    df = dp.read("marathos.silver.marathon_results")

    # Select athlete-related columns
    df = df.select(
        col("athlete_id"),
        col("athlete_year_of_birth"),
        col("athlete_gender"),
        col("athlete_country_code"),
        col("athlete_age_category")
    )

    # Remove duplicate athlete rows
    # Dimension table should contain one row per athlete_id
    df = df.dropDuplicates(["athlete_id"])

    return df
