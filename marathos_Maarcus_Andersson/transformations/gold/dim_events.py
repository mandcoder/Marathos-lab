from pyspark import pipelines as dp
from pyspark.sql.functions import col
from utils.table_config import DEFAULT_TABLE_PROPERTIES

# I create Gold dimenstion table for events
# One row per unique event
@dp.table(
    name="marathos.gold.dim_events",
    comment="Event dimension table built from silver marathon_results",
    table_properties=DEFAULT_TABLE_PROPERTIES
)

def dim_event():

    # I read cleaned Silver OBT
    df = dp.read("marathos.silver.marathon_results")

    # I select related columns
    df = df.select(
        col("event_id"),
        col("event_unit_type"),
        col("event_name"),
        col("event_start_date"),
        col("event_end_date"),
        col("year_of_event"),
        col("event_number_of_finishers"),
        col("event_distance_value"),
        col("event_distance_km"),
        col("host_country_code")
    )

    # Remove duplicate event rows
    # Dimension table should contain one row per event_id
    df = df.dropDuplicates(["event_id"])

    return df
