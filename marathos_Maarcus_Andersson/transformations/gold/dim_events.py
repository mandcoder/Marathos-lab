from pyspark import pipelines as dp
from pyspark.sql.functions import col
from utils.table_config import DEFAULT_TABLE_PROPERTIES

# Gold dimension table for events
# One row per unique event per year
@dp.table(
    name="marathos.gold.dim_events",
    comment="Event dimension table built from silver marathon_results",
    table_properties=DEFAULT_TABLE_PROPERTIES
)
def dim_event():

    # Read cleaned Silver OBT
    df = dp.read("marathos.silver.marathon_results")

    # Select event-related columns only
    # WHY: dimension table should only contain descriptive attributes
    # not measurable values (those belong in fct_results)
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
    # WHY: same event appears multiple times in silver since
    # one row per athlete result exists, not one row per event
    df = df.dropDuplicates(["event_id"])

    return df