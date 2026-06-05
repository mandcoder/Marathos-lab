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
    """
    Read cleaned Silver OBT
    Select event-related columns
    Remove duplicates
    """

    df = dp.read("marathos.silver.marathon_results")

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

    df = df.dropDuplicates(["event_id"])

    return df