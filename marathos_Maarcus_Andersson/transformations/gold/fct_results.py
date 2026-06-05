

from pyspark import pipelines as dp
from pyspark.sql.functions import col, sha2, concat_ws
from utils.table_config import DEFAULT_TABLE_PROPERTIES

@dp.table(
    name="marathos.gold.fct_results",
    comment="Fact table containing marathon athlete results",
    table_properties=DEFAULT_TABLE_PROPERTIES
)

def fct_results():
    """
    Create stable unique result_id using SHA256 hash
    WHY: hash is deterministic - same input always gives same id
    Columns used: event_id + athlete_id + athlete_performance
    to uniquely identify each result row.
    Table only contains ids and measurable values.
    """

    df = spark.sql("FROM STREAM marathos.silver.marathon_results")


    df = df.withColumn(
        "result_id",
        sha2(concat_ws("_", 
            col("event_id"),
            col("athlete_id"),
            col("athlete_performance")
        ), 256)
    )

    return df.select(
        col("result_id"),
        col("event_id"),
        col("athlete_id"),
        col("athlete_performance"),
        col("athlete_average_speed")
    )