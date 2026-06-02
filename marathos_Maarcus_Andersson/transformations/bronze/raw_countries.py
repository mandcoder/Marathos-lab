from pyspark import pipelines as dp
from utils.table_config import DEFAULT_TABLE_PROPERTIES

@dp.table(
    name="marathos.bronze.raw_countries",
    comment="Raw country code to country name mapping",
    table_properties=DEFAULT_TABLE_PROPERTIES
)

def raw_countries():

    # Read country mapping CSV from Unity Catalog volume
    # WHY: batch read is sufficient since country data is static
    # and does not require incremental streaming ingestion
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/marathos/default/raw/countries/country_mapping.csv")
    )

    return df
    