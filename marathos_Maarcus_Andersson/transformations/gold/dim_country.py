from pyspark import pipelines as dp
from utils.table_config import DEFAULT_TABLE_PROPERTIES

# Gold dimmension table for countries
# One row per unique country code
@dp.table(
    name="marathos.gold.dim_country",
    comment="Country dimension table with country codes and names",
    table_properties=DEFAULT_TABLE_PROPERTIES
)

def dim_country():
    """
    Read country mapping from bronze
    """
    
    df = dp.read("marathos.bronze.raw_countries")

    return df

