# I used Claude for some part of this file.

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, year, month, dayofmonth, dayofweek, quarter, date_format, lit, date_add, to_date
    #  lit convert string to a column
)  

from utils.table_config import DEFAULT_TABLE_PROPERTIES

@dp.table(
    name="marathos.gold.dim_date",
    comment="Date dimension table from 1900 to 2030",
    table_properties=DEFAULT_TABLE_PROPERTIES
)

def dim_date():
        
    # Create date-range 1900-01-01 to 2030-12-31 and extract attributes from "date"
    df = (spark.range(0, 47847)
    .withColumn(
        "date", # name for the new column
        date_add(to_date(lit("1900-01-01")), col("id").cast("int")))
    .withColumn("year", year(col("date")))
    .withColumn("month",month(col("date")))
    .withColumn("weekday",dayofweek(col("date")))
    .withColumn("day_of_month",dayofmonth(col("date")))
    .withColumn("quarter",quarter(col("date")))
    .withColumn("weekday_number",((dayofweek(col("date")) + 5) % 7 + 1)) # convert to monday as first day of week
    .withColumn("weekday_name",date_format(col("date"), "EEEE"))
    .withColumn("month_name",date_format(col("date"), "MMMM"))
    .withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast("int"))
    .drop("id")
    .drop("weekday")

    )
    return df 
