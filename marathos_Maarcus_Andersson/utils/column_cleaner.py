import re

from pyspark.sql import DataFrame

def rename_columns_to_snake_case(df: DataFrame) -> DataFrame:
    """
    Rename all DataFrame columns to snake_case
    and remove technical Bronze columns.
    Example:
    'Event distance/length' -> 'event_distance_length'
    """

    # Rename all columns to snake_case
    df = df.toDF(*[
        re.sub(r"[^a-z0-9_]", "",      # Remove non-alpanumeric except underscore
               re.sub(r"[ /]+", "_",   # replace space and slashes with underscore
                      col.casefold())) # convert to lowercase
        for col in df.columns
    ])

    # Remove technical Bronze columns
    for column in ["_rescued_data", "ingestion_timestamp", "source_file"]:
        if column in df.columns:
            df = df.drop(column)

    return df

