"""
@name: src\silver\utils.py
@author: jan.strocki@hotmail.co.uk

This module provides a utility function, get_bronze_events_per_shape, to retrieve events 
for a specific shape type from the bronze.event Delta table in Spark.
"""

# Standard Imports

from pyspark.sql import SparkSession, DataFrame

##########################################################################################


def get_bronze_events_per_shape(spark: SparkSession, shape_name: str) -> DataFrame:
    """
    Retrieves events of a specific shape from the 'bronze.event' Delta table.
    """
    df = spark.sql(
        f"SELECT * FROM bronze.event WHERE event_type = 'shape' AND event_sub_type = '{shape_name}'"
    )
    return df
