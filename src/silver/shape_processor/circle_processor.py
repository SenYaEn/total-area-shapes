"""
@name: src\silver\shape_processor\circle_processor.py
@author: jan.strocki@hotmail.co.uk

This module defines the CircleProcessor class, a specialized processor for handling circle shapes, which 
parses JSON data, calculates derived attributes like area, and prepares the data for merging into Delta tables.
"""

# Standard Imports

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

# Specific Imports

from pyspark.sql.functions import from_json, col, md5, pi, concat, current_timestamp

# Local Imports

from src.silver.shape_processor.base_processor import ShapeProcessor

#################################################################################################################


class CircleProcessor(ShapeProcessor):
    """
    Processor for handling circle shapes.
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.json_schema = StructType(
            [
                StructField("type", StringType(), True),
                StructField("radius", StringType(), True),
            ]
        )

    def parse_shape_data(self, shape_df: DataFrame) -> DataFrame:
        return shape_df.withColumn(
            "parsed_json", from_json(col("value"), self.json_schema)
        )

    def add_calculated_columns(self, parsed_df: DataFrame) -> DataFrame:
        return (
            parsed_df.select("loaded_at", "event_id", "parsed_json.radius")
            .withColumn("circle_event_id", md5(concat(col("loaded_at"), col("radius"))))
            .withColumn("area", pi() * col("radius").cast("int") ** 2)
            .withColumn("loaded_at", current_timestamp())
        )
