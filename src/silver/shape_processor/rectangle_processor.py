"""
@name: src\silver\shape_processor\rectangle_processor.py
@author: jan.strocki@hotmail.co.uk

This module defines the RectangleProcessor class, a specialized processor for handling rectangle shapes, which 
parses JSON data, calculates derived attributes like area, and prepares the data for merging into Delta tables.
"""

# Standard Imports

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

# Specific Imports

from pyspark.sql.functions import from_json, col, md5, concat, current_timestamp

# Local Imports

from src.silver.shape_processor.base_processor import ShapeProcessor

#################################################################################################################


class RectangleProcessor(ShapeProcessor):
    """
    Processor for handling rectangle shapes.
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.json_schema = StructType(
            [
                StructField("width", StringType(), True),
                StructField("type", StringType(), True),
                StructField("height", StringType(), True),
            ]
        )

    def parse_shape_data(self, shape_df: DataFrame) -> DataFrame:
        return shape_df.withColumn(
            "parsed_json", from_json(col("value"), self.json_schema)
        )

    def add_calculated_columns(self, parsed_df: DataFrame) -> DataFrame:
        return (
            parsed_df.select(
                "loaded_at", "event_id", "parsed_json.width", "parsed_json.height"
            )
            .withColumn(
                "rectangle_event_id",
                md5(concat(col("loaded_at"), col("width"), col("height"))),
            )
            .withColumn("area", col("width").cast("int") * col("height").cast("int"))
            .withColumn("loaded_at", current_timestamp())
        )
