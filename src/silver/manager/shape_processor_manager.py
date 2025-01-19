"""
@name: src\silver\manager\shape_processor_manager.py
@author: jan.strocki@hotmail.co.uk

This module provides a unified manager (ShapeProcessorManager) to orchestrate shape-specific 
data processing and merging into silver Delta tables using modular processors for each shape type.
"""

# Standard Imports

from pyspark.sql import SparkSession, DataFrame

# Local Imports

from src.silver.shape_processor.circle_processor import CircleProcessor
from src.silver.shape_processor.rectangle_processor import RectangleProcessor
from src.silver.shape_processor.triangle_processor import TriangleProcessor

##############################################################################################


class ShapeProcessorManager:
    """
    A manager class to handle different shape processors and provide a unified interface for processing shapes.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.processors = {
            "rectangle": RectangleProcessor(spark),
            "circle": CircleProcessor(spark),
            "triangle": TriangleProcessor(spark),
        }

    def process_and_merge(self, shape_type: str, shape_df: DataFrame) -> None:
        """
        Process a shape DataFrame and merge the results into a Delta table.

        Args:
            shape_type (str): The type of shape (e.g., 'rectangle', 'circle', 'triangle').
            shape_df (DataFrame): The input DataFrame for the shape.
        """

        if shape_type not in self.processors:
            raise ValueError(f"Unsupported shape type: {shape_type}")

        processor = self.processors[shape_type]
        parsed_df = processor.parse_shape_data(shape_df)
        enriched_df = processor.add_calculated_columns(parsed_df)
        processor.merge_table(
            f"silver.{shape_type}", enriched_df, f"{shape_type}_event_id"
        )
