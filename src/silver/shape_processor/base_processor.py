"""
@name: src\silver\shape_processor\base_processor.py
@author: jan.strocki@hotmail.co.uk

This module defines a reusable base class (ShapeProcessor) for shape processing, enforcing a 
consistent interface for parsing, enriching, and merging shape data into Delta tables.
"""

# Standard Imports

from pyspark.sql import SparkSession, DataFrame

##############################################################################################


class ShapeProcessor:
    """
    Base class for shape processors. Enforces a common interface for parsing, enriching
    and merging shapes.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def parse_shape_data(self, shape_df: DataFrame) -> DataFrame:
        raise NotImplementedError("parse_shape_data method must be implemented.")

    def add_calculated_columns(self, parsed_df: DataFrame) -> DataFrame:
        raise NotImplementedError("add_calculated_columns method must be implemented.")

    def merge_table(self, table_name: str, df: DataFrame, merge_key: str) -> None:
        """
        Merges the processed shape data into the target Delta table.
        """
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forName(self.spark, table_name)
        delta_table.alias("target").merge(
            df.alias("source"), f"target.{merge_key} = source.{merge_key}"
        ).whenNotMatchedInsertAll().execute()
