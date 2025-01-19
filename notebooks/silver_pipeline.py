# Databricks notebook source
# MAGIC %md
# MAGIC This notebook processes shape-specific events from the bronze table and uses the ShapeProcessorManager
# MAGIC to transform and merge the data into corresponding silver tables for further analysis.

# COMMAND ----------

from src.silver.manager.shape_processor_manager import ShapeProcessorManager
from src.silver.utils import get_bronze_events_per_shape

# COMMAND ----------

shapes = ["circle", "rectangle", "triangle"]

for item in shapes:
    df = get_bronze_events_per_shape(spark, item)

    shape_manager = ShapeProcessorManager(spark)

    shape_manager.process_and_merge(item, df)
