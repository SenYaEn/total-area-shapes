# Databricks notebook source
# MAGIC %md
# MAGIC This notebook processes streaming data from S3 Bucket and writes it to a Delta table.

# COMMAND ----------

# Local Imports
from src.bronze.streaming_data_processor import StreamingDataProcessor

# COMMAND ----------

processor = StreamingDataProcessor(
    spark=spark,
    aws_access_key=dbutils.secrets.get(scope="DevOpsSecret", key="s3Key"),
    aws_secret_key=dbutils.secrets.get(scope="DevOpsSecret", key="s3Secret"),
    schema_location=dbutils.widgets.get("schema_location"),
    files_path=dbutils.widgets.get("files_path"),
    checkpoint_location=dbutils.widgets.get("checkpoint_location"),
    table_name=dbutils.widgets.get("table_name"),
)
processor.process_stream()
