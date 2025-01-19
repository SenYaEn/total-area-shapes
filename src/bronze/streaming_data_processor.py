"""
@name: src\bronze\streaming_data_processor.py
@author: jan.strocki@hotmail.co.uk

This module contains StreamingDataProcessor class which processes streaming data for Bronze layer.
"""

# Standard Imports

from pyspark.sql import SparkSession

# Specific Imports

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    concat,
    md5,
    to_json,
    struct,
)

##############################################################################################


class StreamingDataProcessor:
    """
    A class to process streaming data from cloud storage, transform it, and write to a Delta table.

    Attributes:
    spark : SparkSession
        The Spark session used for the operations.
    aws_access_key : str
        The AWS access key for authentication.
    aws_secret_key : str
        The AWS secret key for authentication.
    schema_location : str
        The location where the schema of the files is stored.
    files_path : str
        The path where the incoming files are stored (cloud storage).
    checkpoint_location : str
        The location where checkpoint data is stored for fault tolerance.
    table_name : str
        The Delta table name to write the data into.

    Methods:
    process_stream():
        Starts a streaming job that reads data, processes it, and writes it into a Delta table.
    """

    def __init__(
        self,
        spark: SparkSession,
        aws_access_key: str,
        aws_secret_key: str,
        schema_location: str,
        files_path: str,
        checkpoint_location: str,
        table_name: str,
    ):
        """
        Initializes the StreamingDataProcessor with necessary configurations.

        Parameters:
        spark (SparkSession): The active Spark session.
        aws_access_key (str): The AWS access key.
        aws_secret_key (str): The AWS secret key.
        schema_location (str): The path for schema storage.
        files_path (str): The path to the directory containing the incoming data files.
        checkpoint_location (str): The location to store checkpoint information.
        table_name (str): The Delta table to write the processed data.
        """
        self.spark = spark
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.schema_location = schema_location
        self.files_path = files_path
        self.checkpoint_location = checkpoint_location
        self.table_name = table_name

    def _set_aws_credentials(self):
        """
        Sets AWS credentials in the Hadoop configuration for S3 access.

        This method configures the AWS access and secret keys for Spark to interact with AWS S3.
        """
        try:
            self.spark._jsc.hadoopConfiguration().set(
                "fs.s3n.awsAccessKeyId", self.aws_access_key
            )
            self.spark._jsc.hadoopConfiguration().set(
                "fs.s3n.awsSecretAccessKey", self.aws_secret_key
            )
        except Exception as e:
            raise ValueError(f"Error setting AWS credentials: {e}")

    def _create_streaming_df(self):
        """
        Creates the streaming DataFrame by reading data from cloud storage and applying transformations.

        Returns:
        DataFrame: The processed streaming DataFrame.
        """
        try:
            stream_df = (
                self.spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", self.schema_location)
                .load(self.files_path)
                .withColumn("value", to_json(struct("*")))
                .withColumnRenamed("type", "event_sub_type")
                .withColumn("loaded_at", current_timestamp())
                .withColumn(
                    "event_id",
                    md5(concat(col("loaded_at"), col("event_sub_type"), col("value"))),
                )
                .withColumn("event_type", lit("shape"))
                .select(
                    "event_id", "event_type", "event_sub_type", "value", "loaded_at"
                )
            )
            return stream_df
        except Exception as e:
            raise ValueError(f"Error creating the streaming DataFrame: {e}")

    def _write_to_delta(self, stream_df):
        """
        Writes the processed streaming DataFrame to a Delta table.

        Parameters:
        stream_df (DataFrame): The streaming DataFrame to write to the Delta table.

        Raises:
        ValueError: If there's an issue writing to the Delta table.
        """
        try:
            query = (
                stream_df.writeStream.format("delta")
                .outputMode("append")
                .option("checkpointLocation", self.checkpoint_location)
                .toTable(self.table_name)
            )
            query.awaitTermination()
        except Exception as e:
            raise ValueError(f"Error writing to Delta table {self.table_name}: {e}")

    def process_stream(self):
        """
        Processes the streaming data, applies transformations, and writes it into a Delta table.

        This method combines reading the streaming data, transforming it, and writing it to the Delta table.
        """
        # Set AWS credentials for S3 access
        self._set_aws_credentials()

        # Create the streaming DataFrame
        stream_df = self._create_streaming_df()

        # Write the streaming DataFrame to the Delta table
        self._write_to_delta(stream_df)
        print(f"Streaming data is being written to the Delta table: {self.table_name}")
