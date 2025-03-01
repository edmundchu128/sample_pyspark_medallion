import logging.config
import datetime as dt
import pandas as pd
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, TimestampType
import os

# Configure logging
logging.basicConfig(
    filename=f'{dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_pipeline_log.log',
    level=logging.INFO, 
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class SilverPipeline:
    def __init__(self):
        self.spark = self.spark_client()
        self.EXPECTED_SCHEMA = StructType([
            StructField("ticker", StringType()), 
            StructField("company_name", StringType()), 
            StructField("close", DoubleType()),  
            StructField("high", DoubleType()),   
            StructField("low", DoubleType()),    
            StructField("trades", LongType()),   
            StructField("open", DoubleType()),   
            StructField("volume", DoubleType()),   
            StructField("volume_weighted_average_price", DoubleType()),
            StructField("ingested_at", TimestampType()),
            StructField("date", DateType()),
        ])

    @staticmethod
    def spark_client() -> SparkSession:
        """
        Initialize and return a SparkSession.
        
        Returns:
            SparkSession: The Spark session object.
        """
        spark = SparkSession.builder \
            .appName("SilverPipeline") \
            .master("local[*]") \
            .getOrCreate()
        return spark

    @staticmethod
    def validate_schema(df: DataFrame, expected_schema: StructType) -> bool:
        """
        Validate the schema of the DataFrame against the expected schema.
        
        Args:
            df (DataFrame): The DataFrame to validate.
            expected_schema (StructType): The expected schema.
        
        Returns:
            bool: True if the schema matches, False otherwise.
        """

        if df.schema != expected_schema:
            logging.error("Schema validation failed.")
            logging.error("Expected schema: %s", expected_schema.simpleString())
            logging.error("Actual schema: %s", df.schema.simpleString())
            for expected_field, actual_field in zip(expected_schema, df.schema):
                if expected_field != actual_field:
                    logging.error("Field mismatch: Expected field %s, Actual field %s", expected_field, actual_field)
            raise AssertionError("Schema validation failed. The transformed data does not match the expected schema.")
        
        
        assert df.schema == expected_schema

    @staticmethod
    def validate_primary_key(df: DataFrame, primary_key_columns: list) -> bool:
        """
        Validate that the DataFrame does not contain duplicate primary keys.
        
        Args:
            df (DataFrame): The DataFrame to validate.
            primary_key_columns (list): The list of columns that make up the primary key.
        
        Returns:
            bool: True if there are no duplicate primary keys, False otherwise.
        """
        duplicate_count = df.groupBy(primary_key_columns).count().filter(F.col("count") > 1).count()
        if duplicate_count > 0:
            logging.error("Primary key validation failed. Found %d duplicate rows for primary key columns: %s", duplicate_count, primary_key_columns)
            raise AssertionError("Primary key validation failed. The transformed data contains duplicate primary keys.")
        return True

        
    def transform_stocks_to_silver(self, input_folder_path: str, output_file_path: str):
        """
        Transform data from the bronze layer to the silver layer.
        
        Args:
            spark (SparkSession): The Spark session object.
            input_folder_path (str): The path to the input folder containing the bronze data.
            output_file_path (str): The path to the output file for the silver data.
        """

        ### Read bronze data and mapping company name
        logging.info("Reading bronze data from %s", input_folder_path)
        stocks_df = self.spark.read.json(input_folder_path, multiLine=True)
        stocks_mapping_df = self.spark.read.csv("../seeds/stocks.csv", header=True, inferSchema=True)

        logging.info("Transforming bronze data to silver data")

        ### Explode the results column and join with stocks_mapping_df to get company name
        stocks_df = stocks_df.select("ticker", F.explode("results").alias("ticker_data")).withColumn("file_name", F.input_file_name())
        stocks_df = stocks_df.join(stocks_mapping_df, how="left", on=stocks_df.ticker == stocks_mapping_df.symbol)
        
        ###  Rename columns and add date columns and select only required columns
        stocks_df = stocks_df.select(
            stocks_df["ticker"],
            stocks_mapping_df['company_name'].alias("company_name"),
            stocks_df["ticker_data.c"].alias("close"),
            stocks_df["ticker_data.h"].alias("high"),
            stocks_df["ticker_data.l"].alias("low"),
            stocks_df["ticker_data.n"].alias("trades"),
            stocks_df["ticker_data.o"].alias("open"),
            stocks_df["ticker_data.t"].alias("unix_timestamp"),
            stocks_df["ticker_data.v"].alias("volume"),
            stocks_df["ticker_data.vw"].alias("volume_weighted_average_price"),
            F.split(F.reverse(F.split(stocks_df["file_name"], "_"))[0], '.')[0].alias("ingested_at")
        ).withColumn("date", F.to_date(F.from_unixtime(F.col("unix_timestamp") / 1000, "yyyy-MM-dd"))) \
         .withColumn("ingested_at", F.to_timestamp(F.from_unixtime(F.col("ingested_at"))))\
         .drop("unix_timestamp")
        
        ### Validate schema and primary key before writing to silver if passed
        try:
            self.validate_schema(stocks_df, self.EXPECTED_SCHEMA)
            self.validate_primary_key(stocks_df, ["date", "ticker"])
            logging.info(f"Schema validation passed. Writing silver data to {output_file_path}.")
            stocks_df.write.mode("overwrite").parquet(output_file_path)
        except AssertionError as e:
            logging.error(e)
            raise AssertionError(e)

        logging.info("Transformation complete")

if __name__ == "__main__":
    logging.info("Starting SilverPipeline")
    spark = SilverPipeline.spark_client()
    pipeline = SilverPipeline()

    bronze_bucket = "../buckets/bronze/2023-01-01_2024-12-31"
    silver_bucket = "../buckets/silver/stocks.parquet"

    try:
        pipeline.transform_stocks_to_silver(input_folder_path=bronze_bucket, output_file_path=silver_bucket)
    except Exception as e:
        logging.error("Error in transforming bronze to silver: %s", e)
    finally:
        logging.info("SilverPipeline finished")

        