"""
Main file to orchestrate other files
"""
import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import s3_to_staging
import staging_to_delta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_spark_session():
    """
    Creates SparkSession
    - Increased the memory to process large datasets
    :return: SparkSession
    """
    try:
        # Get configurations from environment variables
        load_dotenv("local.env")
        master = os.getenv('SPARK_MASTER', 'local[*]')
        app_name = os.getenv('SPARK_APP_NAME', 'DeliveryRobot')
        driver_memory = os.getenv('SPARK_DRIVER_MEMORY', '4g')
        executor_memory = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')

        # Create SparkSession using environment variables
        spark = (SparkSession.builder.master(master).appName(app_name)
                 .config("spark.driver.memory", driver_memory)
                 .config("spark.executor.memory", executor_memory).getOrCreate())
        return spark
    except Exception as e:
        logging.error("An error occurred while creating spark session: %s", e)
        return None


def main():
    """
    To call the methods to load data from s3 to staging and staging to final layer
    :return: None
    """
    spark = create_spark_session()
    logging.info("Spark session started")
    try:
        logging.info("Starting Delivery Robot data processing pipeline")

        # Read data from S3 and create staging delta tables
        if s3_to_staging.create_staged_table(spark):
            # Create final delta tables after transformations
            staging_to_delta.create_final_tables(spark)
            logging.info("Delivery Robot data processing pipeline completed")
        else:
            logging.warning("Skipping create_final_tables() due to errors in create_staged_table()")
    except Exception as e:
        logging.error("An error occurred: %s", e)
    finally:
        spark.stop()
        logging.info("Spark session stopped")


if __name__ == "__main__":
    main()
