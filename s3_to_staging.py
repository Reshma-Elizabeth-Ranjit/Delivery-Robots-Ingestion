from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
from pyspark.sql.functions import col, to_timestamp, to_date
import requests
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_parquet_from_folder(spark, folder_path):
    """
    Reads all Parquet files in a folder (using this function as the storage is in local)
    :param spark: SparkSession
    :param folder_path: the source folder to read the data from
    :return: DataFrame
    """
    files = [str(f) for f in Path(folder_path).rglob("*.parquet")]
    if not files:
        raise FileNotFoundError(f"No Parquet files found in {folder_path}")

    return spark.read.parquet(*files)


def get_schema():
    """
    Sets the schema for the yellow taxi trip dataset
    :return: schema
    """
    # Define the schema
    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", LongType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True)
    ])
    return schema


def validate_schema(existing_schema, new_schema):
    """
    To check if the incoming schema matches with the existing schema
    :param existing_schema:
    :param new_schema:
    :return:
    """
    # To ignore
    existing_fields = {field.name: field.dataType for field in existing_schema.fields}
    new_fields = {field.name: field.dataType for field in new_schema.fields}

    if existing_fields != new_fields:
        logging.error(f"Schema mismatch:\nExisting Schema: {existing_schema}\nNew Schema: {new_schema}")
        return False
    logging.info("Schema matches")
    return True


def send_alert(message):
    """
    To send alert to slack channel
    :param message: The alert message that has to be sent
    :return: None
    """
    # Slack Webhook URL
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logging.error("SLACK_WEBHOOK_URL is not set!")
        return
    try:
        # Send the message
        response = requests.post(webhook_url, json=message)

        # Check if the message was sent successfully
        response.raise_for_status()

        # If the status code is 200, the message was sent successfully
        logging.info("Message sent successfully!")
    except Exception as e:
        logging.error(f"Failed to send message: {e}")


def create_staged_table(spark):
    try:
        # Read the data from the folder (In real scenario from AWS S3 location)
        input_data_path = os.path.join(os.path.dirname(__file__), 'source_data')
        robot_data_df = read_parquet_from_folder(spark, input_data_path)

        # Remove duplicate records
        robot_data_df = robot_data_df.dropDuplicates()
        invalid_pickup_location = robot_data_df.filter(col("PULocationID").isNull())
        invalid_dropoff_location = robot_data_df.filter(col("DOLocationID").isNull())
        if invalid_pickup_location.count() > 0 or invalid_dropoff_location.count() > 0:
            message = {"text": "NULL values found for pickup/dropoff locations"}
            send_alert(message)

        invalid_pickup_times = robot_data_df.filter(col("tpep_pickup_datetime").isNull())
        invalid_dropoff_times = robot_data_df.filter(col("tpep_dropoff_datetime").isNull())

        # Proceed with further processing if no invalid timestamps are found
        if invalid_pickup_times.count() == 0 and invalid_dropoff_times.count() == 0:
            # Ensure date/timestamp format
            robot_data_staged_df = robot_data_df.withColumn(
                "tpep_pickup_datetime",
                to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
            ).withColumn(
                "tpep_dropoff_datetime",
                to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")
            )
            if not validate_schema(get_schema(), robot_data_staged_df.schema):
                return False
            robot_data_staged_df = robot_data_df.withColumn(
                "pickup_date",
                to_date(col("tpep_pickup_datetime"))
            ).withColumn(
                "dropoff_date",
                to_date(col("tpep_dropoff_datetime"))
            )
            robot_data_staged_df.createOrReplaceTempView("robot_trip_staged_table")
            logging.info("Staging table created successfully")
            return True
        else:
            message = {"text": "NULL values found for pickup/dropoff timestamps"}
            send_alert(message)
            logging.warning("NULL values found for pickup/dropoff timestamps")
            return False
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return False
