from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
from pyspark.sql.functions import col, to_timestamp, to_date
import requests
import os
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv("local.env")
env_variable = os.getenv("ENV")

def read_parquet_from_folder(spark, folder_path):
    """
    Reads all Parquet files in a folder (using this function as the storage is in local)
    :param spark: SparkSession
    :param folder_path: the source folder to read the data from
    :return: a DataFrame containing the data in the folder provided
    """
    files = [str(f) for f in Path(folder_path).rglob("*.parquet")]
    if not files:
        raise FileNotFoundError(f"No Parquet files found in {folder_path}")
    return spark.read.parquet(*files)


def get_schema():
    """
    Sets the schema for the Robot's delivery data (yellow taxi trip dataset)
    :return: the schema generated
    """
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
    :param existing_schema: the set schema
    :param new_schema: the schema of the incoming data
    :return: boolean
    """
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
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logging.error("SLACK_WEBHOOK_URL is not set!")
        return
    try:
        response = requests.post(webhook_url, json=message)
        response.raise_for_status()
        logging.info("Message sent successfully!")
    except Exception as e:
        logging.error(f"Failed to send message: {e}")


def validate_and_filter(robot_data_df):
    """
    To run validations and filter anomalies
    :param robot_data_df: the dataframe that has to be validated and filtered
    :return: the filtered dataframe
    """
    logging.info("Validation and cleansing started")

    # Ensuring timestamp format
    robot_data_df = robot_data_df.withColumn(
        "tpep_pickup_datetime",
        to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "tpep_dropoff_datetime",
        to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")
    )

    # Validate schema first
    if not validate_schema(get_schema(), robot_data_df.schema):
        message = {"text": f"ENV: {env_variable} \n Schema mismatch occurred",
                   "username": "DeliveryRobotAlertBot"}
        send_alert(message)
        return None

    # Dropping duplicates
    robot_data_df = robot_data_df.dropDuplicates()

    # Validation for pickup/dropoff locations
    invalid_pickup_location = robot_data_df.filter(col("PULocationID").isNull())
    invalid_dropoff_location = robot_data_df.filter(col("DOLocationID").isNull())
    if invalid_pickup_location.count() > 0 or invalid_dropoff_location.count() > 0:
        message = {"text": f"ENV: {env_variable} \n NULL values found for pickup/dropoff locations",
                   "username": "DeliveryRobotAlertBot"}
        send_alert(message)

    # Validation for pickup/dropoff timestamps
    invalid_pickup_times = robot_data_df.filter(col("tpep_pickup_datetime").isNull())
    invalid_dropoff_times = robot_data_df.filter(col("tpep_dropoff_datetime").isNull())
    if invalid_pickup_times.count() > 0 or invalid_dropoff_times.count() > 0:
        message = {"text": f"ENV: {env_variable} \nNULL values found for pickup/dropoff timestamps",
                   "username": "DeliveryRobotAlertBot"}
        send_alert(message)
        logging.warning("NULL values found for pickup/dropoff timestamps")
        return None

    robot_data_staged_df = robot_data_df.withColumn(
        "pickup_date",
        to_date(col("tpep_pickup_datetime"))
    ).withColumn(
        "dropoff_date",
        to_date(col("tpep_dropoff_datetime"))
    )

    # Check for negative values and filter them out
    negative_values_filtered_df = robot_data_staged_df.filter(
        (col("fare_amount") < 0) &
        (col("trip_distance") < 0) &
        (col("tip_amount") < 0) &
        (col("total_amount") < 0))
    if negative_values_filtered_df.count() > 0:
        message = {"text": f"ENV: {env_variable} \nNegative values found for amount fields",
                   "username": "DeliveryRobotAlertBot"}
        logging.warning("Negative values found for amount fields")
        logging.info(f"Count of records with negative values: {negative_values_filtered_df.count()}")
        send_alert(message)
        robot_data_staged_df = robot_data_staged_df.filter(
            (col("fare_amount") >= 0) &
            (col("trip_distance") >= 0) &
            (col("tip_amount") >= 0) &
            (col("total_amount") >= 0)
        )

    # Check for valid VendorID
    invalid_vendor_id_df = robot_data_staged_df.filter(~col("VendorID").isin([1, 2]))
    if invalid_vendor_id_df.count() > 0:
        message = {"text": f"ENV: {env_variable} \n Invalid Vendor IDs found",
                   "username": "DeliveryRobotAlertBot"}
        logging.warning("Invalid Vendor IDs found")
        logging.info(f"Count of records with invalid Vendor IDs : {invalid_vendor_id_df.count()}")
        send_alert(message)
        robot_data_staged_df = robot_data_staged_df.filter(col("VendorID").isin([1, 2]))
    logging.info("Validation and cleansing done")
    return robot_data_staged_df


def create_staged_table(spark):
    """
    To create staged delta tables
    :param spark: SparkSession
    :return: boolean
    """
    try:
        input_data_path = os.path.join(os.path.dirname(__file__), 'source_data')
        robot_data_df = read_parquet_from_folder(spark, input_data_path)
        robot_data_staged_df = validate_and_filter(robot_data_df)
        if robot_data_staged_df is None:
            return False
        robot_data_staged_df.createOrReplaceTempView("robot_trip_staged_table")
        logging.info("Staging table created successfully")
        return True
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return False
