import os
from dotenv import load_dotenv
import holidays
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, to_date
import logging
from pyspark.sql.functions import broadcast

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_holiday_dataset(spark, years_set):
    """
    To get the holiday dataset
    :param years_set: set of years for which the holiday list need to be generated
    :param spark: SparkSession
    :return: holiday dataframe
    """
    holiday_list = holidays.US(years=years_set)
    holiday_data = [{"date": str(date), "name": name} for date, name in holiday_list.items()]

    # Define schema
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("name", StringType(), True)
    ])
    # Create DataFrame from the list of dictionaries
    holiday_df = spark.createDataFrame(holiday_data, schema)
    # Convert 'date' column to date
    holiday_df = holiday_df.withColumn("date", to_date(col("date")))
    return holiday_df


def get_city_zones_data(spark):
    """
    To get the city zones data
    :param spark: SparkSession
    :return: city zones dataframe
    """
    try:
        load_dotenv("local.env")
        city_zones_data_path = os.getenv('CITY_ZONES_DATA_PATH')
        city_zones_df = spark.read.option("header", "true").csv(city_zones_data_path)
        return city_zones_df
    except Exception as e:
        logging.error(f"Error loading city zones data: {e}")
        return None


def create_final_tables(spark):
    """
    To create final delta tables
    :param spark: SparkSession
    :return: None
    """
    try:
        zones_df = get_city_zones_data(spark)
        zones_df.createOrReplaceTempView("zones_table")
        intercity_trip = spark.sql("""
            SELECT 
                pu.Borough AS PickupCity,
                do.Borough AS DropoffCity, y.*
            FROM robot_trip_staged_table y
            LEFT JOIN zones_table pu 
                ON y.PULocationID = pu.LocationID
            LEFT JOIN zones_table do 
                ON y.DOLocationID = do.LocationID
        """)
        intercity_trip = intercity_trip.filter(intercity_trip.PickupCity != intercity_trip.DropoffCity)
        intercity_trip.createOrReplaceTempView("intercity_trip_table")

        # Extract the year to download the holiday for the year
        extract_year = spark.sql(
            "select distinct (year(pickup_date), year(dropoff_date)) as year from robot_trip_staged_table")
        years = extract_year.select("year").distinct().collect()

        # Extracting years from col1 and col2, then storing in a set to avoid duplicates
        years_set = {year for row in years for year in (row.year.col1, row.year.col2)}
        holiday_dataset = get_holiday_dataset(spark, years_set)
        holiday_dataset.createOrReplaceTempView("holiday_dataset_table")
        holiday_trip_result = spark.sql("select * from robot_trip_staged_table y join holiday_dataset_table h where "
                                        "y.pickup_date = h.date or y.dropoff_date = h.date")
        holiday_trip_result.createOrReplaceTempView("holiday_trip_table")
        logging.info("Final Delta tables created successfully")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
