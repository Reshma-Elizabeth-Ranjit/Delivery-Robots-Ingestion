import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, Row
from datetime import datetime
from s3_to_staging import get_schema, validate_schema, validate_and_filter


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.master('local[*]').appName('pytest').getOrCreate()


def test_get_schema():
    schema = get_schema()
    assert isinstance(schema, StructType)


def test_validate_schema():
    schema1 = get_schema()
    schema2 = get_schema()
    assert validate_schema(schema1, schema2) is True
    schema2.fields = schema2.fields[:-1]  # Alter schema
    assert validate_schema(schema1, schema2) is False


def test_validate_and_filter(spark):
    # Setup: Create a DataFrame with test data
    data = [Row(VendorID=1,
                tpep_pickup_datetime=datetime.now(),
                tpep_dropoff_datetime=datetime.now(),
                passenger_count=2,
                trip_distance=5.0,
                RatecodeID=1,
                store_and_fwd_flag='N',
                PULocationID=100,
                DOLocationID=200,
                payment_type=1,
                fare_amount=10.0,
                extra=1.0,
                mta_tax=0.5,
                tip_amount=2.0,
                tolls_amount=0.0,
                improvement_surcharge=0.3,
                total_amount=13.8,
                congestion_surcharge=2.5,
                Airport_fee=0.0
                )]
    test_data_df = spark.createDataFrame(data, get_schema())
    filtered_df = validate_and_filter(test_data_df)
    assert filtered_df is not None
    assert filtered_df.count() == 1
