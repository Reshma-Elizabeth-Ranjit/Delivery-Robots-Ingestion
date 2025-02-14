**Delivery Robots Ingestion Framework**

This framework simulates delivery robot trips using NYC Taxi data to demonstrate ingestion, validation, cleansing, and processing workflows. The code is written mainly in Python and PySpark running on Databricks, which leverages Apache Spark for distributed data processing.

Architecture diagram:

An architecture diagram is designed that allows us to collect, process and analyze data from our robots delivery operation as well as send data back to individual robots.

PFB the diagram:

[Delivery_Robot_Ingestion_Framework.pdf](https://github.com/user-attachments/files/18797764/Delivery_Robot_Ingestion_Framework.pdf)


Prerequisites:

- Python version: Python 3.11.0

- PySpark version: 3.5.4

- PyCharm IDE

Installation steps:

1. Clone the repository
2. Open the project in PyCharm
3. Install the required packages in the requirements.txt
4. Set up environment variables in a .env file in the project root (slack URLs or anything containing sensitive data shouldn't be added to git)

   Environment variables used:
    - `SLACK_WEBHOOK_URL`: Webhook URL for Slack notifications.  
    - `SPARK_DRIVER_MEMORY`: Memory allocated for the Spark driver node.
    - `SPARK_EXECUTOR_MEMORY`: Memory allocated for each Spark executor.
    - `CITY_ZONES_DATA_PATH`: Path to the taxi zone lookup CSV file.
    - `ENV`: Environment where the app is running (local, dev, prod). 


How to run the project?

Run the main script, which will call the run the functions in the other files. Run the below command or use the run option in Pycharm.
Command: "python main.py"

How to Run Unit tests?

In PyCharm, open the terminal and run the below command:

Command: "pytest unit_tests/test_s3_to_staging.py"
