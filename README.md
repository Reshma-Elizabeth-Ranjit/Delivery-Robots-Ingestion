Delivery Robots Ingestion
This project handles the ingestion, validation, cleansing, and processing of delivery robots' trip data using Apache Spark on Databricks. The code is written mainly in Python and PySpark.

Architecture diagram:
An architecture that allows us to collect, process and analyze data from our robots delivery operation as well as send data back to individual robots.
PFB the diagram:
[Delivery_Robot_Ingestion_Framework.pdf](https://github.com/user-attachments/files/18787115/Delivery_Robot_Ingestion_Framework.pdf)

Prerequisites:
Python version: Python 3.11.0
PySpark version: 3.5.4
PyCharm IDE

Installation steps:

1. Clone the repository
2. Open the project in PyCharm
3. Install the required packages in the requirements.txt
4. Set up environment variables in a .env file in the project root (slack URLs or anything containing sensitive data shouldn't be added to git)

How to run the project?
Run the main script, which will call the run the functions in the other files

How to Run Unit tests?
In PyCharm, open the terminal and run the command - "pytest unit_tests/test_s3_to_staging.py"
