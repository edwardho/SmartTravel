# Vehicle Data Streaming Pipeline through Kafka, Apache Spark, S3, Glue, Athena, Redshift, and Tabeleau

This repository contains an Apache Spark application for real-time vehicle data analytics built using Docker Compose to orchestrate the infrastructure components. The application processes real time vehicle travel data from Kafka using Kafka KRaft, sends them to Apache Spark for processing, which get streamed to the AWS Cloud. The application uses Kafka, Apache Spark streaming into AWS S3 buckets to be crawled by AWS Glue to produce a Data Catalog utilized by Amazon Athena and Redshift. From Redshift, the data can be ported to Tableau or other data analytics platforms like Power BI or Looker Studio.

## Requirements
- Docker
- Confluent Kafka
- Apache Spark
- AWS Cloud
- Tableau (or any other cloud BI tool)

## System Design
![System Design.png](System%20Design.png)

## Installation, Setup, and Usage
1. Clone this repository
2. Navigate to ~/SmartTravel
3. If the following components are not installed execute:
- `pip install confluent_kafka`
- `pip install simplejson`
4. Run `docker compose up -d` to start the required services (Kafka, Apache Spark (Workers and Master))
5. Ensure the ACCESS KEY for the profile has permissions `AmazonS3FullAccess`, `AWSGlueConsoleFullAccess`
6. Create a new set of security credentials for use case "Application running outside AWS" if you do not have them
7. Add your AWS ACCESS KEY and AWS SECRET KEY from your secruity credentials in config.py (Be sure to keep these credentials secure)
8. Create a public S3 Bucket in AWS Cloud. (In my project, it's named eh-spark-streaming-data)
9. Ensure the bucket has the following bucket policy to allow get and put of objects in the bucket
```
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            "Resource": "arn:aws:s3:::[BUCKET_NAME]/*"
        }
    ]
}
```
10. In spark-travel.py, modify the Checkpoint Folder Paths and Output Paths to be in line with your newly created S3 Bucket. (""s3a://[BUCKET_NAME]/checkpoints/vehicle_data")
11. Run the Vehicle Data Generator `main.py` in order to generate the data: Vehicle Data, GPS Data, Camera Data, Weather Data, and Emergency Data
12. Submit the job by running `docker exec -it smarttravel-spark-master-1 spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.664 \
  jobs/spark-travel.py`
  This will start have Kafka ingest the data from `main.py` and stream it to Apache Spark and AWS Cloud from Spark
13. You can access the Spark Master at localhost:9090
14. Once the trip has finished in main.py where it says "Vehicle has reached destination. Simulation ending..." we can crawl the data with AWS Glue to ETL (Extract, Transform, Load) into our AWS Glue Data Catalog
15. Create a new AWS Glue Crawler and select S3 path s3://[BUCKET_NAME]/output/ and crawl all subfolders. Make sure to exclude files matching pattern "_spark_metadata" and anything that looks like it
16. Create an AWSGlue IAM role and attach it to the crawler
17. Create an output Target Database to output results of the crawler
18. Run the AWS Glue Crawler to generate the Glue Catalog and database
19. You can now use AWS Athena to query the database generated via SQL by selecting Data Source AWSDataCatalog and the Databse name created before
20. Create a new Redshift Cluster
21. Create an AWSRedshift IAM role with AmazonS3ReadOnlyAccess, AWSGlueConsoleFullAccess and attach it to the Redshift cluster
22. You should now be able to log into your Redshift cluster with the Redshift url, username, and password to access the database and respective tables
23. In Redshift, you can create a script to access the AWS Glue Data Catalog directly
```
create external schema [SCHEMA_NAME]
from data cataklog
database [DATABASE_NAME]
iam_role [ARN_OF_REDSHIFT_IAM_ROLE]
region [REGION]
```
24. Open port 5439 in your Redshift security group for inbound traffic from Tabeleau Cloud IP address
25. Log into Tableau Cloud and select New > Published Data Source > Connect to Data > Amazon Redshift
26. Enter the Redshift url, port number, and database as we declared previously as well as the username and password and sign in
27. Select the external schema we created with our script and you can now analyze the database data in Tableau

## Debugging
You may need to go into the kafka-kraft broker to clear out old data if you have run the docker job with previous data. To do this use the following commands:
```
kafka-topics --list --bootstrap-server kafka-kraft:29092
kafka-topics --delete --topic vehicle_data --bootstrap-server kafka-kraft:29092
kafka-topics --delete --topic gps_data --bootstrap-server kafka-kraft:29092
kafka-topics --delete --topic camera_data --bootstrap-server kafka-kraft:29092
kafka-topics --delete --topic weather_data --bootstrap-server kafka-kraft:29092
kafka-topics --delete --topic emergency_data --bootstrap-server kafka-kraft:29092
```

## Application Details
The `DataStreamJob` class within the `FlinkTransactions` package serves as the main entry point for the Flink application. The application consumes financial transaction data from Kafka, performs transformations, and stores aggregated results in both Postgres and Elasticsearch.

### Components
#### Apache Kafka
- Facilitates the ingestion and distribution of streaming data.
- Acts as a distributed messaging system for real-time data processing.
- Enables fault-tolerant and scalable event streaming architectures.

#### Apache Spark
- Provides a distributed computing framework for processing large-scale data.
- Supports in-memory processing for iterative algorithms and interactive data analysis.
- Used to process and stream data

#### Amazon S3 (Simple Storage Service)
- Object storage service that allows storing and retrieving data from anywhere on the web.
- Scales seamlessly and provides high durability and availability.
- Stores our raw data and transformed data to be utilized

#### Amazon Glue
- Fully managed extract, transform, and load (ETL) service for preparing and transforming data for analytics.
- Automatically discovers, catalogs, and transforms metadata from various data sources.
- Used to crawl our data in S3 to create our data pipeline

#### Amazon Athena
- Interactive query service that enables querying data stored in Amazon S3 using standard SQL.
- Serverless and requires no infrastructure setup, allowing users to analyze data on-demand.
- Integrates with our AWS Glue Data Catalog in Parquet

#### Amazon Redshift
- Fully managed data warehouse service that enables running complex analytic queries against large datasets.
- Offers high performance and scalability for analytics workloads.
- Supports various data integration and business intelligence tools. (We use Tabelau as an example)

#### Tableau
Data visualization and analytics platform for creating interactive and shareable dashboards.
Connects to multiple data sources including databases, spreadsheets, and cloud services.
Enables users to explore and analyze data visually to uncover insights and trends.





## Configuration
- Kafka settings are defined within the Kafka docker source setup using KRaft.
- AWS connection details (access key and secret key) are defined within config.py under "AWS_ACCESS_KEY" and "AWS_SECRET_KEY"
