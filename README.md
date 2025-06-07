# Milestone2
1. Data Acquisition & EDA Execution
- The dataset was downloaded directly from Kaggle, including:
"fhvhv_tripdata_2019-02.parquet",
"fhvhv_tripdata_2019-03.parquet",
"fhvhv_tripdata_2019-04.parquet",
"fhvhv_tripdata_2019-05.parquet",
"fhvhv_tripdata_2019-06.parquet".
- The total dataset size is approximately 2.6 GB.
- Due to capacity issues on Expanse SDSC, analysis was performed on a local machine using a relatively smaller dataset.

2. Exploratory Data Analysis (EDA) Process
- Schema inspection
- Dataset size analysis (Raw data, number of columns)
- Data table format verification
- Data distribution over time (Taxi service trip counts across different providers)
- Bar chart visualization of data distribution
- Column-based calculations (Time computations)
- Group-wise mean and median calculations based on computed time differences
- Scatter plot visualization was planned but could not be executed due to computing limitations on the local PC


Revision Version

## 1. Data Acquisition

This project uses publicly available datasets from Kaggle for FHVHV and TLC taxi trip records in NYC.

To download and extract the datasets, use the Kaggle CLI (make sure you have it installed and authenticated):

# Download datasets
kaggle datasets download -d jeffsinsel/nyc-fhvhv-data --path ~/kaggle_test/
kaggle datasets download -d salikhussaini49/nyc-tlc-trip-record-data-repository --path ~/kaggle_test/ --force

# Unzip downloaded files
unzip ~/kaggle_test/nyc-fhvhv-data.zip -d ~/kaggle_test/data2
unzip ~/kaggle_test/nyc-tlc-trip-record-data-repository.zip -d ~/kaggle_test/data2


2. Data Loading Using PySpark

# Code : 
from pyspark.sql import SparkSession
import glob

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FHVHV Trip Data Analysis") \
    .getOrCreate()

# Load Parquet files
file_list = sorted(glob.glob("./data1/fhvhv_tripdata_*.parquet"))
df = spark.read.parquet(*file_list)

# Example: Count total rows
print("Total rows:", df.count())


3. Exploratory Data Analysis (EDA) Process
- Schema inspection
- Dataset size analysis (Raw data, number of columns)
- Data table format verification
- Data distribution over time (Taxi service trip counts across different providers)
- Bar chart visualization of data distribution
- Column-based calculations (Time computations)
- Group-wise mean and median calculations based on computed time differences
- Scatter plot visualization was planned but could not be executed due to computing limitations on the local PC
