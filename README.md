# NYC Taxi Trip Data Analysis (FHVHV & TLC)

This project analyzes NYC taxi trip data (FHVHV and TLC) using PySpark. Due to limitations on Expanse SDSC, most processing and EDA were performed locally on a subset of the dataset.

---

## 1. Data Acquisition
I use publicly available datasets from Kaggle:

- FHVHV dataset  
- TLC Trip Record dataset

To download and extract the datasets, follow these steps (using [Kaggle CLI](https://www.kaggle.com/docs/api)):

#### - Download datasets
- kaggle datasets download -d jeffsinsel/nyc-fhvhv-data --path ~/kaggle_test/
- kaggle datasets download -d salikhussaini49/nyc-tlc-trip-record-data-repository --path ~/kaggle_test/ --force

#### - Unzip downloaded files
- unzip ~/kaggle_test/nyc-fhvhv-data.zip -d ~/kaggle_test/data2
- unzip ~/kaggle_test/nyc-tlc-trip-record-data-repository.zip -d ~/kaggle_test/data2


## 2. Data loading using PySpark
```bash
from pyspark.sql import SparkSession
import glob

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FHVHV Trip Data Analysis") \
    .getOrCreate()

# Load Parquet files (change path as needed)
file_list = sorted(glob.glob("./data1/fhvhv_tripdata_*.parquet"))
df = spark.read.parquet(*file_list)

# Check number of rows
print("Total rows:", df.count())
```

## 3.  Exploratory Data Analysis (EDA)
The following steps were performed:
- Schema inspection
- Dataset size analysis (row/column count)
- Data format validation
- Distribution analysis over time (e.g., trip counts by provider)
- Bar chart visualization of distribution
- Time-based calculations (e.g., duration, pickup vs. drop-off)
- Grouped statistics (mean and median of trip duration)
- Scatter plots were planned but not executed due to local hardware limitations.
