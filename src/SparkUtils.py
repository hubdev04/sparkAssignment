from pyspark.sql import SparkSession
import requests


def create_spark_session():
    ## Create Spark session
    spark = SparkSession.builder \
    .appName("Covid-Data") \
    .getOrCreate()
    return spark

def read_csv(spark, file_path):
    """
    Read a CSV file into a DataFrame using the provided SparkSession.

    Args:
        spark (SparkSession): The SparkSession object.
        file_path (str): The path to the CSV file.

    Returns:
        DataFrame: The DataFrame containing the data from the CSV file.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df


def verifyAPI(route):
    url = f"http://127.0.0.1:5000/{route}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for non-200 status codes
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching data from {url}: {e}")
        return None