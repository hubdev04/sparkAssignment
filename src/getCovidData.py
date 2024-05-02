from pyspark.sql import SparkSession
import requests
import pandas as pd
from pyspark.sql.functions import col
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)


def get_covid_data_to_csv(url,output):
    try:
        response = requests.get(url)
        data = response.json()
        for item in data:
            if "countryInfo" in item:
                del item["countryInfo"]

        df = pd.DataFrame(data)

        df.to_csv(output, header=True)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching COVID-19 data from {url}: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
