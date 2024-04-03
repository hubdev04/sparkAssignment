from pyspark.sql import SparkSession
import requests
import pandas as pd
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Read API Data") \
    .getOrCreate()
#Api requesting using API
url = "https://disease.sh/v3/covid-19/countries/india,france,germany,brazil,south%20korea,japan,italy,uk,russia,turkey,spain,australia,vietnam,taiwan,argentina,netherlands,mexico,iran,indonesia,"



response = requests.get(url)
data = response.json()
for item in data:
    if "countryInfo" in item:
        del item["countryInfo"]

df = pd.DataFrame(data)

df.to_csv("./covidData.csv", header=True)
