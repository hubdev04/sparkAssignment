from pyspark.sql import SparkSession
import requests
import pandas as pd
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Read API Data") \
    .getOrCreate()

url = "http://localhost:5000"

#taking all resukts from Api created in covidDataApi.py

#covidData from Api 
response=requests.get(url+"/covid-data")
data=response.json()
df = pd.DataFrame(data)
print(df)

#2.1
response1=requests.get(url+"/most-affected-country")
data1=response1.json()
df1 = pd.DataFrame(data1)


#2.2
response2=requests.get(url+"/least-affected-country")
data2=response2.json()
df2 = pd.DataFrame(data2)

 #2.3
response3=requests.get(url+"/country-with-max-cases")
data3=response3.json()
df3 = pd.DataFrame(data3)

#2.4
response4=requests.get(url+"/country-with-min-cases")
data4=response4.json()
df4 = pd.DataFrame(data4)

#2.5
response5=requests.get("/total-cases")
data5=response5.json()
df5 = pd.DataFrame(data5)

#2.6
response6=requests.get("/max-recovery-to-cases-ratio")
data6=response6.json()
df6 = pd.DataFrame(data6)

#2.7
response7=requests.get("/min-recovery-to-cases-ratio")
data7=response7.json()
df7 = pd.DataFrame(data7)

#2.8
response8=requests.get("/least-suffering-country")
data8=response8.json()
df8 = pd.DataFrame(data8)

#2.9
response9=requests.get("/most-suffering-country")
data9=response9.json()
df9 = pd.DataFrame(data9)