
from pyspark.sql import SparkSession
import requests
import pandas as pd
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Read API Data") \
    .getOrCreate()

covidDf=spark.read.option("header","True").option("inferschema","true").csv("./covidData.csv")
newCovidDf=covidDf.drop("_c0")

#Q2.1
from pyspark.sql.functions import desc,asc
newCovidDf.select('country',"cases","deaths",(newCovidDf["deaths"] / newCovidDf["cases"]).alias("deaths per cases")).orderBy(desc(newCovidDf["deaths"] / newCovidDf["cases"])).limit(1).show()

#q 2.2

newCovidDf.select('country',"cases","deaths",(newCovidDf["deaths"] / newCovidDf["cases"]).alias("deaths per cases")).orderBy(asc(newCovidDf["deaths"] / newCovidDf["cases"])).limit(1).show()

#q.3
newCovidDf.select('*').orderBy(desc(newCovidDf["cases"])).limit(1).show()


#2.4)country with minimum covid cases
newCovidDf.select('*').orderBy(asc(newCovidDf["cases"])).limit(1).show()


# 2.5
from pyspark.sql.functions import sum
newCovidDf.select(sum("cases")).collect()[0][0]

#2.6
newCovidDf.withColumn("recovery_to_cases_ratio", col("recovered") / col("cases")) \
                               .orderBy(desc("recovery_to_cases_ratio")) \
                               .select("*") \
                               .limit(1) \
                               .show()


#2.7

newCovidDf.withColumn("recovery_to_cases_ratio", col("recovered") / col("cases")) \
                                 .orderBy("recovery_to_cases_ratio") \
                                 .select("*") \
                                 .limit(1) \
                                 .show()


#2.8

newCovidDf.orderBy(col("critical")) \
                                   .select("*") \
                                   .limit(1) \
                                   .show()

#2.9
most_critical_country = newCovidDf.orderBy(col("critical").desc()) \
                                  .select("*") \
                                  .limit(1) \
                                  .show()