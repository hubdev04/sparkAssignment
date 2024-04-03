from flask import Flask, jsonify
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,asc,col,sum
app = Flask(__name__)

#Creating Api for retireiving data for the given questions.
#1 
@app.route('/covid-data')
def get_covid_data():
    try:
        # Read the CSV file
        df = pd.read_csv('./covidData.csv')

        # Convert the DataFrame to a list of dictionaries
        data = df.to_dict(orient='records')

        # Return the data as JSON
        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.1
@app.route('/most-affected-country')
def most_affected_country():
    try:
        spark = SparkSession.builder \
            .appName("MostAffectedCountry") \
            .getOrCreate()

        # Read the CSV file into a DataFrame
        covid_df = spark.read.csv('./covidData.csv', header=True)

        # Calculate the death rate (deaths per cases) for each country
        country_stats = covid_df.select(
            'country',
            'cases',
            'deaths',
            (covid_df["deaths"] / covid_df["cases"]).alias("deaths_per_cases")
        ).orderBy(desc("deaths_per_cases"))

        # Get the most affected country
        most_affected_country = country_stats.limit(1).collect()[0]

        # Convert the result to a dictionary
        result = {
            'most_affected_country': {
                'country': most_affected_country['country'],
                'cases': most_affected_country['cases'],
                'deaths': most_affected_country['deaths'],
                'deaths_per_cases': most_affected_country['deaths_per_cases']
            }
        }

        # Stop the SparkSession
        spark.stop()

        # Return the result
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.2
@app.route('/least-affected-country')
def least_affected_country():
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("LeastAffectedCountry") \
            .getOrCreate()

        # Read the CSV file into a DataFrame
        covid_df = spark.read.csv('covidData.csv', header=True)

        # Calculate the death rate (deaths per cases) for each country
        country_stats = covid_df.select(
            'country',
            'cases',
            'deaths',
            (covid_df["deaths"] / covid_df["cases"]).alias("deaths_per_cases")
        ).orderBy(asc("deaths_per_cases"))

        # Get the least affected country
        least_affected_country = country_stats.limit(1).collect()[0]

        # Convert the result to a dictionary
        result = {
            'least_affected_country': {
                'country': least_affected_country['country'],
                'cases': least_affected_country['cases'],
                'deaths': least_affected_country['deaths'],
                'deaths_per_cases': least_affected_country['deaths_per_cases']
            }
        }

        # Stop the SparkSession
        spark.stop()

        # Return the result
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

#2.3
@app.route('/country-with-max-cases')
def country_with_max_cases():
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("CountryWithMaxCases") \
            .getOrCreate()

        # Read the CSV file into a DataFrame
        covid_df = spark.read.csv('covidData.csv', header=True)
        covid_df=covid_df.drop("_c0")
        # Find the country with the maximum number of cases
        max_cases_country = covid_df.select('*').orderBy(desc(col("cases"))).limit(1).collect()[0].asDict()

        # Stop the SparkSession
        spark.stop()

        # Return the result
        return jsonify(max_cases_country)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.4
@app.route('/country-with-min-cases')
def country_with_min_cases():
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("CountryWithMinCases") \
            .getOrCreate()

        # Read the CSV file into a DataFrame
        covid_df = spark.read.csv('covidData.csv', header=True)
        covid_df=covid_df.drop("_c0")
        # Find the country with the minimum number of cases
        min_cases_country = covid_df.select('*').orderBy(asc(col("cases"))).limit(1).collect()[0].asDict()

        # Stop the SparkSession
        spark.stop()

        # Return the result
        return jsonify(min_cases_country)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.5
@app.route('/total-cases')
def total_cases():
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("TotalCases") \
            .getOrCreate()

        # Read the CSV file into a DataFrame
        covid_df = spark.read.csv('covidData.csv', header=True)
        covid_df=covid_df.drop("_c0")
        # Calculate the total number of cases
        total_cases = covid_df.select(sum("cases")).collect()[0][0]

        # Stop the SparkSession
        spark.stop()

        # Return the result
        return jsonify({'total_cases': total_cases})

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.6
@app.route('/max-recovery-to-cases-ratio')
def max_recovery_to_cases_ratio():
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("MaxRecoveryToCasesRatio") \
            .getOrCreate()

        # Read the CSV file into a DataFrame
        covid_df = spark.read.csv('covidData.csv', header=True)
        covid_df=covid_df.drop("_c0")
        # Calculate recovery to cases ratio and find country with maximum ratio
        max_ratio_country = covid_df.withColumn("recovery_to_cases_ratio", col("recovered") / col("cases")) \
                                    .orderBy(desc("recovery_to_cases_ratio")) \
                                    .select("country","recovery_to_cases_ratio") \
                                    .limit(1) \
                                    .collect()[0].asDict()

        # Stop the SparkSession
        spark.stop()

        # Return the result
        return jsonify(max_ratio_country)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.7
@app.route('/min-recovery-to-cases-ratio')
def min_recovery_to_cases_ratio():
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("MinRecoveryToCasesRatio") \
            .getOrCreate()

        # Read the CSV file into a DataFrame
        covid_df = spark.read.csv('covidData.csv', header=True)
        covid_df=covid_df.drop("_c0")
        # Calculate recovery to cases ratio and find country with minimum ratio
        min_ratio_country = covid_df.withColumn("recovery_to_cases_ratio", col("recovered") / col("cases")) \
                                    .orderBy("recovery_to_cases_ratio") \
                                    .select("country","recovery_to_cases_ratio") \
                                    .limit(1) \
                                    .collect()[0].asDict()

        # Stop the SparkSession
        spark.stop()

        # Return the result
        return jsonify(min_ratio_country)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.8
@app.route('/least-suffering-country')
def least_suffering_country():
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("LeastSufferingCountry") \
            .getOrCreate()

        # Read the CSV file into a DataFrame
        covid_df = spark.read.csv('covidData.csv', header=True)
        covid_df=covid_df.drop("_c0")
        # Find the country with the least critical cases
        least_suffering_country = covid_df.orderBy(col("critical")) \
                                          .select("country") \
                                          .limit(1) \
                                          .collect()[0].asDict()

        # Stop the SparkSession
        spark.stop()

        # Return the result
        return jsonify(least_suffering_country)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.9
@app.route('/most-critical-country')
def most_critical_country():
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("MostCriticalCountry") \
            .getOrCreate()

        # Read the CSV file into a DataFrame
        covid_df = spark.read.csv('covidData.csv', header=True)
        covid_df=covid_df.drop("_c0")
        # Find the country with the most critical cases
        most_critical_country = covid_df.orderBy(col("critical").desc()) \
                                        .select("country") \
                                        .limit(1) \
                                        .collect()[0].asDict()

        # Stop the SparkSession
        spark.stop()

        # Return the result
        return jsonify(most_critical_country)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
if __name__ == '__main__':
    app.run(debug=True)