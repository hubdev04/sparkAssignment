import os
import logging
import json
from flask import Flask, jsonify
from Q2usingcsv import *
from SparkUtils import read_csv, create_spark_session
from pyspark.sql.functions import col,asc,desc
# Initialize Flask
app = Flask(__name__)

# Load configurations from config.json
with open('../config.json', 'r') as f:
    config = json.load(f)

# Set up logging
log_directory = config['logs']['directory']
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

log_file = os.path.join(log_directory, config['logs']['file'])
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a SparkSession
spark = create_spark_session()

# Read the CSV file into a DataFrame
df = read_csv(spark, config['spark']['data_file_path'])

app = Flask(__name__)

#Creating Api for retireiving data for the given questions.
#1 
@app.route('/covid-data')
def get_covid_data():
    try:
        # Read the CSV file
      
        data = df.to_dict(orient='records')

        # Return the data as JSON
        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.1
@app.route('/most-affected-country')
def most_affected_country():
    try:


        # Calculate the death rate (deaths per cases) for each country
        country_stats = df.select(
            'country',
            'cases',
            'deaths',
            (df["deaths"] / df["cases"]).alias("deaths_per_cases")
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
        # Return the result
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.2
@app.route('/least-affected-country')
def least_affected_country():
    try:
        # Calculate the death rate (deaths per cases) for each country
        country_stats = df.select(
            'country',
            'cases',
            'deaths',
            (df["deaths"] / df["cases"]).alias("deaths_per_cases")
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

        # Return the result
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

#2.3
@app.route('/country-with-max-cases')
def country_with_max_cases():
    try:

        df=df.drop("_c0")
        # Find the country with the maximum number of cases
        max_cases_country = df.select('*').orderBy(desc(col("cases"))).limit(1).collect()[0].asDict()

        # Return the result
        return jsonify(max_cases_country)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.4
@app.route('/country-with-min-cases')
def country_with_min_cases():
    try:

        df=df.drop("_c0")
        # Find the country with the minimum number of cases
        min_cases_country = df.select('*').orderBy(asc(col("cases"))).limit(1).collect()[0].asDict()

        # Return the result
        return jsonify(min_cases_country)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.5
@app.route('/total-cases')
def total_cases():
    try:
        df=df.drop("_c0")
        # Calculate the total number of cases
        total_cases = df.select(sum("cases")).collect()[0][0]

        # Return the result
        return jsonify({'total_cases': total_cases})

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.6
@app.route('/max-recovery-to-cases-ratio')
def max_recovery_to_cases_ratio():
    try:

        df=df.drop("_c0")
        # Calculate recovery to cases ratio and find country with maximum ratio
        max_ratio_country = df.withColumn("recovery_to_cases_ratio", col("recovered") / col("cases")) \
                                    .orderBy(desc("recovery_to_cases_ratio")) \
                                    .select("country","recovery_to_cases_ratio") \
                                    .limit(1) \
                                    .collect()[0].asDict()

        # Return the result
        return jsonify(max_ratio_country)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.7
@app.route('/min-recovery-to-cases-ratio')
def min_recovery_to_cases_ratio():
    try:

        df=df.drop("_c0")
        # Calculate recovery to cases ratio and find country with minimum ratio
        min_ratio_country = df.withColumn("recovery_to_cases_ratio", col("recovered") / col("cases")) \
                                    .orderBy("recovery_to_cases_ratio") \
                                    .select("country","recovery_to_cases_ratio") \
                                    .limit(1) \
                                    .collect()[0].asDict()
        # Return the result
        return jsonify(min_ratio_country)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.8
@app.route('/least-suffering-country')
def least_suffering_country():
    try:
        df=df.drop("_c0")
        # Find the country with the least critical cases
        least_suffering_country = df.orderBy(col("critical")) \
                                          .select("country") \
                                          .limit(1) \
                                          .collect()[0].asDict()

        # Return the result
        return jsonify(least_suffering_country)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
#2.9
@app.route('/most-critical-country')
def most_critical_country():
    try:
        df=df.drop("_c0")
        # Find the country with the most critical cases
        most_critical_country = df.orderBy(col("critical").desc()) \
                                        .select("country") \
                                        .limit(1) \
                                        .collect()[0].asDict()

        # Return the result
        return jsonify(most_critical_country)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
if __name__ == '__main__':
    app.run(debug=True)