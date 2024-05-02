from src.getCovidData import get_covid_data_to_csv;
from src.SparkUtils import create_spark_session,read_csv,verifyAPI;
import json
from src.Q2usingcsv import (
    most_affected_country,
    least_affected_country,
    country_with_highest_cases,
    country_with_minimum_cases,
    total_cases,
    most_efficient_country,
    least_efficient_country,
    least_suffering_country,
    still_suffering_country
)
def main():
    try :
        with open('config.json','r') as f:
            config =json.load(f)
        # Create Spark Session
        spark= create_spark_session()
        url = config['api']['url']
        output = config['spark']['data_file_path'][3:]
        get_covid_data_to_csv(url,output)
        df = read_csv(spark, output)
        print("2.1) Most affected country (total death/total covid cases):", most_affected_country(df))
        print("2.2) Least affected country (total death/total covid cases):", least_affected_country(df))
        print("2.3) Country with highest covid cases:", country_with_highest_cases(df))
        print("2.4) Country with minimum covid cases:", country_with_minimum_cases(df))
        print("2.5) Total cases:", total_cases(df))
        print("2.6) Country that handled the covid most efficiently (total recovery/total covid cases):",
              most_efficient_country(df))
        print("2.7) Country that handled the covid least efficiently (total recovery/total covid cases):",
              least_efficient_country(df))
        print("2.8) Country least suffering from covid (least critical cases):", least_suffering_country(df))
        print("2.9) Country still suffering from covid (highest critical cases):", still_suffering_country(df))

        # Verifying APIs Created in covid_api.py
        # Call the verifyAPI function for each endpoint and print the data
        endpoints = [
            "most-affected-country",
            "least-affected-country",
            "country-with-max-cases",
            "country-with-min-cases",
            "total-cases",
            "max-recovery-to-cases-ratio",
            "min-recovery-to-cases-ratio",
            "least-suffering-country",
            "most-critical-country"
        ]
        for endpoint in endpoints:
            print(f"Verifying endpoint: {endpoint}")
            data = verifyAPI(endpoint)
            if data:
                print(f"{data}")
            else:
                print(f"Failed to verify endpoint: {endpoint}")
            print("-" * 50)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()

