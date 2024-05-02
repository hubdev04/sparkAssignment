##  Spark COVID-19 Data Analysis
This project fetches COVID-19 data for selected countries from the [disease.sh API](https://disease.sh/docs/#/COVID-19%3A%20Worldometers/get_v3_covid_19_countries) and performs data analysis using PySpark DataFrame. Additionally, it exposes an API using Flask to display the analyzed data.

## Usage

1. Open the main.py which creates the covidData.csv and create All Apis and return results of all questions.

## Dependencies

- Python file
- Pandas
- Requests (for fetching data from the API)
- Spark
- flask
- Other dependencies as required (specified within the file)

## How to Use

To run the project locally, follow these steps:

```bash
# Clone the repository:
git clone <repository-url>

# Navigate to the project directory:
cd <project-directory>

# Install the required packages using pip:
pip install -r requirements.txt

# Run the Flask application:
python src/covid_api.py

# Run the Main application:
python main.py
```
## Folder Structure

    .
    ├── data 
        ├──output                # data as csv file is sotred
    ├── logs                    # contains logs files
    ├── src                     # contains all python  file which creates Apis and query results from csv file.
    ├── config.json              # contains configuration details
    ├── main.py                  #main python file
    ├── requirements.txt        # contains required dependencies versions.
    └── README.md