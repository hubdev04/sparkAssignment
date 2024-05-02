from pyspark.sql import DataFrame

def show_data(df: DataFrame, num_rows: int = 5):
    """
    Render the initial rows of a given Spark DataFrame.

    Args:
        df (DataFrame): The DataFrame from Spark to display.
        num_rows (int): The quantity of rows to render. Defaults to 5.
    """
    df.show(num_rows)


def most_affected_country(df: DataFrame) -> str:
    """
    Determine which country has the highest mortality ratio (deaths to cases).

    Args:
        df (DataFrame): The DataFrame that contains the relevant dataset.

    Returns:
        str: The country with the highest mortality ratio.
    """
    most_affected_country = df.withColumn("affected_ratio", df["deaths"] / df["cases"]) \
        .orderBy("affected_ratio", ascending=False).first()["country"]
    return most_affected_country


def least_affected_country(df: DataFrame) -> str:
    """
    Identify the country with the lowest mortality ratio.

    Args:
        df (DataFrame): The DataFrame containing pertinent data.

    Returns:
        str: The name of the country with the lowest mortality ratio.
    """
    least_affected_country = df.withColumn("affected_ratio", df["deaths"] / df["cases"]) \
        .orderBy("affected_ratio").first()["country"]
    return least_affected_country


def country_with_highest_cases(df: DataFrame) -> str:
    """
    Ascertain the country with the greatest number of reported COVID-19 cases.

    Args:
        df (DataFrame): The DataFrame containing the data.

    Returns:
        str: The country with the most cases.
    """
    country_with_highest_cases = df.orderBy("cases", ascending=False).first()["country"]
    return country_with_highest_cases


def country_with_minimum_cases(df: DataFrame) -> str:
    """
    Determine which country has the fewest COVID-19 cases.

    Args:
        df (DataFrame): The DataFrame housing the data.

    Returns:
        str: The country reporting the fewest cases.
    """
    country_with_minimum_cases = df.orderBy("cases").first()["country"]
    return country_with_minimum_cases


def total_cases(df: DataFrame) -> int:
    """
    Compute the aggregate number of COVID-19 cases.

    Args:
        df (DataFrame): The DataFrame containing the dataset.

    Returns:
        int: The cumulative count of COVID-19 cases.
    """
    total_cases = df.agg({"cases": "sum"}).collect()[0][0]
    return total_cases


def most_efficient_country(df: DataFrame) -> str:
    """
    Identify the nation that has managed COVID-19 recoveries most effectively.

    Args:
        df (DataFrame): The DataFrame containing all necessary data.

    Returns:
        str: The country with the highest recovery rate.
    """
    most_efficient_country = df.withColumn("recovery_ratio", df["recovered"] / df["cases"]) \
        .orderBy("recovery_ratio", ascending=False).first()["country"]
    return most_efficient_country


def least_efficient_country(df: DataFrame) -> str:
    """
    Identify the country with the least efficient COVID-19 recovery rate.

    Args:
        df (DataFrame): The DataFrame containing relevant data.

    Returns:
        str: The country with the lowest recovery ratio.
    """
    least_efficient_country = df.withColumn("recovery_ratio", df["recovered"] / df["cases"]) \
        .orderBy("recovery_ratio").first()["country"]
    return least_efficient_country


def least_suffering_country(df: DataFrame) -> str:
    """
    Identify the country experiencing the least critical cases of COVID-19.

    Args:
        df (DataFrame): The DataFrame with the necessary data.

    Returns:
        str: The country with the fewest critical cases.
    """
    least_suffering_country = df.orderBy("critical").first()["country"]
    return least_suffering_country


def still_suffering_country(df: DataFrame) -> str:
    """
    Pinpoint the country currently grappling with the highest number of critical COVID-19 cases.

    Args:
        df (DataFrame): The DataFrame containing essential data.

    Returns:
        str: The country still heavily impacted by critical cases.
    """
    still_suffering_country = df.orderBy("critical", ascending=False).first()["country"]
    return still_suffering_country
