import os
import sys  # Voeg deze regel toe
import pandas as pd

# Set the PYSPARK_PYTHON environment variable
os.environ['PYSPARK_PYTHON'] = 'python'
# Set the HADOOP_HOME environment variable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
# Add the Hadoop bin directory to the PATH
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], 'bin')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("KommatiPara Data Processing") \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

def read_data(spark, file_path):
    """Read data from a CSV file into a Spark DataFrame."""
    return spark.read.csv(file_path, header=True, inferSchema=True)

def filter_clients_by_country(df, countries):
    """Filter clients by country."""
    return df.filter(col("country").isin(countries))

def remove_personal_info(df, columns_to_exclude):
    """Remove personal identifiable information, excluding specified columns."""
    return df.select([col for col in df.columns if col in columns_to_exclude or col == 'id'])

def join_data(df1, df2, join_col):
    """Join two DataFrames on a specified column."""
    return df1.join(df2, df1[join_col] == df2[join_col], 'inner').drop(df2[join_col])

def remove_credit_card_number(df):
    """Remove credit card number from DataFrame."""
    return df.drop("credit_card_number")

def rename_columns(df, columns_mapping):
    """Rename columns in the DataFrame as specified."""
    for old_name, new_name in columns_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df

def main(client_info_path, financial_info_path, countries):
    spark = create_spark_session()

    # Read the datasets
    clients_df = read_data(spark, client_info_path)
    financial_df = read_data(spark, financial_info_path)

    # Filter clients from specified countries
    clients_filtered = filter_clients_by_country(clients_df, countries)

    # Remove personal information from clients data
    personal_info_cols = ["email", "id"]  # Keep 'id' for joining
    clients_cleaned = remove_personal_info(clients_filtered, personal_info_cols)

    # Remove credit card number from financial data
    financial_cleaned = remove_credit_card_number(financial_df)

    # Join the datasets on the 'id' field BEFORE renaming
    joined_df = join_data(clients_cleaned, financial_cleaned, "id")

    # Define column renames
    columns_mapping = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type"
    }

    # Rename columns after the join
    final_df = rename_columns(joined_df, columns_mapping)

    # Display the final DataFrame
    final_df.show()

    # Converteer het final_df naar een Pandas DataFrame
    pandas_df = final_df.toPandas()

    # Schrijf het Pandas DataFrame naar een CSV-bestand in de client_data directory
    output_path = "C:/codc-interviews/client_data/final_output.csv"  # Zorg dat deze map bestaat
    pandas_df.to_csv(output_path, index=False)

    print(f"Data opgeslagen in {output_path}")

if __name__ == "__main__":
    # Controleer of alle benodigde argumenten zijn meegegeven
    if len(sys.argv) != 4:
        print("Gebruik: script.py <path_to_client_info> <path_to_financial_info> <country1,country2,...>")
        sys.exit(1)

    # Verkrijg paden en landen van command line
    client_info_path = sys.argv[1]
    financial_info_path = sys.argv[2]
    countries = sys.argv[3].split(',')

    main(client_info_path, financial_info_path, countries)

