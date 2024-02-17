import sys
import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session():
    """Create and return a Spark session."""
    logging.info("Creating Spark session")
    return SparkSession.builder \
        .appName("KommatiPara Data Processing") \
        .getOrCreate()

def read_data(spark, file_path):
    """Read data from a CSV file into a Spark DataFrame."""
    logging.info(f"Reading data from {file_path}")
    return spark.read.csv(file_path, header=True, inferSchema=True)

def filter_clients_by_country(df, countries):
    """Filter clients by country."""
    logging.info(f"Filtering clients by countries: {countries}")
    return df.filter(col("country").isin(countries))

def remove_personal_info(df, columns_to_keep):
    """Remove personal identifiable information, keeping specified columns."""
    logging.info("Removing personal identifiable information")
    return df.select([col for col in df.columns if col in columns_to_keep])

def join_data(df1, df2, join_col):
    """Join two DataFrames on a specified column."""
    logging.info(f"Joining data on column: {join_col}")
    return df1.join(df2, df1[join_col] == df2[join_col], 'inner').drop(df2[join_col])

def remove_credit_card_number(df):
    """Remove credit card number from DataFrame."""
    logging.info("Removing credit card number from DataFrame")
    return df.drop("credit_card_number")

def rename_columns(df, columns_mapping):
    """Rename columns in the DataFrame as specified."""
    logging.info("Renaming columns")
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

    # Remove personal information from clients data except email, id, and country
    personal_info_cols_to_keep = ["email", "id", "country"]
    clients_cleaned = remove_personal_info(clients_filtered, personal_info_cols_to_keep)

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

    # Convert final_df to a Pandas DataFrame
    pandas_df = final_df.toPandas()

    # Write the Pandas DataFrame to a CSV file in the client_data directory
    output_path = "C:/codc-interviews/client_data/final_output.csv"
    pandas_df.to_csv(output_path, index=False)
    logging.info(f"Data saved in {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        logging.error("Incorrect number of arguments provided")
        sys.exit(1)

    client_info_path = sys.argv[1]
    financial_info_path = sys.argv[2]
    countries = sys.argv[3].split(',')

    main(client_info_path, financial_info_path, countries)
