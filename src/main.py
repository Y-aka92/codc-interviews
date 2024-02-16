import os
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ["HADOOP_HOME"] = r"C:\hadoop\bin"  # Note the raw string literal with 'r' or use "C:\\hadoop\\bin"

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

    # Save the output
    final_df.write.mode('overwrite').csv('client_data/output.csv', header=True)

if __name__ == "__main__":
    client_info_path = 'file:///C:/codc-interviews/data/samples/sample_dataset_one.csv'
    financial_info_path = 'file:///C:/codc-interviews/data/samples/sample_dataset_two.csv'
    countries = ['Nederland', 'Duitsland']

    main(client_info_path, financial_info_path, countries)



