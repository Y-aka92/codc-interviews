from pyspark.sql import SparkSession

# Definieer het pad naar de bestanden
client_info_path = 'file:///C:/codc-interviews/data/samples/sample_dataset_one.csv'
financial_info_path = 'file:///C:/codc-interviews/data/samples/sample_dataset_two.csv'

# Start de SparkSession
spark = SparkSession.builder \
    .appName("PySpark File Read Test") \
    .getOrCreate()

# Lees de bestanden in DataFrames
client_info_df = spark.read.csv(client_info_path, header=True, inferSchema=True)
financial_info_df = spark.read.csv(financial_info_path, header=True, inferSchema=True)

# Toon de inhoud van de DataFrames
print("Client Info DataFrame:")
client_info_df.show()

print("Financial Info DataFrame:")
financial_info_df.show()

# Sluit de SparkSession netjes af
spark.stop()
