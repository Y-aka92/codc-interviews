from pyspark.sql import SparkSession

# Initialiseer een SparkSession
spark = SparkSession.builder \
    .appName("Simple PySpark Example") \
    .getOrCreate()

# Creëer een DataFrame met één kolom en één rij met de tekst 'Hello, Spark!'
df = spark.createDataFrame([("Hello, Spark!",)], ["greeting"])

# Toon de inhoud van de DataFrame
df.show()

# Sluit de SparkSession netjes af
spark.stop()
