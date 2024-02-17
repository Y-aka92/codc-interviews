from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_PYTHON'] = 'python'  # of het volledige pad naar je python executable

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Simple PySpark Example") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()


# Creëer een DataFrame met één kolom en één rij met de tekst 'Hello, Spark!'
df = spark.createDataFrame([("Hello, Spark!",)], ["greeting"])

# Toon de inhoud van de DataFrame
df.show()

# Sluit de SparkSession netjes af
spark.stop()
