from pyspark.sql import SparkSession

def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder.appName("KommatiPara Data Processing Test").getOrCreate()

# Test de SparkSession creatie
if __name__ == "__main__":
    spark = create_spark_session()
    print("SparkSession is succesvol aangemaakt.")
    spark.stop()  # Sluit de SparkSession netjes af na de test


# Dit is een eenvoudig Python-script dat 'Hello, World!' print.
print("Hello, World!")
