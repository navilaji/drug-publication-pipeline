from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("drug-publication-pipeline") \
    .getOrCreate()
