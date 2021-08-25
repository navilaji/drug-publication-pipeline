from pyspark.sql.types import *

clinical_trial_schema = StructType([
    StructField("id", StringType()),
    StructField("scientific_title", StringType()),
    StructField("date", StringType()),
    StructField("journal", StringType())])


pubmed_schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("date", StringType()),
    StructField("journal", StringType())])

drug_schema = StructType([
    StructField("atccide", StringType()),
    StructField("drug", StringType())])
