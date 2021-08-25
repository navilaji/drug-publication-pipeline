from drug_analysis_pipeline.src.helper.spark import spark
from drug_analysis_pipeline.src.schema.schemas import drug_schema, pubmed_schema, clinical_trial_schema
from pyspark.sql.functions import *

class DataParser():
    '''
    '''
    def parse_csv(self, filepath, header, schema, separator=","):
        return spark.read.schema(schema)\
            .format("csv")\
            .option("sep", separator)\
            .option("header", str(header).lower())\
            .load(filepath)

    def parse_json(self, filepath, schema):
        return spark.read.option("multiline","true").schema(schema).json(filepath)

data_parser = DataParser()

class DrugParser():
    def parse_drugs_csv(self, filepath):
        return data_parser.parse_csv(filepath, True, drug_schema, ",")

class ClinicalTrialParser():
    def parse_clinical_trials_csv(self, filepath):
        return data_parser.parse_csv(filepath, True, clinical_trial_schema, ",")

class PubmedParser():
    def parse_pubmed_csv(self, filepath):
        return data_parser.parse_csv(filepath, True, pubmed_schema, ",")

    def parse_pubmed_json(self, filepath):
        return data_parser.parse_json(filepath, pubmed_schema)

drug_parser = DrugParser()
clinical_trial_parser = ClinicalTrialParser()
pubmed_parser = PubmedParser()
