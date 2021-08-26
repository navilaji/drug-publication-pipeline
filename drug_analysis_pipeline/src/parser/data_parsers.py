from drug_analysis_pipeline.src.helper.spark import spark
from drug_analysis_pipeline.src.schema.schemas import drug_schema, pubmed_schema, clinical_trial_schema
from pyspark.sql.functions import *

class DataParser():
    '''
    This class is responsible for parsing csv and json files using spark
    '''
    def parse_csv(self, filepath, header, schema, separator=","):
        '''
        parses a csv file and returns a dataframe with a schema
        :param filepath: path to the csv file
        :param header: has header or not
        :param schema: the schema of the data to parse
        :param separator: the separator used in the csv file
        :return: a dataframe containing the csv rows
        '''
        return spark.read.schema(schema)\
            .format("csv")\
            .option("sep", separator)\
            .option("header", str(header).lower())\
            .load(filepath)

    def parse_json(self, filepath, schema):
        '''
        parses a json file and returns a dataframe with a schema
        :param filepath: path to the json file
        :param schema: the schema of the data to parse
        :return: a dataframe containing the json data
        '''
        return spark.read.option("multiline","true").schema(schema).json(filepath)

data_parser = DataParser()

class DrugParser():
    '''
    class responsible for parsing drug data
    '''
    def parse_drugs_csv(self, filepath):
        '''
        parses the drugs from a csv file
        :param filepath: path to the csv file
        :return: dataframe containing the drug data
        '''
        return data_parser.parse_csv(filepath, True, drug_schema, ",")

class ClinicalTrialParser():
    '''
    class responsible for parsing clinical trial data
    '''
    def parse_clinical_trials_csv(self, filepath):
        '''
        parses the clinical trial data from a csv file
        :param filepath: path to the csv file
        :return: dataframe containing the clinical trial data
        '''
        return data_parser.parse_csv(filepath, True, clinical_trial_schema, ",")

class PubmedParser():
    '''
    class responsible for parsing pubmed data
    '''
    def parse_pubmed_csv(self, filepath):
        '''
        parses the pubmed data from a csv file
        :param filepath: path to the csv file
        :return: dataframe containing the pubmed data
        '''
        return data_parser.parse_csv(filepath, True, pubmed_schema, ",")

    def parse_pubmed_json(self, filepath):
        '''
        parses the pubmed data from a json file
        :param filepath: path to the json file
        :return: dataframe containing the pubmed data
        '''
        return data_parser.parse_json(filepath, pubmed_schema)

drug_parser = DrugParser()
clinical_trial_parser = ClinicalTrialParser()
pubmed_parser = PubmedParser()
