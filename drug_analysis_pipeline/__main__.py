from drug_analysis_pipeline.src.parser.data_parsers import *
from pyspark.sql.functions import *
from drug_analysis_pipeline.src.service.drug_publication_analyser import drug_pub_analyse_service
import sys
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':

    try:
        drugs_csv_path = sys.argv[1]
        pubmed_csv_path = sys.argv[2]
        pubmed_json_path = sys.argv[3]
        clinical_trial_csv_path = [sys.argv[4]]
        result_dir = sys.argv[5]
    except:
        logging.error("Missing required arguments. The required arguments are:\n"
                      "arg-1: drugs csv file\n"
                      "arg-2: pubmed csv file\n"
                      "arg-3: pubmed json file\n"
                      "arg-4: clinical trial csv file\n"
                      "arg-5: output directory\n")
        raise Exception("Missing required argument!")

    drugs = drug_parser.parse_drugs_csv(drugs_csv_path)
    pubmeds = pubmed_parser.parse_pubmed_csv(pubmed_csv_path).union(
                pubmed_parser.parse_pubmed_json(pubmed_json_path))
    clinical_trials = clinical_trial_parser.parse_clinical_trials_csv(clinical_trial_csv_path)

    drug_pub_analyse_service.gen_drug_pub_graph_df(drugs, pubmeds, clinical_trials) \
        .coalesce(1) \
        .write \
        .format('json') \
        .mode('overwrite') \
        .save(result_dir)

    logging.info(f"The resualt has been written to {result_dir}")

    result = drug_pub_analyse_service.find_top_journal_from_file(f'{result_dir}/*.json')

    logging.info(f"The top journal is : {result}")



