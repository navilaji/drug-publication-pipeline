from drug_analysis_pipeline.src.service.drug_publication_analyser import drug_pub_analyse_service
from drug_analysis_pipeline.src.parser.data_parsers import pubmed_parser, drug_parser, clinical_trial_parser

PUBMEDS_CSV = "drug_analysis_pipeline/test/resources/pubmed_1.csv"
DRUGS_CSV = "drug_analysis_pipeline/test/resources/drug_1.csv"
CL_TRIALS_CSV = "drug_analysis_pipeline/test/resources/clinical_trials_1.csv"

pubmeds = pubmed_parser.parse_pubmed_csv(PUBMEDS_CSV)
clinical_trials = clinical_trial_parser.parse_clinical_trials_csv(CL_TRIALS_CSV)
drugs =  drug_parser.parse_drugs_csv(DRUGS_CSV)

def test_gen_drug_pub_graph_df():

def find_top_mentioning_journal():

