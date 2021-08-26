from drug_analysis_pipeline.src.service.drug_publication_analyser import drug_pub_analyse_service
from drug_analysis_pipeline.src.parser.data_parsers import pubmed_parser, drug_parser, clinical_trial_parser

PUBMEDS_CSV = "drug_analysis_pipeline/test/resources/pubmed_1.csv"
PUBMEDS_JSON = "drug_analysis_pipeline/test/resources/pubmed_1.json"
DRUGS_CSV = "drug_analysis_pipeline/test/resources/drugs_1.csv"
CL_TRIALS_CSV = "drug_analysis_pipeline/test/resources/clinical_trials_1.csv"
GRAPH_JSON = "drug_analysis_pipeline/test/resources/graph.json"
pubmeds = pubmed_parser.parse_pubmed_csv(PUBMEDS_CSV).union(
    pubmed_parser.parse_pubmed_json(PUBMEDS_JSON)
)
clinical_trials = clinical_trial_parser.parse_clinical_trials_csv(CL_TRIALS_CSV)
drugs =  drug_parser.parse_drugs_csv(DRUGS_CSV)

def test_gen_drug_pub_graph_df():
    graph = drug_pub_analyse_service.gen_drug_pub_graph_df(drugs, pubmeds, clinical_trials)
    list  = graph.collect()
    assert (len(list) == 5)
    row = list[0]
    assert (row["drug"] == "D1")
    assert (len(row["journals"]) == 2)
    assert (len(row["clinical_trials"]) == 0)
    assert (len(row["pubmeds"]) == 2)
    row = list[3]
    assert (row["drug"] == "D4")
    assert (len(row["journals"]) == 2)
    assert (len(row["clinical_trials"]) == 1)
    assert (len(row["pubmeds"]) == 1)
    row = list[4]
    assert (row["drug"] == "D5")
    assert (len(row["journals"]) == 0)
    assert (len(row["clinical_trials"]) == 0)
    assert (len(row["pubmeds"]) == 0)

def test_find_top_journal():
    result = drug_pub_analyse_service.find_top_journal_from_file(GRAPH_JSON)
    assert (result['journal']=='journal_1')
    assert (result['mentioned_drugs']==3)
