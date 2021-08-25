from drug_analysis_pipeline.src.parser.data_parsers import data_parser, drug_parser, pubmed_parser, clinical_trial_parser
from drug_analysis_pipeline.src.schema.schemas import drug_schema, pubmed_schema

DRUGS_CSV = "drug_analysis_pipeline/test/resources/drugs.csv"
PUBMED_JSON = "drug_analysis_pipeline/test/resources/pubmed.json"
PUBMED_CSV = "drug_analysis_pipeline/test/resources/pubmed.csv"
CLINICAL_TRIAL_CSV = "drug_analysis_pipeline/test/resources/clinical_trials.csv"

def test_data_parser_csv():
    result = data_parser.parse_csv(DRUGS_CSV, True, drug_schema).collect()
    assert (len(result) == 7)
    assert (result[0][0] == 'A04AD')
    assert (result[6][1] == 'BETAMETHASONE')
    result = data_parser.parse_csv(DRUGS_CSV, False, drug_schema).collect()
    assert (len(result) == 8)

def test_data_parser_json():
    result = data_parser.parse_json(PUBMED_JSON, pubmed_schema).collect()
    assert (len(result) == 5)
    assert (result[1][0] == '10')
    assert ("Effects of Topical Application" in result[2][1])
    assert (result[3][2] == '01/03/2020')
    assert (result[4][3] == 'The journal of maternal-fetal & neonatal medicine')

def test_drug_parser():
    result = drug_parser.parse_drugs_csv(DRUGS_CSV).collect()
    assert (len(result) == 7)
    assert (result[0][0] == 'A04AD')
    assert (result[6][1] == 'BETAMETHASONE')

def test_pubmed_parser():
    result = pubmed_parser.parse_pubmed_csv(PUBMED_CSV).collect()
    assert (len(result) == 8)
    assert (result[0][0] == '1')
    assert ("An evaluation of benadryl" in result[1][1])
    assert (result[2][2] == '02/01/2019')
    assert (result[3][3] == 'Journal of food protection')
    result = pubmed_parser.parse_pubmed_json(PUBMED_JSON).collect()
    assert (len(result) == 5)
    assert (result[4][0] == '')
    assert (result[4][2] == '01/03/2020')

def test_clinical_trial_parser():
    result = clinical_trial_parser.parse_clinical_trials_csv(CLINICAL_TRIAL_CSV).collect()
    assert (len(result) == 8)
    assert (result[0][0] == 'NCT01967433')
    assert ("Phase 2 Study IV QUZYTTIR™" in result[1][1])
    assert (result[2][2] == '1 January 2020')
    assert (result[6][3] == 'Journal of emergency nursing')
