from pyspark.sql.functions import *
from drug_analysis_pipeline.src.helper.spark import spark

# regular expression that helps us to remove the unwanted characters such \x3 etc. from the strings
REGEX = r'\\x[a-f0-9]{2}'

# dataframe columns
ATCCIDE_COL = "atccide"
ID_COL = "id"
DRUG_COL = "drug"
DATE_COL = "date"
SC_TITLE_COL = "scientific_title"
CLINICALS_TRIAL_COL = "clinical_trials"
JOURNAL_COL = "journal"
JOURNAL_INFO_COL = "journal_info"
TITLE_COL = "title"
PUBMEDS_COL = "pubmeds"

class DrugPublicationAnalyseService():
    def gen_drug_pub_graph_df(self, drugs, pubmeds, clinical_trials):
        return drugs.join(pubmeds, upper(pubmeds.title).contains(upper(drugs.drug)), "left") \
                    .withColumn(PUBMEDS_COL, \
                        when(pubmeds.title.isNull(), expr("null")) \
                        .otherwise(struct(trim(regexp_replace(pubmeds.title, REGEX,'')).alias(TITLE_COL), pubmeds.date))) \
                    .withColumn(JOURNAL_INFO_COL, \
                        when(pubmeds.journal.isNull(), expr("null")) \
                        .otherwise(struct(lower(trim(regexp_replace(pubmeds.journal, REGEX, ''))).alias(JOURNAL_COL), pubmeds.date))) \
                    .withColumn(CLINICALS_TRIAL_COL, expr("null")) \
                    .drop(ATCCIDE_COL, ID_COL, JOURNAL_COL, DATE_COL, TITLE_COL) \
            .union(
                drugs.join(clinical_trials, upper(clinical_trials.scientific_title).contains(upper(drugs.drug)), "left") \
                    .withColumn(PUBMEDS_COL, expr("null")) \
                    .withColumn(JOURNAL_INFO_COL, \
                        when(clinical_trials.journal.isNull(), expr("null")) \
                        .otherwise(struct(lower(trim(regexp_replace(clinical_trials.journal, REGEX, ''))).alias(JOURNAL_COL), clinical_trials.date))) \
                    .withColumn(CLINICALS_TRIAL_COL, \
                         when(col(SC_TITLE_COL).isNull(), expr("null")) \
                        .otherwise(struct(trim(regexp_replace(clinical_trials.scientific_title,REGEX,'')).alias(SC_TITLE_COL), clinical_trials.date))) \
                    .drop(ATCCIDE_COL, ID_COL, JOURNAL_COL, DATE_COL, SC_TITLE_COL)) \
            .groupBy(DRUG_COL)\
            .agg(collect_list(JOURNAL_INFO_COL).alias("journals"), \
               collect_list(PUBMEDS_COL).alias(PUBMEDS_COL), \
               collect_list(CLINICALS_TRIAL_COL).alias(CLINICALS_TRIAL_COL)) \
            .orderBy(DRUG_COL)

    def find_top_mentioning_journal(self, graph_path):
        graph = spark.read.json(graph_path)
        return self.get_top_journal_in_graph_df(graph)

    def get_top_journal_in_graph_df(self, graph_df):
        row = graph_df.select(col(DRUG_COL), explode(col("journals").journal).alias("journal")) \
            .dropDuplicates([JOURNAL_COL, DRUG_COL]) \
            .groupBy(col(JOURNAL_COL).alias(JOURNAL_COL)) \
            .agg(count("*").alias('drugs_count')) \
            .orderBy(desc('drugs_count'), JOURNAL_COL) \
            .limit(1) \
            .collect()[0]
        return {
            JOURNAL_COL : row[0],
            "mentioned_drugs": row[1]
        }

drug_pub_analyse_service = DrugPublicationAnalyseService()
