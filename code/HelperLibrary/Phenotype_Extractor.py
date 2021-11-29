# Databricks notebook source
# Phenotype extractor class
# Project: CCU003_03
# By Mehrdad Mizani
# Based on the functional code by CALIBER team
# New phenotypes 
# a. Asthma definition by Jennifer Quint
# b. CKD definition by Ashkan Dashtban
# c. Diabetes with added ICD-10 codes from CALIBER
# d. Prescribed corticosteroid based on CALIBER mapped to SNOMED by Mehrdad Mizani
# e. Dispensed corticosteroid based on CALIBER mapped to BNF by Ashkan Dashtban


from functools import reduce
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.types import DateType


class PhenoExtractor:
    """Extracts phenotype from mastercodelist, GDPPR,  HES APC, and Primacy Care Medication tables
    
    Todo: 
        - Refine the PEP style 
        - There are still records (at the scale of 1000s) with null eventdate. Write a method to remove this if the user needs only non-null eventdates.
        - Add other terminologies (e.g., CVD3)
        - (external to TRE) APIs for automatic fetching of codelists from CALIBER, Open Safely, and other codelist providers.
    """

    def __init__(self, mastercodelist, gdppr, hesapc, analysisdf, pcm=spark.createDataFrame([], schema=StructType())):
        """Constructor. The attributes are documented at getter methods with the ``@property`` decorator"""
        self.__mastercodelist = mastercodelist
        self.__gdppr = gdppr
        self.__hesapc = hesapc
        self.__pcm = pcm
        self.__analysisdf= analysisdf
        self.__cohort_ids = self.__analysisdf.select('NHS_NUMBER_DEID')


    @property
    def mastercodelist(self):
        """A dataframe containing the name of the condition, terminology (e.g. ICD-10, Snomed), code, and other columns.
        At this stage, the codelist at bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127 is used.

        Todo: Change this to YAML
        """
        return self.__mastercodelist

    @mastercodelist.setter
    def mastercodelist(self, mastercodelist):
        self.__mastercodelist=mastercodelist

    @property
    def gdppr(self):
        """GDPPR dataframe
        """
        return self.__gdppr

    @gdppr.setter
    def gdppr(self, gdppr):
        self.__gdppr=gdppr
        #self.__gdppr=gdppr.filter(F.col('DATE').isNotNull())

    @property
    def hesapc(self):
        """A dataframe containing the unio of HES APC tables from all financial years.
        """
        return self.__hesapc

    @hesapc.setter
    def hesapc(self, hesapc):
        self.__hesapc=hesapc

    @property
    def pcm(self):
        """Privacy Care Medication dataset
        """
        return self.__pcm

    @pcm.setter
    def pcm(self, pcm):
        self.__pcm = pcm

    @property
    def analysisdf(self):
        """The dataframe containing one row per patient with a column for patient ID (e.g. skinny table, key table)
        """
        return self.__analysisdf

    @analysisdf.setter
    def analysisdf(self, analysisdf):
        self.__analysisdf=analysisdf

    def add_codes_to_masterlist(self, vals_list_of_tuples, mastercodelist_cols=['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']):
        """ Adds missing codes to the mastercodelist

        Args:
            vals_list_of_tuples(list): List of tuples each with elements represeting name, terminology, code, term, code type, and record date.
            For exmple ('diabetes', 'ICD10', 'E10', 'Insulin-dependent diabetes mellitus', '1', '20210127')

            mastercodelist_cols(list): A list showing the column names of the mastercodelist. For now, we only use one mastercodelist and the columns are the same througout.

        """

        new_codes = spark.createDataFrame(vals_list_of_tuples, mastercodelist_cols)
        self.__mastercodelist=self.__mastercodelist.union(new_codes)

    def unique_codetype_givendisease(self, disease_start):
        """ Prints distinct terminologies (e.g. ICD, SNOMED) for a string starting with the input variable

        Args:
            disease_start(str): The starting part of the disease name.
        """
        print(self.__master_codelist.filter((self.__master_codelist.name.startswith(disease_start)) ).select("terminology").distinct().show())

    def get_codelist_from_master(self, disease_start, code_name):
        """ Returns the codes for ICD10 or SNOMED for a particular disease

        Args:
            disease_start(str): The starting part of the disease name.

            code_name(str): A string indicating if the terminology is ICD10, SNOMED, or BNF_chemical

        Returns:
            Pyspark dataframe: Dataframe containing the codes.
        """
        df = self.__mastercodelist.withColumn('code', F.regexp_replace("code", '[.]', ''))
        df = df.filter((df.name.startswith(disease_start)) & (df.terminology.startswith(code_name)) ).select("code")
        return df

    def list_medcodes(self, codelist_column_df):
        """Extracts all the codes (e.g. ICD-10, SNOMED) for a certian condition
        """
        codelist = [item.code for item in codelist_column_df.select('code').collect()]
        return codelist

    def snomed_from_gdppr(self, patid, condition, codelist):
        """Returns the event date of the earliest record in GDPPR data for the specified condition codded in SNOMED in the codelist
        """
        gdppr_tmp = self.__gdppr.withColumnRenamed('NHS_NUMBER_DEID', patid).select([patid, "DATE", "RECORD_DATE", "CODE"])
        df_out = gdppr_tmp.withColumn(condition, F.when(F.col("CODE").cast('string').isin(codelist), F.lit(1)).otherwise(F.lit(0))
        temp_col ='dup_id'
        window_spec = Window.partitionBy(df_out[patid]).orderBy(F.col(condition).desc(), F.col('DATE').asc_nulls_last())
        df_out2 = df_out.withColumn(temp_col, F.row_number().over(window_spec))
        df_out2 = df_out2.filter(F.col(temp_col)==1)
        # to make date null for pheno=0
        df_out2 = df_out2.withColumn('DATE', F.when(F.col(condition)==1, F.col('DATE')))
        df_out2 = df_out2.withColumnRenamed('DATE', f'''event_date_{condition}''').drop(temp_col).drop("RECORD_DATE").drop("CODE")
        return df_out2

    def icd_from_hes(self, patid, condition, codelist):
        """Returns the event date of the earliest record in HES APC data for the specified condition codded in ICD-10 in the codelist
        """
        hes_tmp = self.__hesapc.withColumnRenamed('PERSON_ID_DEID', patid).select([patid, "EPISTART", "DIAG_4_CONCAT"])
        hes_tmp = hes_tmp.withColumn("DIAG_4_CONCAT",F.regexp_replace(F.col("DIAG_4_CONCAT"), "\\.", ""))
        if codelist!=[]:
            df_out = hes_tmp.where(reduce(lambda a, b: a|b, (F.col('DIAG_4_CONCAT').like('%'+code+"%") for code in codelist))).select([patid, "EPISTART"]).withColumn(condition, F.lit(1))
        else:
            """If there is no ICD-10 codes available"""
            schema=StructType([
            StructField(patid, StringType(), True),
            StructField("EPISTART", DateType(), True),
            StructField(condition, StringType(), True)])
            df_out=spark.createDataFrame([], schema)
        temp_col ='dup_id'
        # Earliest non-null record
        window_spec = Window.partitionBy(df_out[patid]).orderBy(F.col(condition).desc(), F.col('EPISTART').asc_nulls_last())
        df_out2 = df_out.withColumn(temp_col, F.row_number().over(window_spec))
        df_out2 = df_out2.filter(F.col(temp_col)==1)
        # to make date null for pheno=0
        df_out2 = df_out2.withColumn('EPISTART', F.when(F.col(condition)==1, F.col('EPISTART')))
        df_out2 = df_out2.withColumnRenamed('EPISTART', f'''event_date_{condition}''').drop(temp_col)
        return df_out2

    def dispensed_bnf_chemical_from_pcm_simple(self, patid, medication_chemical_level, codelist):
        """Returns the event date of the earliest record in Primary Care Medicaion data for the specified chemical level medicaiton
        codded in BNF codelist

        Note: this method only takes BNF at chemical level in the format of AABBCCDEE
        """
        pcm_tmp = self.__pcm.withColumnRenamed('Person_ID_DEID', patid).select([patid, "ProcessingPeriodDate", "PaidBNFCode"])
        # substring funciton index starts at 1 not zero
        pcm_tmp = pcm_tmp.withColumn("BNF_chemical",F.when(((F.col("PaidBNFCode").isNotNull())&(F.length(F.col("PaidBNFCode"))>=9)), F.substring(F.col("PaidBNFCode"), 1, 9)).otherwise(F.col("PaidBNFCode")))
        if codelist!=[]:
            df_out = pcm_tmp.withColumn(medication_chemical_level, F.when(F.col("BNF_chemical").cast('string').isin(codelist), F.lit(1)).otherwise(F.lit(0)))
        else:
            """If there is no BNF_chemical codes available"""
            schema=StructType([
            StructField(patid, StringType(), True),
            StructField("ProcessingPeriodDate", DateType(), True),
            StructField(medication_chemical_level, StringType(), True)])
            df_out=spark.createDataFrame([], schema)
        temp_col ='dup_id'
        # Earliest non-null record
        window_spec = Window.partitionBy(df_out[patid]).orderBy(F.col(medication_chemical_level).desc(), F.col('ProcessingPeriodDate').asc_nulls_last())
        df_out2 = df_out.withColumn(temp_col, F.row_number().over(window_spec))
        df_out2 = df_out2.filter(F.col(temp_col)==1)
        # to make date null for pheno=0
        df_out2 = df_out2.withColumn('ProcessingPeriodDate', F.when(F.col(medication_chemical_level)==1, F.col('ProcessingPeriodDate')))
        df_out2 = df_out2.withColumnRenamed('ProcessingPeriodDate', f'''event_date_{medication_chemical_level}''').drop(temp_col)
        return df_out2

    def prescription_snomed_from_gdppr_simple(self, patid, medication, codelist):
        """Finds the earliest eventdate for a medication, coded in snomed, prescribed in GDPPR
        """
        gdppr_tmp = self.__gdppr.withColumnRenamed('NHS_NUMBER_DEID', patid).select([patid, "DATE", "RECORD_DATE", "CODE"])
        df_out = gdppr_tmp.withColumn(medication, F.when(F.col("CODE").cast('string').isin(codelist), F.lit(1)).otherwise(F.lit(0)))
        temp_col ='dup_id'
        window_spec = Window.partitionBy(df_out[patid]).orderBy(F.col(medication).desc(), F.col('DATE').asc_nulls_last())
        df_out2 = df_out.withColumn(temp_col, F.row_number().over(window_spec))
        df_out2 = df_out2.filter(F.col(temp_col)==1)
        # to make date null for pheno=0
        df_out2 = df_out2.withColumn('DATE', F.when(F.col(medication)==1, F.col('DATE')))
        df_out2 = df_out2.withColumnRenamed('DATE', f'''event_date_{medication}''').drop(temp_col).drop("RECORD_DATE").drop("CODE")
        return df_out2

    def prescription_snomed_from_gdppr_final_date_only(self, patid, medication, codelist, ref_date_start, before_start_period_in_days, ref_date_end):
        """Finds the latest eventdate for a medication, coded in snomed, prescribed in GDPPR.
        It also checks a minimum period of time before the eventdate
        """
        gdppr_tmp = self.__gdppr.withColumnRenamed('NHS_NUMBER_DEID', patid).select([patid, "DATE", "RECORD_DATE", "CODE", "EPISODE_PRESCRIPTION"])
        df_out = gdppr_tmp.withColumn(medication, F.when(F.col("CODE").cast('string').isin(codelist), F.lit(1)).otherwise(F.lit(0)))
        temp_col ='dup_id'
        window_spec = Window.partitionBy(df_out[patid]).orderBy(F.col(medication).desc(), F.col('DATE').desc())
        df_out2 = df_out.withColumn(temp_col, F.row_number().over(window_spec))
        df_out2 = df_out2.filter(F.col(temp_col)==1)
        # keep only those with DATE within period_in_days before ref_date
        dt_int_start = list(map(int, ref_date_start.split("-")))
        dt_int_end = list(map(int, ref_date_end.split("-")))
        df_out2 = df_out2.withColumn("ref_date_start",  F.lit(dt.date(dt_int_start[0], dt_int_start[1], dt_int_start[2])))
        df_out2 = df_out2.withColumn("ref_date_end",  F.lit(dt.date(dt_int_end[0], dt_int_end[1], dt_int_end[2])))
        df_out2 = df_out2.withColumn("day_diff_before", F.datediff(F.col('ref_date_start'), F.col("DATE")))
        df_out2 = df_out2.withColumn(medication, F.when(((F.col(medication)==1)&(F.col("DATE").isNotNull())&(F.col("day_diff_before") > before_start_period_in_days)), F.lit(0)).otherwise(F.col(medication)))
        df_out2 = df_out2.withColumn(medication, F.when(((F.col(medication)==1)&(F.col("DATE").isNotNull())&(F.col("DATE") > F.col("ref_date_end"))), F.lit(0)).otherwise(F.col(medication)))
        # to make date null for pheno=0
        df_out2 = df_out2.withColumn('DATE', F.when(F.col(medication)==1, F.col('DATE')))
        df_out2 = df_out2.withColumnRenamed('DATE', f'''event_date_{medication}''').drop(temp_col).drop("RECORD_DATE").drop("CODE").drop("ref_date_start").drop("day_diff_before").drop("ref_date_end")
        return df_out2

    def prescription_snomed_from_gdppr_first_date_only(self, patid, medication, codelist, ref_date_start, before_start_period_in_days, ref_date_end):
        """Finds the earliest eventdate for a medication, coded in snomed, prescribed in GDPPR.
        It also checks a minimum period of time before the eventdate.
        """
        gdppr_tmp = self.__gdppr.withColumnRenamed('NHS_NUMBER_DEID', patid).select([patid, "DATE", "RECORD_DATE", "CODE", "EPISODE_PRESCRIPTION"])
        df_out = gdppr_tmp.withColumn(medication, F.when(F.col("CODE").cast('string').isin(codelist), F.lit(1)).otherwise(F.lit(0)))
        temp_col ='dup_id'
        window_spec = Window.partitionBy(df_out[patid]).orderBy(F.col(medication).desc(), F.col('DATE').asc_nulls_last())
        df_out2 = df_out.withColumn(temp_col, F.row_number().over(window_spec))
        df_out2 = df_out2.filter(F.col(temp_col)==1)
        # keep only those with DATE within period_in_days before ref_date
        dt_int_start = list(map(int, ref_date_start.split("-")))
        dt_int_end = list(map(int, ref_date_end.split("-")))
        df_out2 = df_out2.withColumn("ref_date_start",  F.lit(dt.date(dt_int_start[0], dt_int_start[1], dt_int_start[2])))
        df_out2 = df_out2.withColumn("ref_date_end",  F.lit(dt.date(dt_int_end[0], dt_int_end[1], dt_int_end[2])))
        df_out2 = df_out2.withColumn("day_diff_before", F.datediff(F.col('ref_date_start'), F.col("DATE")))
        df_out2 = df_out2.withColumn(medication, F.when(((F.col(medication)==1)&(F.col("DATE").isNotNull())&(F.col("day_diff_before") > before_start_period_in_days)), F.lit(0)).otherwise(F.col(medication)))
        df_out2 = df_out2.withColumn(medication, F.when(((F.col(medication)==1)&(F.col("DATE").isNotNull())&(F.col("DATE") > F.col("ref_date_end"))), F.lit(0)).otherwise(F.col(medication)))
        # to make date null for pheno=0
        df_out2 = df_out2.withColumn('DATE', F.when(F.col(medication)==1, F.col('DATE')))
        df_out2 = df_out2.withColumnRenamed('DATE', f'''event_date_{medication}''').drop(temp_col).drop("RECORD_DATE").drop("CODE").drop("ref_date_start").drop("day_diff_before").drop("ref_date_end")
        return df_out2

    def prescription_snomed_from_gdppr_first_n_last_date(self, patid, medication, codelist, ref_date_start, ref_date_end, before_start_period_in_days, max_years_before_start):
        """Finds the first eventdate for a medication, coded in snomed, prescribed in GDPPR.
        It also checks a minimum and maximum period of time before the eventdate
        """
        # this is too complilcated.
        gdppr_tmp = self.__gdppr.withColumnRenamed('NHS_NUMBER_DEID', patid).select([patid, "DATE",  "CODE"])
        df_out_all = gdppr_tmp.withColumn(medication, F.when(F.col("CODE").cast('string').isin(codelist), F.lit(1)).otherwise(F.lit(0)))
        # will join later to reian zeros
        df_out = df_out_all.filter(F.col(medication)==1)
        df_out = df_out.withColumn("DATE_last", F.col('DATE')).withColumn("DATE_first", F.col('DATE'))
        temp_col_last ='temp_last'
        window_spec_last = Window.partitionBy(df_out[patid]).orderBy(F.col(medication).desc(), F.col('DATE_last').desc())
        temp_col_first = 'temp_first'
        window_spec_first = Window.partitionBy(df_out[patid]).orderBy(F.col(medication).desc(), F.col('DATE_first').asc_nulls_last())
        df_out2 = df_out.withColumn(temp_col_last, F.row_number().over(window_spec_last))
        df_out2 = df_out2.filter(F.col(temp_col_last)==1)
        df_out2 = df_out2.drop("DATE").drop("DATE_first").drop("CODE").drop(temp_col_last)
        df_out3 = df_out.withColumn(temp_col_first, F.row_number().over(window_spec_first))
        df_out3 = df_out3.filter(F.col(temp_col_first)==1)
        df_out3 = df_out3.drop("DATE").drop("DATE_last").drop("CODE").drop(temp_col_first)
        df_out4 = df_out2.join(df_out3, on=['NHS_NUMBER_DEID', medication], how="outer")
        dt_int_start = list(map(int, ref_date_start.split("-")))
        dt_int_end = list(map(int, ref_date_end.split("-")))
        df_out4 = df_out4.withColumn("ref_date_a",  F.lit(dt.date(dt_int_start[0], dt_int_start[1], dt_int_start[2])))
        df_out4 = df_out4.withColumn("ref_date_b",  F.lit(dt.date(dt_int_end[0], dt_int_end[1], dt_int_end[2])))
        df_out4 = df_out4.withColumn("diff_a_last", F.datediff(F.col('ref_date_a'), F.col("DATE_last")))
        return df_out4

    def prescription_snomed_from_gdppr_last_two_dates(self, patid, medication, codelist, ref_date_start, ref_date_end, before_start_period_in_days=365, after_start_period_in_days= 181):
        """Finds the last two latest eventdates for a medication, coded in snomed, prescribed in GDPPR.
        It also checks a minimum period of time before the eventdate
        """
        gdppr_tmp = self.__gdppr.withColumnRenamed('NHS_NUMBER_DEID', patid).select([patid, "DATE", "RECORD_DATE", "CODE", "EPISODE_PRESCRIPTION"])
        df_out = gdppr_tmp.withColumn(medication, F.when(F.col("CODE").cast('string').isin(codelist), F.lit(1)).otherwise(F.lit(0)))
        temp_1 ='temp_col'
        temp_2 = "second_date"
        window_spec = Window.partitionBy(df_out[patid]).orderBy(F.col(medication).desc(), F.col('DATE').desc())
        df_out2 = df_out.withColumn(temp_1, F.row_number().over(window_spec))
        df_out2 = df_ou2.withColumn(temp_2,  F.lead('DATE').over(window_spec))
        df_out2 = df_out2.filter(F.col(temp_1)==1)
        dt_int_start = list(map(int, ref_date_start.split("-")))
        dt_int_end = list(map(int, ref_date_end.split("-")))
        df_out2 = df_out2.withColumn("ref_date_start",  F.lit(dt.date(dt_int_start[0], dt_int_start[1], dt_int_start[2])))
        df_out2 = df_out2.withColumn("ref_date_end",  F.lit(dt.date(dt_int_end[0], dt_int_end[1], dt_int_end[2])))
        df_out2 = df_out2.withColumn("diff_refstart_1", F.datediff(F.col('ref_date_start'), F.col("DATE")))
        df_out2 = df_out2.withColumn("diff_refstart_2", F.datediff(F.col('ref_date_start'), F.col(temp_2)))
        # simply if last date or second date in 1 year prior to ref_start, then 1
        df_out2 = df_out2.withColumn(medication, F.when(((F.col(medication)==1)& \
                                                         (((F.col("DATE").isNotNull())&(F.col("diff_refstart_1") > before_start_period_in_days)) | \
                                                         ((F.col(temp_2).isNotNull())&(F.col("diff_refstart_2") > before_start_period_in_days)))), \
                                                        F.lit(0)).otherwise(F.col(medication)))
        # if last date or second date in the first 6 months, then 1
        # in this case, the diff will be between 0 and -181
        df_out2 = df_out2.withColumn(medication, F.when(((F.col(medication)==1)& \
                                                         (((F.col("DATE").isNotNull())&(F.col("diff_refstart_1") > ((-1)*int(after_start_period_in_days)))) | \
                                                         ((F.col(temp_2).isNotNull())&(F.col("diff_refstart_2") > ((-1)*int(after_start_period_in_days)))))), \
                                                        F.lit(0)).otherwise(F.col(medication)))
        # if larger than end date then zero
        # implement this
        df_out2 = df_out2.withColumn(medication, F.when(((F.col(medication)==1)&(F.col("DATE").isNotNull())&(F.col("day_diff_before") > before_start_period_in_days)), F.lit(0)).otherwise(F.col(medication)))
        df_out2 = df_out2.withColumn(medication, F.when(((F.col(medication)==1)&(F.col("DATE").isNotNull())&(F.col("DATE") > F.col("ref_date_end"))), F.lit(0)).otherwise(F.col(medication)))
        # to make date null for pheno=0
        df_out2 = df_out2.withColumn('DATE', F.when(F.col(medication)==1, F.col('DATE')))
        # add drops
        df_out2 = df_out2.withColumnRenamed('DATE', f'''event_date_{medication}''').drop("RECORD_DATE").drop("CODE").drop("ref_date_start").drop("day_diff_before").drop("ref_date_end")
        return df_out2

    def join_gdppr_hes_phenotypes(self, gdppr_pheno, hes_pheno, condition_gdppr, condition_hes):
        """Joins the resutls of SNOMED phenotypes and ICD-10 phenotypes from GDPPR and HES APC respectively.
        """
        df = gdppr_pheno.join(hes_pheno, on=['NHS_NUMBER_DEID'], how="outer")
        return df

    def add_phenotype_to_analysis_table(self,pheno_table, condition):
        """Adds extracted phnenotype to the analysis dataframe
        """
        self.__annalysisdf = self.__analysisdf.join(pheno_table, 'NHS_NUMBER_DEID', "left").select("*")
        self.__annalysisdf = self.__annalysisdf.withColumn(condition, F.when((f.col(condition).isNotNull()), F.lit(1)).otherwise(F.lit(0)))

    def extract_raw_phenotype(self, condition):
        """Extracts raw diagnostic phenoytpes, which is a combination of eventdates of conditons coded with different codes
        """
        icd10_codelist = self.get_codelist_from_master(condition, "ICD10")
        snomed_codelist = self.get_codelist_from_master(condition, "SNOMED")
        icd10_codelist = self.list_medcodes(icd10_codelist)
        snomed_codelist = self.list_medcodes(snomed_codelist)
        gdppr_pheno_tag = "gdppr_"+condition
        gdppr_pheno = self.snomed_from_gdppr('NHS_NUMBER_DEID',  gdppr_pheno_tag, snomed_codelist)
        hes_pheno_tag = "hes_"+condition
        hes_pheno = self.icd_from_hes('NHS_NUMBER_DEID', hes_pheno_tag, icd10_codelist)
        raw_pheno = self.join_gdppr_hes_phenotypes(gdppr_pheno,hes_pheno,gdppr_pheno_tag,hes_pheno_tag)
        return raw_pheno

    def extract_raw_prescription_phenotype(self, medication, ref_date_start, before_start_period_in_days, ref_date_end):
        """Extracts raw  prescribed therapeutic phenoytpes, which is a combination of eventdates of medications coded with SNOMED code
        """
        snomed_codelist = self.get_codelist_from_master(medication, "SNOMED")
        snomed_codelist = self.list_medcodes(snomed_codelist)
        gdppr_pheno_tag = "gdppr_"+medication
        gdppr_pheno = self.prescription_snomed_from_gdppr_simple('NHS_NUMBER_DEID',  gdppr_pheno_tag, snomed_codelist)
        return gdppr_pheno

    def extract_raw_dispensed_phenotype(self, medication, ref_date_start, before_start_period_in_days, ref_date_end):
        """Extracts raw dispensed therapeutic phenoytpes, which is a combination of eventdates of medications coded with BNF at chemical level
        """
        bnf_chemical_codelist = self.get_codelist_from_master(medication, "BNF_chemical")
        bnf_chemical_codelist = self.list_medcodes(bnf_chemical_codelist)
        # Primary care medication , dispensed data
        pcm_pheno_tag = "pcm_"+medication
        pcm_pheno = self.dispensed_bnf_chemical_from_pcm_simple('NHS_NUMBER_DEID', pcm_pheno_tag, bnf_chemical_codelist)
        return pcm_pheno

    def extract_binary_phenotype(self, condition):
        """Makes a single flag indicating the existance of the condition
        by combining ICD-10 and SNOMED resutls from HES APC and GDPPR respectively
        """
        raw_pheno = self.extract_raw_phenotype(condition)
        print(f'''{condition} inside Class Method''')
        binary_pheno=raw_pheno.withColumn(f'''gdppr_{condition}''', F.when(F.col(f'''gdppr_{condition}''')==1, F.lit(1)).otherwise(F.lit(0))). \
        withColumn(f'''hes_{condition}''', F.when(F.col(f'''hes_{condition}''')==1, F.lit(1)).otherwise(F.lit(0)))
        binary_pheno = binary_pheno. \
        withColumn(f'''is_{condition}''', \
                   F.when((F.col(f'''gdppr_{condition}''')==1)| \
                          (F.col(f'''hes_{condition}''')==1), F.lit(1)).otherwise(F.lit(0))). \
        withColumn(f'''eventdate_{condition}''', F.lit(0)). \
        withColumn(f'''eventdate_{condition}''', \
                   F.when((F.col(f'''gdppr_{condition}''')==1)& \
                          (F.col(f'''hes_{condition}''')==0), F.col(f'''event_date_gdppr_{condition}''') )). \
        withColumn(f'''eventdate_{condition}''', \
                   F.when((F.col(f'''gdppr_{condition}''')==0)& \
                          (F.col(f'''hes_{condition}''')==1), F.col(f'''event_date_hes_{condition}''') ). \
                   otherwise(F.col(f'''eventdate_{condition}'''))). \
        withColumn(f'''eventdate_{condition}''', \
                   F.when(((F.col(f'''gdppr_{condition}''')==1)&(F.col(f'''hes_{condition}''')==1))& \
                          (F.col(f'''event_date_gdppr_{condition}''')>F.col(f'''event_date_hes_{condition}''')), \
                          F.col(f'''event_date_hes_{condition}''') ).otherwise(F.col(f'''eventdate_{condition}'''))). \
        withColumn(f'''eventdate_{condition}''', \
                   F.when(((F.col(f'''gdppr_{condition}''')==1)&(F.col(f'''hes_{condition}''')==1))& \
                          (F.col(f'''event_date_hes_{condition}''')>=F.col(f'''event_date_gdppr_{condition}''')), \
                          F.col(f'''event_date_gdppr_{condition}''') ).otherwise(F.col(f'''eventdate_{condition}'''))). \
        drop(f'''gdppr_{condition}''').drop(f'''hes_{condition}'''). \
        drop(f'''event_date_gdppr_{condition}''').drop(f'''event_date_hes_{condition}''')
        return binary_pheno

    def extract_positive_only(self, condition):
        """Returns rows with phenotype flag set to 1
        """
        binary_pheno = self.extract_binary_phenotype(condition)
        pos_pheno = binary_pheno.filter(F.col(f'''is_{condition}''') == 1)
        return pos_pheno

    def extract_binary_prescription_phenotype(self, medication, ref_date_start, before_start_period_in_days, ref_date_end, period_str=""):
        """Extracts a binary prescribed medication phenotype from primary care data (e.g. prescribed vs not-prescribed).
        """
        raw_pheno = self.extract_raw_prescription_phenotype(medication, ref_date_start, before_start_period_in_days, ref_date_end)
        print(f'''{medication} inside Class Method''')
        binary_pheno = raw_pheno.withColumnRenamed(f'''gdppr_{medication}''', f'''is_prescribed_{medication}_{period_str}'''). \
        withColumnRenamed(f'''event_date_gdppr_{medication}''', f'''eventdate_{medication}_{period_str}'''). \
        drop("EPISODE_PRESCRIPTION")
        return binary_pheno
        #return raw_pheno

    def extract_positive_only_prescription(self, medication, ref_date_start, before_start_period_in_days, ref_date_end, period_str=""):
        """Extraxts prescribed medication phenotype from primary care data
        """
        binary_pheno = self.extract_binary_prescription_phenotype(medication, ref_date_start, before_start_period_in_days, ref_date_end, period_str="")
        pos_pheno = binary_pheno.filter(F.col(f'''is_{medication}_{period_str}''') == 1)
        return pos_pheno


    def extract_binary_dispensed_phenotype(self, medication, ref_date_start, before_start_period_in_days, ref_date_end, period_str=""):
        """Extracts a binary dispensed medication phenotype from BSA (e.g. dispensed vs not-dispensed).
        """
        raw_pheno = self.extract_raw_dispensed_phenotype(medication, ref_date_start, before_start_period_in_days, ref_date_end)
        print(f'''{medication} inside Class Method''')
        binary_pheno = raw_pheno.withColumnRenamed(f'''pcm_{medication}''', f'''is_dispensed_{medication}_{period_str}'''). \
        withColumnRenamed(f'''event_date_pcm_{medication}''', f'''eventdate_{medication}_{period_str}''').drop("PaidBNFCode").drop("BNF_chemical")
        return binary_pheno
        #return raw_pheno

    def extract_positive_only_prescription(self, medication, ref_date_start, before_start_period_in_days, ref_date_end, period_str=""):
        """Extraxts dispensed medication from BSA
        """
        binary_pheno = self.extract_binary_prescription_phenotype(medication, ref_date_start, before_start_period_in_days, ref_date_end, period_str="")
        pos_pheno = binary_pheno.filter(F.col(f'''is_{medication}_{period_str}''') == 1)
        return pos_pheno

    def make_multi_phenotypes(self, pheno_list):
        """Joins several extracted phenotypes.
        """
        cohort_ids_loop = self.__cohort_ids
        for pheno in pheno_list:
            print(pheno)
            pheno_j = self.make_single_phenotype(pheno)
            cohort_ids_loop = cohort_ids_loop.join(pheno_j, on=['NHS_NUMBER_DEID'], how='left')
        cohort_ids_filled= cohort_ids_loop.fillna(0)
        return(cohort_ids_filled)

    def get_whole_codelist_per_pheno_string(self, pheno_string):
        """Returns the codelists for a specified phenotype sting.
        """
        df = self.__mastercodelist
        df = df.filter((df.name == pheno_string))
        return df

    def print_phenotype_codelist(self, pheno_string, pheno_kind= "condition"):
        """ Returns the codelist for a specified diagnostic (non-therapeutic)
        phenotype.
        """
        if pheno_kind == "condition":
            snomed_codelist = self.get_whole_codelist_per_pheno_string(pheno_string)
            return(snomed_codelist)


# update mastercodelist
def add_diabetes_codes(pheno_extractor):
    """Adds ICD10 codes from CALIBER for diabetes hes (dm_hes)
    """
    columns = ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']
    vals = [('diabetes', 'ICD10', 'E10', 'Insulin-dependent diabetes mellitus', '1', '20210127'),
            ('diabetes', 'ICD10', 'E11', 'Non-insulin-dependent diabetes mellitus', '1','20210127'),
            ('diabetes', 'ICD10', 'E12', 'Malnutrition-related diabetes mellitus', '1','20210127'),
            ('diabetes', 'ICD10', 'O242', 'Diabetes mellitus in pregnancy: Pre-existing malnutrition-related diabetes mellitus', '1','20210127'),
            ('diabetes', 'ICD10', 'E13', 'Other specified diabetes mellitus', '1','20210127'),
            ('diabetes', 'ICD10', 'E14', 'Unspecified diabetes mellitus', '1','20210127'),
            ('diabetes', 'ICD10', 'G590', 'Diabetic mononeuropathy', '1','20210127'),
            ('diabetes', 'ICD10', 'G632', 'Diabetic polyneuropathy','1', '20210127'),
            ('diabetes', 'ICD10', 'H280', 'Diabetic cataract', '1','20210127'),
            ('diabetes', 'ICD10', 'H360', 'Diabetic retinopathy', '1','20210127'),
            ('diabetes', 'ICD10', 'M142', 'Diabetic athropathy', '1','20210127'),
            ('diabetes', 'ICD10', 'N083', 'Glomerular disorders in diabetes mellitus', '1','20210127'),
            ('diabetes', 'ICD10', 'O240', 'Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, insulin-dependent', '1','20210127'),
            ('diabetes', 'ICD10', 'O241', 'Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, non-insulin-dependent', '1','20210127'),
            ('diabetes', 'ICD10', 'O243', 'Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, unspecified','1', '20210127')]
    pheno_extractor.add_codes_to_masterlist(vals)



def add_asthma_codes(pheno_extractor):
    """Adds SNOMED and ICD10 codes for Asthma by Jeniffer Quint
    """
    columns = ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']
    vals = [('asthma','SNOMED','389145006','Allergic asthma','1','20210624'),
            ('asthma','SNOMED','389145006','Allergic asthma NEC','1','20210624'),
            ('asthma','SNOMED','195967001','Asthma','1','20210624'),
            ('asthma','SNOMED','708038006','Asthma NOS','1','20210624'),
            ('asthma','SNOMED','708038006','Asthma attack','1','20210624'),
            ('asthma','SNOMED','195967001','Asthma attack NOS','1','20210624'),
            ('asthma','SNOMED','195967001','Asthma unspecified','1','20210624'),
            ('asthma','SNOMED','225057002','Brittle asthma','1','20210624'),
            ('asthma','SNOMED','195967001','Bronchial asthma','1','20210624'),
            ('asthma','SNOMED','866881000000101','Chronic asthma with fixed airflow obstruction','1','20210624'),
            ('asthma','SNOMED','195949008','Chronic asthmatic bronchitis','1','20210624'),
            ('asthma','SNOMED','31387002','Exercise induced asthma','1','20210624'),
            ('asthma','SNOMED','31387002','Exercise induced asthma','1','20210624'),
            ('asthma','SNOMED','389145006','Extrinsic (atopic) asthma','1','20210624'),
            ('asthma','SNOMED','424643009','Extrinsic asthma NOS','1','20210624'),
            ('asthma','SNOMED','708093000','Extrinsic asthma with asthma attack','1','20210624'),
            ('asthma','SNOMED','1086701000000102','Extrinsic asthma with status asthmaticus','1','20210624'),
            ('asthma','SNOMED','63088003','Extrinsic asthma without status asthmaticus','1','20210624'),
            ('asthma','SNOMED','233683003','Hay fever with asthma','1','20210624'),
            ('asthma','SNOMED','233683003','Hay fever with asthma','1','20210624'),
            ('asthma','SNOMED','266361008','Intrinsic asthma','1','20210624'),
            ('asthma','SNOMED','266361008','Intrinsic asthma NOS','1','20210624'),
            ('asthma','SNOMED','708094006','Intrinsic asthma with asthma attack','1','20210624'),
            ('asthma','SNOMED','1086711000000100','Intrinsic asthma with status asthmaticus','1','20210624'),
            ('asthma','SNOMED','12428000','Intrinsic asthma without status asthmaticus','1','20210624'),
            ('asthma','SNOMED','233679003','Late onset asthma','1','20210624'),
            ('asthma','SNOMED','233679003','Late-onset asthma','1','20210624'),
            ('asthma','SNOMED','233683003','Pollen asthma','1','20210624'),
            ('asthma','SNOMED','708090002','Severe asthma attack','1','20210624'),
            ('asthma','SNOMED','734904007','Status asthmaticus NOS','1','20210624'),
            ('asthma','SNOMED','10692761000119107','Asthma-chronic obstructive pulmonary disease overlap syndrome','1','20210624'),
            ('asthma','SNOMED','195977004','Mixed asthma','1','20210624'),
            ('asthma','SNOMED','390921001','Absent from work or school due to asthma','1','20210624'),
            ('asthma','SNOMED','708038006','Acute exacerbation of asthma','1','20210624'),
            ('asthma','SNOMED','407674008','Aspirin induced asthma','1','20210624'),
            ('asthma','SNOMED','312453004','Asthma - currently active','1','20210624'),
            ('asthma','SNOMED','708373002','Asthma accident and emergency attendance since last visit','1','20210624'),
            ('asthma','SNOMED','394700004','Asthma annual review','1','20210624'),
            ('asthma','SNOMED','370202007','Asthma causes daytime symptoms 1 to 2 times per month','1','20210624'),
            ('asthma','SNOMED','370203002','Asthma causes daytime symptoms 1 to 2 times per week','1','20210624'),
            ('asthma','SNOMED','370204008','Asthma causes daytime symptoms most days','1','20210624'),
            ('asthma','SNOMED','370205009','Asthma causes night symptoms 1 to 2 times per month','1','20210624'),
            ('asthma','SNOMED','771901000000100','Asthma causes night time symptoms 1 to 2 times per week','1','20210624'),
            ('asthma','SNOMED','771941000000102','Asthma causes symptoms most nights','1','20210624'),
            ('asthma','SNOMED','170632009','Asthma causing night waking','1','20210624'),
            ('asthma','SNOMED','736056000','Asthma clinical management plan','1','20210624'),
            ('asthma','SNOMED','401193004','Asthma confirmed','1','20210624'),
            ('asthma','SNOMED','763077003','Asthma control questionnaire','1','20210624'),
            ('asthma','SNOMED','182727003','Asthma control step 1','1','20210624'),
            ('asthma','SNOMED','182728008','Asthma control step 2','1','20210624'),
            ('asthma','SNOMED','182729000','Asthma control step 3','1','20210624'),
            ('asthma','SNOMED','182730005','Asthma control step 4','1','20210624'),
            ('asthma','SNOMED','182731009','Asthma control step 5','1','20210624'),
            ('asthma','SNOMED','443117005','Asthma control test','1','20210624'),
            ('asthma','SNOMED','373899003','Asthma daytime symptoms','1','20210624'),
            ('asthma','SNOMED','170631002','Asthma disturbing sleep','1','20210624'),
            ('asthma','SNOMED','170634005','Asthma disturbs sleep frequently','1','20210624'),
            ('asthma','SNOMED','170633004','Asthma disturbs sleep weekly','1','20210624'),
            ('asthma','SNOMED','394701000','Asthma follow-up','1','20210624'),
            ('asthma','SNOMED','170637003','Asthma limiting activities','1','20210624'),
            ('asthma','SNOMED','771981000000105','Asthma limits activities 1 to 2 times per month','1','20210624'),
            ('asthma','SNOMED','772011000000107','Asthma limits activities 1 to 2 times per week','1','20210624'),
            ('asthma','SNOMED','772051000000106','Asthma limits activities most days','1','20210624'),
            ('asthma','SNOMED','370206005','Asthma limits walking on the flat','1','20210624'),
            ('asthma','SNOMED','370207001','Asthma limits walking up hills or stairs','1','20210624'),
            ('asthma','SNOMED','406162001','Asthma management plan given','1','20210624'),
            ('asthma','SNOMED','394720003','Asthma medication review','1','20210624'),
            ('asthma','SNOMED','270442000','Asthma monitored','1','20210624'),
            ('asthma','SNOMED','275908000','Asthma monitoring','1','20210624'),
            ('asthma','SNOMED','401183006','Asthma monitoring by doctor','1','20210624'),
            ('asthma','SNOMED','401182001','Asthma monitoring by nurse','1','20210624'),
            ('asthma','SNOMED','370208006','Asthma never causes daytime symptoms','1','20210624'),
            ('asthma','SNOMED','473391009','Asthma never causes night symptoms','1','20210624'),
            ('asthma','SNOMED','170636007','Asthma never disturbs sleep','1','20210624'),
            ('asthma','SNOMED','170658009','Asthma never restricts exercise','1','20210624'),
            ('asthma','SNOMED','395022009','Asthma night-time symptoms','1','20210624'),
            ('asthma','SNOMED','170655007','Asthma restricts exercise','1','20210624'),
            ('asthma','SNOMED','754061000000100','Asthma review using Roy Colleg of Physicians three questions','1','20210624'),
            ('asthma','SNOMED','811921000000103','Asthma self-management plan agreed','1','20210624'),
            ('asthma','SNOMED','810901000000102','Asthma self-management plan review','1','20210624'),
            ('asthma','SNOMED','170657004','Asthma severely restricts exercise','1','20210624'),
            ('asthma','SNOMED','170642006','Asthma severity','1','20210624'),
            ('asthma','SNOMED','170656008','Asthma sometimes restricts exercise','1','20210624'),
            ('asthma','SNOMED','370226009','Asthma treatment compliance satisfactory','1','20210624'),
            ('asthma','SNOMED','370225008','Asthma treatment compliance unsatisfactory','1','20210624'),
            ('asthma','SNOMED','340891000000106','Asthma trigger - airborne dust','1','20210624'),
            ('asthma','SNOMED','201051000000101','Asthma trigger - animals','1','20210624'),
            ('asthma','SNOMED','201191000000108','Asthma trigger - cold air','1','20210624'),
            ('asthma','SNOMED','201201000000105','Asthma trigger - damp','1','20210624'),
            ('asthma','SNOMED','201211000000107','Asthma trigger - emotion','1','20210624'),
            ('asthma','SNOMED','340901000000107','Asthma trigger - exercise','1','20210624'),
            ('asthma','SNOMED','340911000000109','Asthma trigger - pollen','1','20210624'),
            ('asthma','SNOMED','201031000000108','Asthma trigger - respiratory infection','1','20210624'),
            ('asthma','SNOMED','201041000000104','Asthma trigger - seasonal','1','20210624'),
            ('asthma','SNOMED','340921000000103','Asthma trigger - tobacco smoke','1','20210624'),
            ('asthma','SNOMED','340931000000101','Asthma trigger - warm air','1','20210624'),
            ('asthma','SNOMED','185728001','Attends asthma monitoring','1','20210624'),
            ('asthma','SNOMED','390872009','Change in asthma management plan','1','20210624'),
            ('asthma','SNOMED','41553006','Detergent asthma','1','20210624'),
            ('asthma','SNOMED','183478001','Emergency admission asthma"','1','20210624'),
            ('asthma','SNOMED','708358003','Emergency asthma admission since last appointment','1','20210624'),
            ('asthma','SNOMED','182724005','Further asthma - drug prevent.','1','20210624'),
            ('asthma','SNOMED','527191000000104','Health education - asthma self management','1','20210624'),
            ('asthma','SNOMED','527211000000100','Health education - structured asthma discussion','1','20210624'),
            ('asthma','SNOMED','527231000000108','Health education - structured patient focused asthma discuss','1','20210624'),
            ('asthma','SNOMED','370218001','Mild asthma','1','20210624'),
            ('asthma','SNOMED','763695004','Mini asthma quality of life questionnaire','1','20210624'),
            ('asthma','SNOMED','370219009','Moderate asthma','1','20210624'),
            ('asthma','SNOMED','811151000000105','Number days absent from school due to asthma in past 6 month','1','20210624'),
            ('asthma','SNOMED','366874008','Number of asthma exacerbations in past year','1','20210624'),
            ('asthma','SNOMED','370220003','Occasional asthma','1','20210624'),
            ('asthma','SNOMED','57607007','Occupational asthma','1','20210624'),
            ('asthma','SNOMED','527171000000103','Patient has a written asthma personal action plan','1','20210624'),
            ('asthma','SNOMED','448003001','Royal College Physician asthma assessment 3 question score','1','20210624'),
            ('asthma','SNOMED','302331000000106','Royal College of Physicians asthma assessment','1','20210624'),
            ('asthma','SNOMED','23315001','Sequoiosis (red-cedar asthma)','1','20210624'),
            ('asthma','SNOMED','370221004','Severe asthma','1','20210624'),
            ('asthma','SNOMED','390878008','Step down change in asthma management plan','1','20210624'),
            ('asthma','SNOMED','390877003','Step up change in asthma management plan','1','20210624'),
            ('asthma','SNOMED','698509001','Under care of asthma specialist nurse','1','20210624'),
            ('asthma','SNOMED','56968009','Wood asthma','1','20210624'),
            ('asthma','SNOMED','57607007','Work aggravated asthma','1','20210624'),
            ('asthma','SNOMED','312454005','Asthma - currently dormant','1','20210624'),
            ('asthma','SNOMED','713711000000105','Asthma clinic administration','1','20210624'),
            ('asthma','SNOMED','182726007','Asthma control step 0','1','20210624'),
            ('asthma','SNOMED','183099005','Asthma leaflet given','1','20210624'),
            ('asthma','SNOMED','185731000','Asthma monitor 1st letter','1','20210624'),
            ('asthma','SNOMED','185732007','Asthma monitor 2nd letter','1','20210624'),
            ('asthma','SNOMED','185734008','Asthma monitor 3rd letter','1','20210624'),
            ('asthma','SNOMED','185730004','Asthma monitor offer default','1','20210624'),
            ('asthma','SNOMED','185736005','Asthma monitor phone invite','1','20210624'),
            ('asthma','SNOMED','185735009','Asthma monitor verbal invite','1','20210624'),
            ('asthma','SNOMED','713701000000108','Asthma monitoring admin.','1','20210624'),
            ('asthma','SNOMED','713701000000108','Asthma monitoring admin.NOS','1','20210624'),
            ('asthma','SNOMED','270442000','Asthma monitoring check done','1','20210624'),
            ('asthma','SNOMED','390940007','Asthma monitoring due','1','20210624'),
            ('asthma','SNOMED','170635006','Asthma not disturbing sleep','1','20210624'),
            ('asthma','SNOMED','170638008','Asthma not limiting activities','1','20210624'),
            ('asthma','SNOMED','198971000000102','Asthma outreach clinic','1','20210624'),
            ('asthma','SNOMED','170647000','Asthma prophylactic medication used','1','20210624'),
            ('asthma','SNOMED','161105008','Asthma society member','1','20210624'),
            ('asthma','SNOMED','400987003','Asthma trigger','1','20210624'),
            ('asthma','SNOMED','233678006','Childhood asthma','1','20210624'),
            ('asthma','SNOMED','89581000000109','DNA - Did not attend asthma clinic','1','20210624'),
            ('asthma','SNOMED','716491000000100','Excepted from asthma quality indicators: Informed dissent','1','20210624'),
            ('asthma','SNOMED','717291000000103','Excepted from asthma quality indicators: Patient unstable','1','20210624'),
            ('asthma','SNOMED','715801000000103','Exception reporting: asthma quality indicators','1','20210624'),
            ('asthma','SNOMED','161527007','H/O: asthma','1','20210624'),
            ('asthma','SNOMED','401135008','Health education - asthma','1','20210624'),
            ('asthma','SNOMED','195977004','Mixed asthma','1','20210624'),
            ('asthma','SNOMED','185940009','Patient in asthma study','1','20210624'),
            ('asthma','SNOMED','415265005','Referral to asthma clinic','1','20210624'),
            ('asthma','SNOMED','763221007','Refuses asthma monitoring','1','20210624'),
            ('asthma','SNOMED','185242005','Seen in asthma clinic','1','20210624'),
            ('asthma','SNOMED','966011000000109','At risk severe asthma exacerbation','1','20210624'),
            ('asthma','SNOMED','966031000000101','Severe asthma exacerbation risk assessment','1','20210624'),
            ('asthma','SNOMED','905301000103','Childhood Asthma Control Test','1','20210624'),
            ('asthma','SNOMED','170627008','Airways obstruction reversible','1','20210624'),
            ('asthma','SNOMED','892301000000100','Asthma management plan declined','1','20210624'),
            ('asthma','SNOMED','715191006','Telehealth asthma monitoring','1','20210624'),
            ('asthma','SNOMED','791401000000104','Seen in school asthma clinic','1','20210624'),
            ('asthma','SNOMED','928451000000107','Asthma monitoring invitation SMS (short message service) text message','1','20210624'),
            ('asthma','SNOMED','959401000000101','Asthma monitoring SMS (short message service) text message first invitation','1','20210624'),
            ('asthma','SNOMED','959421000000105','Asthma monitoring SMS (short message service) text message second invitation','1','20210624'),
            ('asthma','SNOMED','959441000000103','Asthma monitoring SMS (short message service) text message third invitation','1','20210624'),
            ('asthma','SNOMED','92851000000107','Asthma monitoring invitation email','1','20210624'),
            ('asthma','ICD10','J45','Asthma','1','20210624'),
            ('asthma','ICD10','J46','Status asthmaticus','1','20210624')]

    pheno_extractor.add_codes_to_masterlist(vals)


def add_ckd_codes(pheno_extractor):
    """Adds CKD codes defined by AD from CaReMe project.
    """
    columns = ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']
    vals = [('ckdb', 'ICD10', 'N181', 'chronic kidney disease stage 1', '1', '20210127'),
            ('ckdb', 'ICD10', 'N182', 'chronic kidney disease stage 2', '1','20210127'),
            ('ckdb', 'ICD10', 'N183', 'chronic kidney disease stage 3', '1','20210127'),
            ('ckdb', 'ICD10', 'N184', 'chronic kidney disease stage 4', '1','20210127'),
            ('ckdb', 'ICD10', 'N185', 'chronic kidney disease stage 5', '1','20210127'),
            ('ckdb', 'ICD10', 'T824', 'mechanical complication of vascular dialysis catheter', '1','20210127'),
            ('ckdb', 'ICD10', 'T861', 'kidney transplant failure and rejection', '1', '20210127'),
            ('ckdb', 'ICD10', 'Y602', 'during kidney dialysis or other perfusion', '1','20210127'),
            ('ckdb', 'ICD10', 'Y612', 'during kidney dialysis or other perfusion', '1','20210127'),
            ('ckdb', 'ICD10', 'Y622', 'during kidney dialysis or other perfusion', '1','20210127'),
            ('ckdb', 'ICD10', 'Y841', 'kidney dialysis', '1','20210127'),
            ('ckdb', 'ICD10', 'Z49', 'care involving dialysis', '1','20210127'),
            ('ckdb', 'ICD10', 'Z490', 'preparatory care for dialysis', '1','20210127'),
            ('ckdb', 'ICD10', 'Z491', 'extracorporeal dialysis','1', '20210127'),
            ('ckdb', 'ICD10', 'Z492', 'other dialysis', '1','20210127'),
            ('ckdb', 'ICD10', 'Z940', 'kidney transplant status', '1','20210127'),
            ('ckdb', 'ICD10', 'Z992', 'dependence on renal dialysis//on dialysis treatment', '1','20210127'),
            ('ckdb', 'ICD10', 'N186', 'patients with CKD requiring dialysis', '1','20210127'),
            ('ckdb', 'ICD10', 'I770', 'arteriovenous fistula', '1','20210127'),
            ('ckdb', 'ICD10', 'N165', 'renal tubulo-interstitial disorders in transplant rejection', '1','20210127'),
            ('ckdb','SNOMED','700379002','chronic kidney disease stage 3b','1','20210127'),
            ('ckdb','SNOMED','324121000000109','chronic kidney disease stage 1 with proteinuria','1','20210127'),
            ('ckdb','SNOMED','994401000006102','chronic kidney disease stage 1','1','20210127'),
            ('ckdb','SNOMED','431855005','chronic kidney disease stage 1','1','20210127'),
            ('ckdb','SNOMED','324181000000105','chronic kidney disease stage 2 with proteinuria','1','20210127'),
            ('ckdb','SNOMED','994411000006104','chronic kidney disease stage 2','1','20210127'),
            ('ckdb','SNOMED','431856006','chronic kidney disease stage 2','1','20210127'),
            ('ckdb','SNOMED','324251000000105','chronic kidney disease stage 3 with proteinuria','1','20210127'),
            ('ckdb','SNOMED','324311000000101','chronic kidney disease stage 3a with proteinuria','1','20210127'),
            ('ckdb','SNOMED','324541000000105','chronic kidney disease stage 5 without proteinuria','1','20210127'),
            ('ckdb','SNOMED','994441000006100','chronic kidney disease stage 5','1','20210127'),
            ('ckdb','SNOMED','950291000000103','chronic kidney disease with glomerular filtration rate category g5 and albuminuria category a2','1','20210127'),
            ('ckdb','SNOMED','950211000000107','chronic kidney disease with glomerular filtration rate category g4 and albuminuria category a2','1','20210127'),
            ('ckdb','SNOMED','324441000000106','ckd (chronic kidney disease stage 4 with proteinuria)','1','20210127'),
            ('ckdb','SNOMED','949881000000106','chronic kidney disease with glomerular filtration rate category g3a and albuminuria category a1','1','20210127'),
            ('ckdb','SNOMED','324341000000100','ckd (chronic kidney disease stage 3a without proteinuria)','1','20210127'),
            ('ckdb','SNOMED','714153000','chronic kidney disease 5t','1','20210127'),
            ('ckdb','SNOMED','714152005','ckd (chronic kidney disease stage 5d)','1','20210127'),
            ('ckdb','SNOMED','433146000','chronic kidney disease stage 5','1','20210127'),
            ('ckdb','SNOMED','950181000000106','ckd g4a1 - chronic kidney disease with glomerular filtration rate category g4 and albuminuria category a1','1','20210127'),
            ('ckdb','SNOMED','949521000000108','chronic kidney disease with glomerular filtration rate category g2 and albuminuria category a1','1','20210127'),
            ('ckdb','SNOMED','949621000000109','ckd g2a3 - chronic kidney disease with glomerular filtration rate category g2 and albuminuria category a3','1','20210127'),
            ('ckdb','SNOMED','949901000000109','ckd g3aa2 - chronic kidney disease with glomerular filtration rate category g3a and albuminuria category a2','1','20210127'),
            ('ckdb','SNOMED','949921000000100','ckd g3aa3 - chronic kidney disease with glomerular filtration rate category g3a and albuminuria category a3','1','20210127'),
            ('ckdb','SNOMED','950061000000103','ckd g3ba1 - chronic kidney disease with glomerular filtration rate category g3b and albuminuria category a1','1','20210127'),
            ('ckdb','SNOMED','950101000000101','ckd g3ba3 - chronic kidney disease with glomerular filtration rate category g3b and albuminuria category a3','1','20210127'),
            ('ckdb','SNOMED','950231000000104','ckd g4a3 - chronic kidney disease with glomerular filtration rate category g4 and albuminuria category a3','1','20210127'),
            ('ckdb','SNOMED','949421000000107','ckd g1a2 - chronic kidney disease with glomerular filtration rate category g1 and albuminuria category a2','1','20210127'),
            ('ckdb','SNOMED','431857002','chronic kidney disease stage 4','1','20210127'),
            ('ckdb','SNOMED','433144002','chronic kidney disease stage 3','1','20210127'),
            ('ckdb','SNOMED','324411000000105','chronic kidney disease stage 3b without proteinuria','1','20210127'),
            ('ckdb','SNOMED','324471000000100','chronic kidney disease stage 4 without proteinuria','1','20210127'),
            ('ckdb','SNOMED','324281000000104','ckd (chronic kidney disease stage 3 without proteinuria)','1','20210127'),
            ('ckdb','SNOMED','949481000000108','chronic kidney disease with glomerular filtration rate category g1 and albuminuria category a3','1','20210127'),
            ('ckdb','SNOMED','324371000000106','chronic kidney disease stage 3b with proteinuria','1','20210127'),
            ('ckdb','SNOMED','950251000000106','ckd g5a1 - chronic kidney disease with glomerular filtration rate category g5 and albuminuria category a1','1','20210127'),
            ('ckdb','SNOMED','950081000000107','ckd g3ba2 - chronic kidney disease with glomerular filtration rate category g3b and albuminuria category a2','1','20210127'),
            ('ckdb','SNOMED','949561000000100','chronic kidney disease with glomerular filtration rate category g2 and albuminuria category a2','1','20210127'),
            ('ckdb','SNOMED','994421000006107','chronic kidney disease stage 3','1','20210127'),
            ('ckdb','SNOMED','324501000000107','ckd (chronic kidney disease stage 5 with proteinuria)','1','20210127'),
            ('ckdb','SNOMED','950311000000102','ckd g5a3 - chronic kidney disease with glomerular filtration rate category g5 and albuminuria category a3','1','20210127'),
            ('ckdb','SNOMED','994431000006105','chronic kidney disease stage 4','1','20210127'),
            ('ckdb','SNOMED','700378005','chronic kidney disease stage 3a','1','20210127'),
            ('ckdb','SNOMED','216933008','Failure of sterile precautions during kidney dialysis','1','20210127'),
            ('ckdb','SNOMED','238318009','Continuous ambulatory peritoneal dialysis','1','20210127'),
            ('ckdb','SNOMED','423062001','Stenosis of arteriovenous dialysis fistula','1','20210127'),
            ('ckdb','SNOMED','438546008','Ligation of arteriovenous dialysis fistula','1','20210127'),
            ('ckdb','SNOMED','180272001','Placement ambulatory dialysis apparatus - compens renal fail','1','20210127'),
            ('ckdb','SNOMED','385971003','Preparatory care for dialysis','1','20210127'),
            ('ckdb','SNOMED','426361000000104','Unspecified aftercare involving intermittent dialysis','1','20210127'),
            ('ckdb','SNOMED','302497006','Haemodialysis','1','20210127'),
            ('ckdb','SNOMED','251859005','Dialysis finding','1','20210127'),
            ('ckdb','SNOMED','108241001','Dialysis procedure','1','20210127'),
            ('ckdb','SNOMED','398471000000102','Aftercare involving intermittent dialysis','1','20210127'),
            ('ckdb','SNOMED','180273006','Removal of ambulatory peritoneal dialysis catheter','1','20210127'),
            ('ckdb','SNOMED','426340003','Creation of graft fistula for dialysis','1','20210127'),
            ('ckdb','SNOMED','410511000000103','Aftercare involving renal dialysis NOS','1','20210127'),
            ('ckdb','SNOMED','426351000000102','Aftercare involving peritoneal dialysis','1','20210127'),
            ('ckdb','SNOMED','71192002','Peritoneal dialysis NEC','1','20210127'),
            ('ckdb','SNOMED','116223007','"Kidney dialysis with complication, without blame"','1','20210127'),
            ('ckdb','SNOMED','428648006','Automated peritoneal dialysis','1','20210127'),
            ('ckdb','SNOMED','991521000000102','Dialysis fluid glucose level','1','20210127'),
            ('ckdb','SNOMED','161693006','H/O: renal dialysis','1','20210127'),
            ('ckdb','SNOMED','17778006','Mechanical complication of dialysis catheter','1','20210127'),
            ('ckdb','SNOMED','216878005','"Accidental cut, puncture, perforation or haemorrhage during kidney dialysis"','1','20210127'),
            ('ckdb','SNOMED','276883000','Peritoneal dialysis-associated peritonitis','1','20210127'),
            ('ckdb','SNOMED','265764009','Renal dialysis','1','20210127'),
            ('ckdb','SNOMED','180277007','Insertion of temporary peritoneal dialysis catheter','1','20210127'),
            ('ckdb','SNOMED','79827002','cadaveric renal transplant','1','20210127'),
            ('ckdb','SNOMED','269698004','failure of sterile precautions during kidney dialysis','1','20210127'),
            ('ckdb','SNOMED','269691005','very mild acute rejection of renal transplant','1','20210127'),
            ('ckdb','SNOMED','216904007','acute rejection of renal transplant - grade i','1','20210127'),
            ('ckdb','SNOMED','70536003','Renal transplant stage 5','1','20210127'),
            ('ckdb','SNOMED','427053002','Extracorporeal albumin haemodialysis stage 5','1','20210127'),
            ('ckdb','SNOMED','225283000','Priming haemodialysis lines stage 5','1','20210127'),
            ('ckdb','SNOMED','420106004','Renal transplant venogram stage 5','1','20210127'),
            ('ckdb','SNOMED','708932005','Emergency haemodialysis stage 5','1','20210127'),
            ('ckdb','SNOMED','5571000001109','solution haemodialysis stage 5','1','20210127'),
            ('ckdb','SNOMED','366961000000102','Renal transplant recipient stage 5','1','20210127'),
            ('ckdb','SNOMED','57274006','Initial haemodialysis stage 5','1','20210127'),
            ('ckdb','SNOMED','233575001','Intermittent haemodialysis stage 5','1','20210127'),
            ('ckdb','SNOMED','85223007','Complication of haemodialysis stage 5','1','20210127'),
            ('ckdb','SNOMED','284991000119104','Chronic kidney disease stage 3 due to benign hypertension (disorder)','1','20210127'),
            ('ckdb','SNOMED','368441000119102','Chronic kidney disease stage 3 due to drug induced diabetes mellitus (disorder)','1','20210127'),
            ('ckdb','SNOMED','129171000119106','Chronic kidney disease stage 3 due to hypertension (disorder)','1','20210127'),
            ('ckdb','SNOMED','90741000119107','Chronic kidney disease stage 3 due to type 1 diabetes mellitus (disorder)','1','20210127'),
            ('ckdb','SNOMED','731000119105','Chronic kidney disease stage 3 due to type 2 diabetes mellitus (disorder)','1','20210127'),
            ('ckdb','SNOMED','285001000119105','Chronic kidney disease stage 4 due to benign hypertension (disorder)','1','20210127'),
            ('ckdb','SNOMED','368451000119100','Chronic kidney disease stage 4 due to drug induced diabetes mellitus (disorder)','1','20210127'),
            ('ckdb','SNOMED','90751000119109','Chronic kidney disease stage 4 due to type 1 diabetes mellitus (disorder)','1','20210127'),
            ('ckdb','SNOMED','721000119107','Chronic kidney disease stage 4 due to type 2 diabetes mellitus (disorder)','1','20210127'),
            ('ckdb','SNOMED','285011000119108','Chronic kidney disease stage 5 due to benign hypertension (disorder)','1','20210127'),
            ('ckdb','SNOMED','368461000119103','Chronic kidney disease stage 5 due to drug induced diabetes mellitus (disorder)','1','20210127'),
            ('ckdb','SNOMED','129161000119100','Chronic kidney disease stage 5 due to hypertension (disorder)','1','20210127'),
            ('ckdb','SNOMED','90761000119106','Chronic kidney disease stage 5 due to type 1 diabetes mellitus (disorder)','1','20210127'),
            ('ckdb','SNOMED','711000119100','Chronic kidney disease stage 5 due to type 2 diabetes mellitus (disorder)','1','20210127'),
            ('ckdb','SNOMED','96701000119107','Hypertensive heart AND Chronic kidney disease on dialysis (disorder)','1','20210127'),
            ('ckdb','SNOMED','96751000119106','Hypertensive heart AND Chronic kidney disease stage 1 (disorder)','1','20210127'),
            ('ckdb','SNOMED','96741000119109','Hypertensive heart AND Chronic kidney disease stage 2 (disorder)','1','20210127'),
            ('ckdb','SNOMED','96731000119100','Hypertensive heart AND Chronic kidney disease stage 3 (disorder)','1','20210127'),
            ('ckdb','SNOMED','96721000119103','Hypertensive heart AND Chronic kidney disease stage 4 (disorder)','1','20210127'),
            ('ckdb','SNOMED','96711000119105','Hypertensive heart AND Chronic kidney disease stage 5 (disorder)','1','20210127'),
            ('ckdb','SNOMED','285851000119102','Malignant Hypertensive Chronic kidney disease stage 1 (disorder)','1','20210127'),
            ('ckdb','SNOMED','285861000119100','Malignant Hypertensive Chronic kidney disease stage 2 (disorder)','1','20210127'),
            ('ckdb','SNOMED','285871000119106','Malignant Hypertensive Chronic kidney disease stage 3 (disorder)','1','20210127'),
            ('ckdb','SNOMED','285881000119109','Malignant Hypertensive Chronic kidney disease stage 4 (disorder)','1','20210127'),
            ('ckdb','SNOMED','153851000119106','Malignant Hypertensive Chronic kidney disease stage 5 (disorder)','1','20210127'),
            ('ckdb','SNOMED','285841000119104','Malignant Hypertensive end stage renal disease (disorder)','1','20210127'),
            ('ckdb','SNOMED','286371000119107','Malignant Hypertensive end stage renal disease on dialysis (disorder)','1','20210127')]
    pheno_extractor.add_codes_to_masterlist(vals)



def add_dispensed_steroid_codes(pheno_extractor):
    """Adds corticosteroid codes defined extracted from
    OpenPrescription BHF chemical-level codes based on
    CALIBER Corticosteroid oral medicine phenotype
    """
    columns = ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']
    vals = [('Corticosteroid', 'BNF_chemical', '0603010I0', 'Fludrocortisone Acetate', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020B0', 'Betamethasone', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020C0', 'Betamethasone Sodium Phosphate', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020F0', 'Cortisone Acetate', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020I0', 'Deflazacort', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020G0', 'Dexamethasone', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020AA', 'Dexamethasone Phosphate', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020H0', 'Dexamethasone Sodium Phosphate', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020J0', 'Hydrocortisone', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020L0', 'Hydrocortisone Sodium Phosphate', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020M0', 'Hydrocortisone Sodium Succinate', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020S0', 'Methylprednisolone', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020AC', 'Methylprednisolone Aceponate', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020K0', 'Methylprednisolone Sodium Succinate', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020T0', 'Prednisolone', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020X0', 'Prednisone', '1', '20210802'),
            ('Corticosteroid', 'BNF_chemical', '0603020Z0', 'Triamcinolone Acetonide', '1', '20210802')]
    pheno_extractor.add_codes_to_masterlist(vals)


def add_prescribed_steroid_codes(pheno_extractor):
    """Adds corticosteroid codes defined extracted from NHSD SNOMED CT browser based on
    CALIBER Corticosteroid oral medical phenotype
    """
    columns = ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']
    vals = [('Corticosteroid','SNOMED','350387002','Product containing betamethasone in oral dose form ','1','20210708'),
            ('Corticosteroid','SNOMED','778504001','Product containing only betamethasone in oral dose form','1','20210708'),
            ('Corticosteroid','SNOMED','784609000','Product containing precisely betamethasone 120 microgram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325345003','Product containing precisely betamethasone 500 microgram/1 each conventional release oral tablet','1','20210708'),
            ('Corticosteroid','SNOMED','376549004','Product containing precisely betamethasone 600 microgram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','768296006','Product containing prednisone in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','17872811000001100','Prednisone 1mg modified-release tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17873211000001107','Prednisone 2mg modified-release tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17873811000001108','Prednisone 5mg modified-release tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','373989007','Product containing precisely prednisone 2.5 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','374058000','Product containing precisely prednisone 10 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325456002','Product containing precisely prednisone 20 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','374072009','Product containing precisely prednisone 50 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','11835611000001107','Clipper 5mg gastro-resistant modified-release tablets (Trinity-Chiesi Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','11835711000001103','Clipper 5mg gastro-resistant modified-release tablets (Trinity-Chiesi Pharmaceuticals Ltd) 30 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','11835611000001107','Clipper 5mg gastro-resistant modified-release tablets (Trinity-Chiesi Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','11835711000001103','Clipper 5mg gastro-resistant modified-release tablets (Trinity-Chiesi Pharmaceuticals Ltd) 30 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','11880411000001100','beclometasone gastro-resistant modified release tablets 5mg','1','20210708'),
            ('Corticosteroid','SNOMED','3804311000001103','Betnelan 500microgram tablets (Celltech Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','9223501000001105','Betnelan (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3804411000001105','Betnelan 500microgram tablets (Celltech Pharmaceuticals Ltd) 100 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','9228101000001105','Budenofalk (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3807811000001103','Budenofalk 3mg gastro-resistant capsules (Dr Falk Pharma UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','16349011000001102','Budenofalk 3mg gastro-resistant capsules (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','16349111000001101','Budenofalk 3mg gastro-resistant capsules (Mawdsley-Brooks & Company Ltd) 100 capsule (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3808011000001105','Budenofalk 3mg gastro-resistant capsules (Dr Falk Pharma UK Ltd) 100 capsule 10 x 10 capsules (product)','1','20210708'),
            ('Corticosteroid','SNOMED','19738311000001103','Budenofalk 9mg gastro-resistant granules sachets (Dr. Falk Pharma UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','19738511000001109','Budenofalk 9mg gastro-resistant granules sachets (Dr. Falk Pharma UK Ltd) 60 sachet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','9276901000001105','Entocort CR (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37889411000001101','Entocort CR 3mg capsules (CST Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','13871611000001102','Entocort CR 3mg capsules (Doncaster Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','13160111000001102','Entocort CR 3mg capsules (Dowelhurst Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','19963611000001100','Entocort CR 3mg capsules (Lexon (UK) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','5363111000001107','Entocort CR 3mg capsules (PI) (Waymade Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3765811000001105','Entocort CR 3mg capsules (Tillotts Pharma UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','108622005','Product containing budesonide (medicinal product)','1','20210708'),
            ('Corticosteroid','SNOMED','35912611000001102','Budesonide 3mg gastro-resistant modified-release capsules (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3654611000001109','Budesonide 500micrograms/ml nebuliser liquid 2ml unit dose vials (product)','1','20210708'),
            ('Corticosteroid','SNOMED','19743911000001108','Budesonide 9mg gastro-resistant granules sachets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','29094911000001100','Budesonide 9mg modified-release tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','356117009','Product containing budesonide in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','3669611000001101','Cortisone 25mg tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3670511000001103','Cortisone 25mg tablets (Unichem Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','20093711000001103','Cortisone 25mg tablets (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325349009 ','Product containing precisely cortisone acetate 25 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','376711001','Product containing precisely cortisone acetate 5 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','768270009','Product containing cortisone in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','16602005','Product containing hydrocortisone (medicinal product)','1','20210708'),
            ('Corticosteroid','SNOMED','373982003','Hydrocortisone 5 mg oral tablet','1','20210708'),
            ('Corticosteroid','SNOMED','325374007','Hydrocortisone 20 mg oral tablet','1','20210708'),
            ('Corticosteroid','SNOMED','13614511000001104','Hydrocortisone 20mg tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17974111000001109','Hydrocortisone 20mg tablets (Almus Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','13661911000001107','Hydrocortisone 20mg tablets (Auden McKenzie (Pharma Division) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','34616411000001107','Hydrocortisone 20mg tablets (Bristol Laboratories Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','24154311000001104','Hydrocortisone 20mg tablets (DE Pharmaceuticals) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','35109111000001100','Hydrocortisone 20mg tablets (Flynn Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','34702511000001102','Hydrocortisone 20mg tablets (Focus Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','34958011000001107','Hydrocortisone 20mg tablets (Genesis Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32407611000001101','Hydrocortisone 20mg tablets (Kent Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37089411000001100','Hydrocortisone 20mg tablets (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','38946311000001105','Hydrocortisone 20mg tablets (Medihealth (Northern) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','38063711000001102','Hydrocortisone 20mg tablets (NorthStar Healthcare Unlimited Company) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17930411000001108','Hydrocortisone 20mg tablets (Phoenix Healthcare Distribution Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','36508611000001108','Hydrocortisone 20mg tablets (Resolution Chemicals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15092911000001100','Hydrocortisone 20mg tablets (Sigma Pharmaceuticals Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','29837011000001103','Hydrocortisone 20mg tablets (Sovereign Medical Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','18394311000001100','Hydrocortisone 20mg tablets (Teva UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','13807011000001108','Hydrocortisone 20mg tablets (UniChem Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','22044611000001109','Hydrocortisone 20mg tablets (Waymade Healthcare Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','706611000001109','Hydrocortone 20mg tablets (Merck Sharp & Dohme Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','36917511000001101','Hydventia 20mg tablets (OcXia) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325373001','Product containing precisely hydrocortisone 10 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','13614311000001105','Hydrocortisone 10mg tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32711611000001102','Hydrocortisone 10mg tablets (Alissa Healthcare Research Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17973911000001105','Hydrocortisone 10mg tablets (Almus Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','33423211000001105','Hydrocortisone 10mg tablets (AMCo) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','13661711000001105','Hydrocortisone 10mg tablets (Auden McKenzie (Pharma Division) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','34616211000001108','Hydrocortisone 10mg tablets (Bristol Laboratories Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','24154111000001101','Hydrocortisone 10mg tablets (DE Pharmaceuticals) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','18580411000001102','Hydrocortisone 10mg tablets (Doncaster Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','35108911000001105','Hydrocortisone 10mg tablets (Flynn Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','34957811000001100','Hydrocortisone 10mg tablets (Genesis Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37126311000001101','Hydrocortisone 10mg tablets (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','38946111000001108','Hydrocortisone 10mg tablets (Medihealth (Northern) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','38070711000001101','Hydrocortisone 10mg tablets (Mibe Pharma UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37494211000001109','Hydrocortisone 10mg tablets (NorthStar Healthcare Unlimited Company) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17930211000001109','Hydrocortisone 10mg tablets (Phoenix Healthcare Distribution Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','16504111000001108','Hydrocortisone 10mg tablets (PI) (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17559811000001109','Hydrocortisone 10mg tablets (PI) (Sigma Pharmaceuticals Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','36508411000001105','Hydrocortisone 10mg tablets (Resolution Chemicals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15092711000001102','Hydrocortisone 10mg tablets (Sigma Pharmaceuticals Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','18392311000001107','Hydrocortisone 10mg tablets (Teva UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','13806811000001104','Hydrocortisone 10mg tablets (UniChem Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','22044411000001106','Hydrocortisone 10mg tablets (Waymade Healthcare Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','197211000001101','Hydrocortone 10mg tablets (Merck Sharp & Dohme Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','36917711000001106','Hydventia 10mg tablets (OcXia) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','28422811000001103','Hydrocortisone 5mg/5ml oral solution (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555511000001106','Hydrocortisone 6mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12540711000001109','Hydrocortisone 1mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12541011000001103','Hydrocortisone 2mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','785096006','Product containing precisely hydrocortisone 2 milligram/1 milliliter conventional release oral suspension (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','8581611000001105','Hydrocortisone 5mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12554911000001103','Hydrocortisone 3mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12555111000001102','Hydrocortisone 4mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12563811000001104','Hydrocortisone 7mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12554811000001108','Hydrocortisone 30mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12563711000001107','Hydrocortisone 75mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','8581411000001107','Hydrocortisone 20mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','8581511000001106','Hydrocortisone 25mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12540611000001100','Hydrocortisone 18mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12555211000001108','Hydrocortisone 50mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','8581111000001102','Hydrocortisone 10mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','8581311000001100','Hydrocortisone 2.5mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12540311000001105','Hydrocortisone 1.5mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12563611000001103','Hydrocortisone 7.5mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12540411000001103','Hydrocortisone 100mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12540811000001101','Hydrocortisone 2.6mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12554611000001109','Hydrocortisone 3.6mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12555411000001107','Hydrocortisone 6.25mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12555011000001103','Hydrocortisone 4.25mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12554711000001100','Hydrocortisone 3.75mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','8581211000001108','Hydrocortisone 12.5mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','28418611000001106','Hydrocortisone 5mg/5ml oral solution 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12540511000001104','Hydrocortisone 13.95mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12529811000001108','Hydrocortisone 1mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12558311000001109','Hydrocortisone 7mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','8531711000001108','Hydrocortisone 5mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12552711000001105','Hydrocortisone 4mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12552111000001109','Hydrocortisone 3mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12530911000001107','Hydrocortisone 2mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12553911000001100','Hydrocortisone 6mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12551811000001106','Hydrocortisone 30mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12529511000001105','Hydrocortisone 18mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','8531011000001106','Hydrocortisone 25mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12553311000001101','Hydrocortisone 50mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','8532011000001103','Hydrocortisone 10mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','8533011000001107','Hydrocortisone 20mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12555911000001104','Hydrocortisone 75mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12551211000001105','Hydrocortisone 3.6mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12528411000001103','Hydrocortisone 1.5mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12563611000001103','Hydrocortisone 7.5mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12540411000001103','Hydrocortisone 100mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12540811000001101','Hydrocortisone 2.6mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12554611000001109','Hydrocortisone 3.6mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555411000001107','Hydrocortisone 6.25mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12555011000001103','Hydrocortisone 4.25mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12554711000001100','Hydrocortisone 3.75mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','8581211000001108','Hydrocortisone 12.5mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','28418611000001106','Hydrocortisone 5mg/5ml oral solution 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12540511000001104','Hydrocortisone 13.95mg/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12529811000001108','Hydrocortisone 1mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12558311000001109','Hydrocortisone 7mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','8531711000001108','Hydrocortisone 5mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12552711000001105','Hydrocortisone 4mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12552111000001109','Hydrocortisone 3mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12530911000001107','Hydrocortisone 2mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12553911000001100','Hydrocortisone 6mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12551811000001106','Hydrocortisone 30mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12529511000001105','Hydrocortisone 18mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','8531011000001106','Hydrocortisone 25mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12553311000001101','Hydrocortisone 50mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','8532011000001103','Hydrocortisone 10mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','8533011000001107','Hydrocortisone 20mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12555911000001104','Hydrocortisone 75mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12551211000001105','Hydrocortisone 3.6mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12528411000001103','Hydrocortisone 1.5mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','8532211000001108','Hydrocortisone 2.5mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','21315711000001109','Hydrocortisone 5mg/5ml oral suspension 100 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12528711000001109','Hydrocortisone 100mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12555611000001105','Hydrocortisone 7.5mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12530111000001109','Hydrocortisone 2.6mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','21390311000001104','Hydrocortisone 10mg/5ml oral suspension 100 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12553611000001106','Hydrocortisone 6.25mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','28422811000001103','Hydrocortisone 5mg/5ml oral solution (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12551511000001108','Hydrocortisone 3.75mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','8533311000001105','Hydrocortisone 12.5mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12552411000001104','Hydrocortisone 4.25mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12529211000001107','Hydrocortisone 13.95mg/5ml oral suspension 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','12563911000001109','Hydrocortisone 85micrograms/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12555311000001100','Hydrocortisone 50micrograms/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12554911000001103','Hydrocortisone 3mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12540911000001106','Hydrocortisone 250micrograms/5ml oral suspension','1','20210708'),
            ('Corticosteroid','SNOMED','12540711000001109','Hydrocortisone 1mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8581611000001105','Hydrocortisone 5mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555111000001102','Hydrocortisone 4mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12541011000001103','Hydrocortisone 2mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12563811000001104','Hydrocortisone 7mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555511000001106','Hydrocortisone 6mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12540611000001100','Hydrocortisone 18mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12563711000001107','Hydrocortisone 75mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8581111000001102','Hydrocortisone 10mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555211000001108','Hydrocortisone 50mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','20093511000001108','Hydrocortisone 5mg/5ml oral suspension sugar free (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8581511000001106','Hydrocortisone 25mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12554811000001108','Hydrocortisone 30mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8581411000001107','Hydrocortisone 20mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12540811000001101','Hydrocortisone 2.6mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12563611000001103','Hydrocortisone 7.5mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8581311000001100','Hydrocortisone 2.5mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12554611000001109','Hydrocortisone 3.6mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12540311000001105','Hydrocortisone 1.5mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12540411000001103','Hydrocortisone 100mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555011000001103','Hydrocortisone 4.25mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','350448001','Product containing hydrocortisone in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','8581211000001108','Hydrocortisone 12.5mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12554711000001100','Hydrocortisone 3.75mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555411000001107','Hydrocortisone 6.25mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','28418611000001106','Hydrocortisone 5mg/5ml oral solution 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','28418711000001102','Hydrocortisone 5mg/5ml oral solution (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12540511000001104','Hydrocortisone 13.95mg/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12558611000001104','Hydrocortisone 85micrograms/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12553011000001104','Hydrocortisone 50micrograms/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12553911000001100','Hydrocortisone 6mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12552711000001105','Hydrocortisone 4mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12529811000001108','Hydrocortisone 1mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12530911000001107','Hydrocortisone 2mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12552111000001109','Hydrocortisone 3mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8531711000001108','Hydrocortisone 5mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12558311000001109','Hydrocortisone 7mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12530411000001104','Hydrocortisone 250micrograms/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8532011000001103','Hydrocortisone 10mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12551811000001106','Hydrocortisone 30mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','20092111000001105','Hydrocortisone 5mg/5ml oral suspension sugar free 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8531011000001106','Hydrocortisone 25mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12529511000001105','Hydrocortisone 18mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12553311000001101','Hydrocortisone 50mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555911000001104','Hydrocortisone 75mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8533011000001107','Hydrocortisone 20mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8532211000001108','Hydrocortisone 2.5mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','21315711000001109','Hydrocortisone 5mg/5ml oral suspension 100 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12528411000001103','Hydrocortisone 1.5mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12528711000001109','Hydrocortisone 100mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12551211000001105','Hydrocortisone 3.6mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555611000001105','Hydrocortisone 7.5mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12530111000001109','Hydrocortisone 2.6mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','20092211000001104','Hydrocortisone 5mg/5ml oral suspension sugar free 100 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','21390311000001104','Hydrocortisone 10mg/5ml oral suspension 100 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12553611000001106','Hydrocortisone 6.25mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12551511000001108','Hydrocortisone 3.75mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8533311000001105','Hydrocortisone 12.5mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12552411000001104','Hydrocortisone 4.25mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12563911000001109','Hydrocortisone 85micrograms/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555311000001100','Hydrocortisone 50micrograms/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12529211000001107','Hydrocortisone 13.95mg/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12540911000001106','Hydrocortisone 250micrograms/5ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','20093511000001108','Hydrocortisone 5mg/5ml oral suspension sugar free (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12553011000001104','Hydrocortisone 50micrograms/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12558611000001104','Hydrocortisone 85micrograms/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12530411000001104','Hydrocortisone 250micrograms/5ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','20092111000001105','Hydrocortisone 5mg/5ml oral suspension sugar free 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','20092211000001104','Hydrocortisone 5mg/5ml oral suspension sugar free 100 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','20428711000001106','Hydrocortisone 1mg capsules (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15427611000001107','Hydrocortisone 2mg capsules (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','28044511000001102','Hydrocortisone 4mg pastilles (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15524111000001100','Hydrocortisone 2.5mg capsules (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','28418711000001102','Hydrocortisone 5mg/5ml oral solution (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15427811000001106','Hydrocortisone 2mg capsules (Special Order) 1 capsule (product)','1','20210708'),
            ('Corticosteroid','SNOMED','20428711000001106','Hydrocortisone 1mg capsules (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','20428911000001108','Hydrocortisone 1mg capsules (Special Order) 1 capsule (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15427611000001107','Hydrocortisone 2mg capsules (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12552211000001103','Hydrocortisone 3mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12529911000001103','Hydrocortisone 1mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12531011000001104','Hydrocortisone 2mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12552811000001102','Hydrocortisone 4mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','28044511000001102','Hydrocortisone 4mg pastilles (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12558411000001102','Hydrocortisone 7mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12554011000001102','Hydrocortisone 6mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','28044611000001103','Hydrocortisone 4mg pastilles (Special Order) 1 pastille (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15524211000001106','Hydrocortisone 2.5mg capsules (Special Order) 1 capsule (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8531111000001107','Hydrocortisone 25mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15524111000001100','Hydrocortisone 2.5mg capsules (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8533211000001102','Hydrocortisone 20mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12551911000001101','Hydrocortisone 30mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12556011000001107','Hydrocortisone 75mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12529611000001109','Hydrocortisone 18mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','12553411000001108','Hydrocortisone 50mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','12551311000001102','Hydrocortisone 3.6mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','12530211000001103','Hydrocortisone 2.6mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','12528511000001104','Hydrocortisone 1.5mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','12555711000001101','Hydrocortisone 7.5mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','12528911000001106','Hydrocortisone 100mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','8532411000001107','Hydrocortisone 2.5mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','12553711000001102','Hydrocortisone 6.25mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','12551611000001107','Hydrocortisone 3.75mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','8533511000001104','Hydrocortisone 12.5mg/5ml oral suspension (Special Order)','1','20210708'),
            ('Corticosteroid','SNOMED','28418811000001105','Hydrocortisone 5mg/5ml oral solution (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12552511000001100','Hydrocortisone 4.25mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12529311000001104','Hydrocortisone 13.95mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','16017911000001100','Hydrocortisone 1mg suppositories (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12531111000001103','Hydrocortisone 2mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12552911000001107','Hydrocortisone 4mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12558511000001103','Hydrocortisone 7mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12552311000001106','Hydrocortisone 3mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12530011000001108','Hydrocortisone 1mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12554111000001101','Hydrocortisone 6mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12552011000001108','Hydrocortisone 30mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12553511000001107','Hydrocortisone 50mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12556111000001108','Hydrocortisone 75mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12529711000001100','Hydrocortisone 18mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8531311000001109','Hydrocortisone 25mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8533611000001100','Hydrocortisone 20mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','8532511000001106','Hydrocortisone 2.5mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12551411000001109','Hydrocortisone 3.6mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12528611000001100','Hydrocortisone 1.5mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12529111000001101','Hydrocortisone 100mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12530311000001106','Hydrocortisone 2.6mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12555811000001109','Hydrocortisone 7.5mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','28418711000001102','Hydrocortisone 5mg/5ml oral solution (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12551711000001103','Hydrocortisone 3.75mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','12553811000001105','Hydrocortisone 6.25mg/5ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','373982003','Product containing precisely hydrocortisone 5 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','785096006','Product containing precisely hydrocortisone 2 milligram/1 milliliter conventional release oral suspension (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','6215001000027100','Cortelan 25 mg tablet (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','6255001000027100','CORTISTAB tablets 25mg [WAYMADE]  (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','6245001000027100','CORTISTAB tablets 5mg [WAYMADE]  (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','6285001000027100','Cortisyl 25mg Abentis (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','9230701000001107','Calcort (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3756411000001107','Calcort 6mg tablets (Sanofi)','1','20210708'),
            ('Corticosteroid','SNOMED','30020511000001109','Calcort 6mg tablets (Lexon (UK) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30269811000001102','Calcort 6mg tablets (Waymade Healthcare Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30020611000001108','Calcort 6mg tablets (Lexon (UK) Ltd) 60 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30020511000001109','Calcort 6mg tablets (Lexon (UK) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3850711000001101','Calcort 1mg tablets (Shire Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3756611000001105','Calcort 6mg tablets (Shire Pharmaceuticals Ltd) 60 tablet 6 x 10 tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30269911000001107','Calcort 6mg tablets (Waymade Healthcare Plc) 60 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30269811000001102','Calcort 6mg tablets (Waymade Healthcare Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30020611000001108','Calcort 6mg tablets (Lexon (UK) Ltd) 60 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3850711000001101','Calcort 1mg tablets (Shire Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3756411000001107','Calcort 6mg tablets (Shire Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3960211000001106','Calcort 30mg tablets (Shire Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30269911000001107','Calcort 6mg tablets (Waymade Healthcare Plc) 60 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3960411000001105','Calcort 30mg tablets (Shire Pharmaceuticals Ltd) 30 tablet 3 x 10 tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3850911000001104','Calcort 1mg tablets (Shire Pharmaceuticals Ltd) 100 tablet 10 x 10 tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3756611000001105','Calcort 6mg tablets (Shire Pharmaceuticals Ltd) 60 tablet 6 x 10 tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3960411000001105','Calcort 30mg tablets (Shire Pharmaceuticals Ltd) 30 tablet 3 x 10 tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3850911000001104','Calcort 1mg tablets (Shire Pharmaceuticals Ltd) 100 tablet 10 x 10 tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','116578002','Product containing deflazacort (medicinal product)','1','20210708'),
            ('Corticosteroid','SNOMED','325476008','Product containing precisely deflazacort 1 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325472005','Product containing precisely deflazacort 6 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325474006','Product containing precisely deflazacort 30 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325476008','Product containing precisely deflazacort 1 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325472005','Product containing precisely deflazacort 6 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325474006','Product containing precisely deflazacort 30 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','15449711000001109','Deflazacort 1mg tablets 1 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3756211000001108','Deflazacort 6mg tablets 60 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3960011000001101','Deflazacort 30mg tablets 30 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3850611000001105','Deflazacort 1mg tablets 100 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15449611000001100','Deflazacort 1mg tablets (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','33037911000001102','Deflazacort 22.75mg/1ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','33037911000001102','Deflazacort 22.75mg/1ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15449711000001109','Deflazacort 1mg tablets 1 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','778928005','Product containing only deflazacort in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','3756211000001108','Deflazacort 6mg tablets 60 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3850611000001105','Deflazacort 1mg tablets 100 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32989411000001106','Deflazacort 22.75mg/1ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3960011000001101','Deflazacort 30mg tablets 30 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15449811000001101','Deflazacort 1mg tablets (Special Order) 1 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','767955000','Product containing deflazacort in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','15449611000001100','Deflazacort 1mg tablets (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','33037911000001102','Deflazacort 22.75mg/1ml oral suspension (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32989411000001106','Deflazacort 22.75mg/1ml oral suspension 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32989611000001109','Deflazacort 22.75mg/1ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15449811000001101','Deflazacort 1mg tablets (Special Order) 1 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32989711000001100','Deflazacort 22.75mg/1ml oral suspension (Special Order) 1 ml','1','20210708'),
            ('Corticosteroid','SNOMED','32989611000001109','Deflazacort 22.75mg/1ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32989711000001100','Deflazacort 22.75mg/1ml oral suspension (Special Order) 1 ml (product)','1','20210708'),
            ('Corticosteroid','SNOMED','767955000','Product containing deflazacort in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','778928005','Product containing only deflazacort in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','325476008','Product containing precisely deflazacort 1 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325472005','Product containing precisely deflazacort 6 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325474006','Product containing precisely deflazacort 30 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','9254701000001101','Decadron (product)','1','20210708'),
            ('Corticosteroid','SNOMED','39013111000001100','Decadron 4mg tablets (CST Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','39013311000001103','Decadron 500microgram tablets (CST Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','39013211000001106','Decadron 4mg tablets (CST Pharma Ltd) 50 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3286911000001103','Decadron 500microgram tablets (Organon Pharma (UK) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','39013411000001105','Decadron 500microgram tablets (CST Pharma Ltd) 30 tablet (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3287311000001101','Decadron 500microgram tablets (Organon Pharma (UK) Ltd) 30 tablet 3 x 10 tablets (product)','1','20210708'),
            ('Corticosteroid','SNOMED','778957009','Product containing only dexamethasone in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','766473001','Product containing precisely dexamethasone (as dexamethasone sodium phosphate) 400 microgram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','412334007','Product containing precisely dexamethasone 1.5 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325365005','Product containing precisely dexamethasone 100 microgram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','8433011000001100','Dexamethasone 500micrograms/5ml oral solution (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325356003','Product containing precisely dexamethasone 2 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','3290311000001101','Dexamethasone 2mg tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','33634511000001106','Dexamethasone 2mg tablets (Accord Healthcare Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17967911000001107','Dexamethasone 2mg tablets (Almus Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','34045511000001104','Dexamethasone 2mg tablets (Aspire Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15628711000001100','Dexamethasone 2mg tablets (Auden McKenzie (Pharma Division) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','23968511000001105','Dexamethasone 2mg tablets (DE Pharmaceuticals) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32404811000001106','Dexamethasone 2mg tablets (Kent Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37221211000001103','Dexamethasone 2mg tablets (Martindale Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30809411000001107','Dexamethasone 2mg tablets (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','38867211000001104','Dexamethasone 2mg tablets (Medihealth (Northern) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3291411000001104','Dexamethasone 2mg tablets (Organon Laboratories Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17890811000001109','Dexamethasone 2mg tablets (Phoenix Healthcare Distribution Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15104911000001100','Dexamethasone 2mg tablets (Sigma Pharmaceuticals Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3292011000001100','Dexamethasone 2mg tablets (Unichem Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','21925111000001105','Dexamethasone 2mg tablets (Waymade Healthcare Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','412333001','Product containing precisely dexamethasone 250 microgram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','376688006','Product containing precisely dexamethasone 4 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','33557411000001107','Dexamethasone 4mg tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37628711000001102','Dexamethasone 4mg tablets (Advanz Pharma) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','34464111000001106','Dexamethasone 4mg tablets (Alliance Healthcare (Distribution) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','33497811000001107','Dexamethasone 4mg tablets (Consilient Health Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37743911000001104','Dexamethasone 4mg tablets (CST Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37236811000001109','Dexamethasone 4mg tablets (DE Pharmaceuticals) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','38867411000001100','Dexamethasone 4mg tablets (Medihealth (Northern) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','35560411000001105','Dexamethasone 4mg tablets (Teva UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325355004','Product containing precisely dexamethasone 500 microgram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','3286511000001105','Dexamethasone 500microgram tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','38039211000001104','Dexamethasone 500microgram tablets (Actavis UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32064711000001105','Dexamethasone 500microgram tablets (Aspire Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','24672711000001108','Dexamethasone 500microgram tablets (Auden McKenzie (Pharma Division) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','35477511000001105','Dexamethasone 500microgram tablets (Consilient Health Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37690211000001104','Dexamethasone 500microgram tablets (CST Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30047811000001109','Dexamethasone 500microgram tablets (DE Pharmaceuticals) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15245611000001104','Dexamethasone 500microgram tablets (Essential Generics Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37220711000001102','Dexamethasone 500microgram tablets (Martindale Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37393311000001107','Dexamethasone 500microgram tablets (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','39299011000001106','Dexamethasone 500microgram tablets (Medihealth (Northern) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3287811000001105','Dexamethasone 500microgram tablets (Organon Laboratories Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','29803711000001101','Dexamethasone 500microgram tablets (Sigma Pharmaceuticals Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','3288811000001109','Dexamethasone 500microgram tablets (Unichem Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','412335008','Product containing precisely dexamethasone 6 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','376667002','Product containing precisely dexamethasone 750 microgram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','350396002','Product containing dexamethasone in oral dose form (medicinal product form),','1','20210708'),
            ('Corticosteroid','SNOMED','766473001','Product containing precisely dexamethasone (as dexamethasone sodium phosphate) 400 microgram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','412334007','Product containing precisely dexamethasone 1.5 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','8433011000001100','Dexamethasone 500micrograms/5ml oral solution (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','412333001','Product containing precisely dexamethasone 250 microgram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','412335008','Product containing precisely dexamethasone 6 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','376667002','Product containing precisely dexamethasone 750 microgram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','41195001000027100','ORADEXON-ORGANON tablets 2mg [ORGANON] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','3851611000001100','DEXSOL oral solution 2mg/5ml [ROSEMONT] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','14365001000027100','HYDROCORTISTAB tablets 20mg [WAYMADE] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','4052211000001100','Medrone 100mg (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','5051911000001100','Medrone 16 mg(from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','4051211000001100','Medrone 2 mg (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','4051611000001100','Medrone 4 mg(from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','459611000001100','DELTACORTRIL ENTERIC tablets 2.5mg [ALLIANCE] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','331111000001100','DELTACORTRIL ENTERIC tablets 5mg [ALLIANCE] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','47605001000027100','DELTASTAB tablets 1mg [WAYMADE] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','47615001000027100','DELTASTAB tablets 5mg [WAYMADE] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','79935001000027100','PRECORTISYL FORTE tablets 25mg [AVENTIS] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','25615001000027100','PRECORTISYL tablets 1mg [HOECHSTMAR] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','25625001000027100','PRECORTISYL tablets 5mg [HOECHSTMAR] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','780280003','Product containing only prednisolone in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','374291003','Product containing precisely prednisolone (as prednisolone sodium phosphate) 1 milligram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','374290002','Product containing precisely prednisolone (as prednisolone sodium phosphate) 3 milligram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','416646003','Product containing precisely prednisolone (as prednisolone sodium phosphate) 5 milligram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325426006','Product containing precisely prednisolone 1 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','649711000001109','Prednisolone 1mg tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','646211000001106','Prednisolone 1mg tablets (Accord Healthcare Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','9807711000001108','Prednisolone 1mg tablets (Almus Pharmaceutical Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','238511000001107','Prednisolone 1mg tablets (Approved Prescription Services) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','10409611000001103','Prednisolone 1mg tablets (Arrow Generics Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','844111000001103','Prednisolone 1mg tablets (C P Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','18285011000001109','Prednisolone 1mg tablets (Co-Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30114811000001104','Prednisolone 1mg tablets (DE Pharmaceuticals) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','13245311000001104','Prednisolone 1mg tablets (Dowelhurst Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32807911000001101','Prednisolone 1mg tablets (Genesis Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','707911000001100','Prednisolone 1mg tablets (Kent Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','14786411000001103','Prednisolone 1mg tablets (LPC Medical (UK) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30858911000001100','Prednisolone 1mg tablets (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','39169011000001106','Prednisolone 1mg tablets (Medihealth (Northern) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15175511000001102','Prednisolone 1mg tablets (Sigma Pharmaceuticals Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','798611000001108','Prednisolone 1mg tablets (The Boots Company) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','52911000001107','Prednisolone 1mg tablets (Unichem Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','21851211000001107','Prednisolone 1mg tablets (Waymade Healthcare Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','432224007','Product containing precisely prednisolone 1 milligram/1 milliliter conventional release oral suspension (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','13120111000001109','Prednisolone 5mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','429995001','Product containing precisely prednisolone 2 milligram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','13054711000001107','Prednisolone 10mg/5ml oral solution (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325442004','Product containing precisely prednisolone 2.5 milligram/1 each gastro-resistant oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','459611000001100','Deltacortril 2.5mg gastro-resistant tablets (Phoenix Labs Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','22519711000001105','Dilacort 2.5mg gastro-resistant tablets (Auden McKenzie (Pharma Division) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32652311000001103','Dilacort 2.5mg gastro-resistant tablets (Crescent Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','33577711000001104','Dilacort 2.5mg gastro-resistant tablets (Teva UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','858811000001104','Prednisolone 2.5mg gastro-resistant tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','380011000001100','Prednisolone 2.5mg gastro-resistant tablets (Accord Healthcare Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','19743011000001109','Prednisolone 2.5mg gastro-resistant tablets (Almus Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','4208511000001105','Prednisolone 2.5mg gastro-resistant tablets (Approved Prescription Services) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','36568211000001107','Prednisolone 2.5mg gastro-resistant tablets (Bristol Laboratories Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30114011000001105','Prednisolone 2.5mg gastro-resistant tablets (DE Pharmaceuticals) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','13245711000001100','Prednisolone 2.5mg gastro-resistant tablets (Dowelhurst Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','685311000001104','Prednisolone 2.5mg gastro-resistant tablets (Kent Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37130211000001109','Prednisolone 2.5mg gastro-resistant tablets (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','39300111000001105','Prednisolone 2.5mg gastro-resistant tablets (Medihealth (Northern) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17916411000001100','Prednisolone 2.5mg gastro-resistant tablets (Phoenix Healthcare Distribution Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17999311000001104','Prednisolone 2.5mg gastro-resistant tablets (Phoenix Labs Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15175711000001107','Prednisolone 2.5mg gastro-resistant tablets (Sigma Pharmaceuticals Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','18307011000001109','Prednisolone 2.5mg gastro-resistant tablets (Teva UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','779211000001109','Prednisolone 2.5mg gastro-resistant tablets (Unichem Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','22452711000001102','Prednisolone 2.5mg gastro-resistant tablets (Waymade Healthcare Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325450008','Product containing precisely prednisolone 25 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','28799811000001108','Pevanti 25mg tablets (AMCo) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','716111000001103','Prednisolone 25mg tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30991311000001108','Prednisolone 25mg tablets (Accord Healthcare Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37778711000001106','Prednisolone 25mg tablets (DE Pharmaceuticals) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37131011000001108','Prednisolone 25mg tablets (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','39180111000001100','Prednisolone 25mg tablets (Medihealth (Northern) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17917211000001102','Prednisolone 25mg tablets (Phoenix Healthcare Distribution Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','313911000001107','Prednisolone 25mg tablets (Unichem Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','4852811000001108','Prednisolone 25mg tablets (Zentiva Pharma UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','416533002','Product containing precisely prednisolone 3 milligram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','13055911000001108','Prednisolone 15mg/5ml oral solution (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','432225008','Product containing precisely prednisolone 3 milligram/1 milliliter conventional release oral suspension (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','13055311000001107','Prednisolone 15mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325427002','Product containing precisely prednisolone 5 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','28799011000001102','Pevanti 5mg tablets (AMCo) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','876911000001107','Prednisolone 5mg tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','85511000001101','Prednisolone 5mg tablets (Accord Healthcare Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','9807911000001105','Prednisolone 5mg tablets (Almus Pharmaceutical Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','110811000001100','Prednisolone 5mg tablets (Approved Prescription Services) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','10410111000001103','Prednisolone 5mg tablets (Arrow Generics Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','940311000001100','Prednisolone 5mg tablets (C P Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','18285211000001104','Prednisolone 5mg tablets (Co-Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30115011000001109','Prednisolone 5mg tablets (DE Pharmaceuticals) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','13245511000001105','Prednisolone 5mg tablets (Dowelhurst Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32808311000001101','Prednisolone 5mg tablets (Genesis Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','350611000001104','Prednisolone 5mg tablets (Kent Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','14786811000001101','Prednisolone 5mg tablets (LPC Medical (UK) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30859111000001105','Prednisolone 5mg tablets (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','38721711000001107','Prednisolone 5mg tablets (NorthStar Healthcare Unlimited Company) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','18625211000001107','Prednisolone 5mg tablets (Phoenix Healthcare Distribution Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','86711000001103','Prednisolone 5mg tablets (The Boots Company) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','772111000001107','Prednisolone 5mg tablets (Unichem Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','21851411000001106','Prednisolone 5mg tablets (Waymade Healthcare Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325443009','Product containing precisely prednisolone 5 milligram/1 each gastro-resistant oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','331111000001100','Deltacortril 5mg gastro-resistant tablets (Phoenix Labs Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','22520311000001107','Dilacort 5mg gastro-resistant tablets (Auden McKenzie (Pharma Division) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','32652511000001109','Dilacort 5mg gastro-resistant tablets (Crescent Pharma Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','33577911000001102','Dilacort 5mg gastro-resistant tablets (Teva UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','662011000001105','Prednisolone 5mg gastro-resistant tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','512811000001108','Prednisolone 5mg gastro-resistant tablets (Accord Healthcare Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','19743211000001104','Prednisolone 5mg gastro-resistant tablets (Almus Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','4208911000001103','Prednisolone 5mg gastro-resistant tablets (Approved Prescription Services) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','34444511000001106','Prednisolone 5mg gastro-resistant tablets (Bristol Laboratories Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','30114311000001108','Prednisolone 5mg gastro-resistant tablets (DE Pharmaceuticals) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','13245911000001103','Prednisolone 5mg gastro-resistant tablets (Dowelhurst Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','447211000001105','Prednisolone 5mg gastro-resistant tablets (Kent Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','37130611000001106','Prednisolone 5mg gastro-resistant tablets (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','39204911000001106','Prednisolone 5mg gastro-resistant tablets (Medihealth (Northern) Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17916811000001103','Prednisolone 5mg gastro-resistant tablets (Phoenix Healthcare Distribution Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17999611000001109','Prednisolone 5mg gastro-resistant tablets (Phoenix Labs Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','15176111000001100','Prednisolone 5mg gastro-resistant tablets (Sigma Pharmaceuticals Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','18307211000001104','Prednisolone 5mg gastro-resistant tablets (Teva UK Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','255211000001101','Prednisolone 5mg gastro-resistant tablets (Unichem Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','22453011000001108','Prednisolone 5mg gastro-resistant tablets (Waymade Healthcare Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325445002','Product containing precisely prednisolone 50 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','4752611000001107','Prednisolone 50mg tablets (A A H Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','4752811000001106','Prednisolone 50mg tablets (Approved Prescription Services) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','780280003','Product containing only prednisolone in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','349354003','Product containing prednisolone in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','374291003','Product containing precisely prednisolone (as prednisolone sodium phosphate) 1 milligram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','374290002','Product containing precisely prednisolone (as prednisolone sodium phosphate) 3 milligram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','416646003','Product containing precisely prednisolone (as prednisolone sodium phosphate) 5 milligram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','432224007','Product containing precisely prednisolone 1 milligram/1 milliliter conventional release oral suspension (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','13120111000001109','Prednisolone 5mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','429995001','Product containing precisely prednisolone 2 milligram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','13054711000001107','Prednisolone 10mg/5ml oral solution (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','416533002','Product containing precisely prednisolone 3 milligram/1 milliliter conventional release oral solution (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','13055911000001108','Prednisolone 15mg/5ml oral solution (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','432225008','Product containing precisely prednisolone 3 milligram/1 milliliter conventional release oral suspension (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','13055311000001107','Prednisolone 15mg/5ml oral suspension (Special Order) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','779866002','Methylprednisolone only product in oral dose form','1','20210708'),
            ('Corticosteroid','SNOMED','325410005','Product containing precisely methylprednisolone 100 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','18592011000001106','Medrone 100mg tablets (Doncaster Pharmaceuticals Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17516611000001107','Medrone 100mg tablets (Mawdsley-Brooks & Company Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','36815111000001106','Medrone 100mg tablets (Originalis B.V.) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','4052211000001108','Medrone 100mg tablets (Pfizer Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','17570011000001108','Medrone 100mg tablets (Sigma Pharmaceuticals Plc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325413007','Product containing precisely methylprednisolone 16 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','4051911000001105','Medrone 16mg tablets (Pfizer Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325411009','Product containing precisely methylprednisolone 2 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','4051211000001101','Medrone 2mg tablets (Pfizer Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','325412002','Product containing precisely methylprednisolone 4 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','4051611000001104','Medrone 4mg tablets (Pfizer Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','350449009','Methylprednisolone-containing product in oral dose form','1','20210708'),
            ('Corticosteroid','SNOMED','779866002','Product containing only methylprednisolone in oral dose form (medicinal product form)','1','20210708'),
            ('Corticosteroid','SNOMED','325413007','Product containing precisely methylprednisolone 16 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','4051911000001105','Medrone 16mg tablets (Pfizer Ltd) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','25675001000027100','PREDNESOL tablets 5mg [SOVEREIGN] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','28475001000027100','SINTISONE tablets [PHARMACIA] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','6935001000027100','DECORTISYL tablets 5mg [ROUSSEL] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','17873311000001100','LODOTRA modified release tablet 2mg [NAPPPHARM] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','17873911000001100','LODOTRA modified release tablet 5mg [NAPPPHARM] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','780283001','Prednisone only product in oral dose form','1','20210708'),
            ('Corticosteroid','SNOMED','418349006','Product containing precisely prednisone 1 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','11386511000001109','Decortin 1mg tablets (Merck & Co. Inc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','374058000','Product containing precisely prednisone 10 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','373989007','Product containing precisely prednisone 2.5 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','325456002','Product containing precisely prednisone 20 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','373994007','Product containing precisely prednisone 5 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','11387011000001103','Decortin 5mg tablets (Merck & Co. Inc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','374072009','Product containing precisely prednisone 50 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','11387311000001100','Decortin 50mg tablets (Merck & Co. Inc) (product)','1','20210708'),
            ('Corticosteroid','SNOMED','768296006','Prednisone-containing product in oral dose form','1','20210708'),
            ('Corticosteroid','SNOMED','53555001000027100','LEDERCORT tablets 2mg [WYETH PHAR] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','53565001000027100','LEDERCORT tablets 4mg [WYETH PHAR] (from CPRD product.txt)','1','20210708'),
            ('Corticosteroid','SNOMED','780692007','Triamcinolone only product in oral dose form','1','20210708'),
            ('Corticosteroid','SNOMED','374400005','Product containing precisely triamcinolone 4 milligram/1 each conventional release oral tablet (clinical drug)','1','20210708'),
            ('Corticosteroid','SNOMED','350463003','Triamcinolone-containing product in oral dose form','1','20210708')]
    pheno_extractor.add_codes_to_masterlist(vals)




