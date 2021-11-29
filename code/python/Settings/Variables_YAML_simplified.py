# Databricks notebook source
# List of all variable and object names used in notebooks of CCU003_03
# The key of each key:value is the name of a widget in Databricks.
# To access the value of each key:value, use one of the following options
# a) dbutils.widgets.get(key)
# b) Function gw(key) or get_wgt(key) at the start of each notebook.
# Note: always run dbutils.widgets.removeAll() before loading widgets.

import yaml

global_vars = """
###################
#### Databases ####
###################
db_raw: dars_nic_391419_j3w9t
db_collab:  dars_nic_391419_j3w9t_collab
####################
#### Raw tables ####
####################
gdppr_raw: gdppr_dars_nic_391419_j3w9t
sgss_raw: sgss_dars_nic_391419_j3w9t
pillar2_raw: pillar_2_dars_nic_391419_j3w9t
chess_raw: chess_dars_nic_391419_j3w9t
deaths_raw: deaths_dars_nic_391419_j3w9t
primary_care_meds_raw: primary_care_meds_dars_nic_391419_j3w9t
sus_raw: sus_dars_nic_391419_j3w9t
hes_ons_raw: hes_ons_dars_nic_391419_j3w9t # ignore this table for now. use deaths instead
vaccine_status_raw: vaccine_status_dars_nic_391419_j3w9t
#########################
#### table snapshots ####
#########################
# In February-apr 2021
old:
  feb21:
    gdppr_freeze: ccu003_mm_gdppr_180221_dars_nic_391419_j3w9t
    sgss_freeze: ccu003_mm_sgss_180221_dars_nic_391419_j3w9t
    chess_freeze: ccu003_mm_chess_180221_dars_nic_391419_j3w9t
    deaths_freeze: ccu003_mm_deaths_180221_dars_nic_391419_j3w9t
    primary_care_meds_freeze: ccu003_mm_primary_care_meds_180221_dars_nic_391419_j3w9t
    sus_freeze: ccu003_mm_sus_180221_dars_nic_391419_j3w9t
    hes_ae_all_freeze: ccu003_mm_hes_ae_190221_dars_nic_391419_j3w9t
    hes_apc_all_freeze: ccu003_mm_hes_apc_190221_dars_nic_391419_j3w9t
    hes_cc_all_freeze: ccu003_mm_hes_cc_190221_dars_nic_391419_j3w9t
    ons_all_freeze: ccu003_mm_hes_ons_180221_dars_nic_391419_j3w9t
    hes_op_all_freeze: ccu003_mm_hes_op_190221_dars_nic_391419_j3w9t
    pillar2_: ccu003_mm_pilar2_150421_dars_nic_391419_j3w9t
  apr21:
  # in April 2021
    gdppr_freeze: ccu003_mm_gdppr_frzon280421_dars_nic_391419_j3w9t
    sgss_freeze: ccu003_mm_sgss_frzon280421_dars_nic_391419_j3w9t
    chess_freeze: ccu003_mm_chess_frzon280421_dars_nic_391419_j3w9t
    deaths_freeze: ccu003_mm_deaths_frzon280421_dars_nic_391419_j3w9t
    primary_care_meds_freeze: ccu003_mm_primary_care_meds_frzon280421_dars_nic_391419_j3w9t
    sus_freeze: ccu003_mm_sus_frzon280421_dars_nic_391419_j3w9t
    hes_ae_all_freeze: ccu003_mm_hes_ae_frzon280421_dars_nic_391419_j3w9t
    hes_apc_all_freeze: ccu003_mm_hes_apc_frzon280421_dars_nic_391419_j3w9t
    hes_cc_all_freeze: ccu003_mm_hes_cc_frzon280421_dars_nic_391419_j3w9t
    ons_all_freeze: ccu003_mm_hes_ons_frzon280421_dars_nic_391419_j3w9t
    hes_op_all_freeze: ccu003_mm_hes_op_frzon280421_dars_nic_391419_j3w9t
    pillar2_freeze: ccu003_mm_pillar2_frzon280421_dars_nic_391419_j3w9t
# May 2021cut
gdppr_freeze: ccu003_direfcts_dataprep_1_gdppr_frzon28may_mm_210528 
sgss_freeze: ccu003_direfcts_dataprep_1_sgss_frzon28may_mm_210528 
chess_freeze: ccu003_direfcts_dataprep_1_chess_frzon28may_mm_210528 
deaths_freeze: ccu003_direfcts_dataprep_1_deaths_frzon28may_mm_210528 
primary_care_meds_freeze: ccu003_direfcts_dataprep_1_primary_care_meds_frzon28may_mm_210528 
sus_freeze: ccu003_direfcts_dataprep_1_sus_frzon28may_mm_210528 
hes_ae_all_freeze: ccu003_direfcts_dataprep_1_ae_all_frzon28may_mm_210528 
hes_apc_all_freeze: ccu003_direfcts_dataprep_1_apc_all_frzon28may_mm_210528 
hes_cc_all_freeze: ccu003_direfcts_dataprep_1_cc_all_frzon28may_mm_210528 
ons_all_freeze: ccu003_direfcts_dataprep_1_ons_all_frzon28may_mm_210528 
hes_op_all_freeze: ccu003_direfcts_dataprep_1_op_all_frzon28may_mm_210528 
pillar2_freeze: ccu003_direfcts_dataprep_1_pillar2_frzon28may_mm_210528 
# November 2021 cut
vaccine_status_freeze: ccu003_direfcts_dataprep_1_vaccine_status_frozen10nov_mm_221011
#################
#### ID cols ####
#################
key_id: NHS_NUMBER_DEID
gdppr_id: NHS_NUMBER_DEID
sgss_id: PERSON_ID_DEID
chess_id: PERSON_ID_DEID
deaths_id: DEC_CONF_NHS_NUMBER_CLEAN_DEID
ae_id: PERSON_ID_DEID
apc_id: PERSON_ID_DEID
aekey_id: AEKEY
epikey_id: EPIKEY
cc_id: PERSON_ID_DEID
op_id: PERSON_ID_DEID
####################
#### Key tables ####
####################
core_key_beta: ccu003_direfcts_dataprep_09_core_key_beta_mm_210528 # key table for ids from gdppr, sgss, chess, deaths, hes ae, hes apc, cc, and op
core_demog_beta: ccu003_direfcts_dataprep_09_core_demographic_mm_210528 # key table for core demographic feataures
supp_demog_beta: ccu003_direfcts_dataprep_09_supp_demographic_mm_210528 # key table with supplementary demographic features for feature engineering and quality control 
heskey_ae_chkpnt: ccu003_direfcts_dataprep_05_heskey_ae_chekpoint_5_mm_210528 # one id per patient in hes ae
heskey_apc_chkpnt: ccu003_direfcts_dataprep_06_heskey_apc_chekpoint_6_mm_210528 # one id per patient in hes apc
heskey_cc_chkpnt: ccu003_direfcts_dataprep_07_heskey_cc_chekpoint_7_mm_210528 # one id per patient in hes cc
heskey_op_chkpnt: ccu003_direfcts_dataprep_08_heskey_op_chekpoint_8_mm_210528 # one id per patient in hes op
#####################################
#### Re-used from other projects ####
#####################################
# Vanexia's codelist for COVID
from_001_codelist: ccu001_01_codelists
from_ccu0013_covid_trajectory: ccu013_covid_trajectory
##############################
####### Processed tables  ####
##############################
cohort_table: test
codelist_001: ccu003_direfcts_03_codelist_001_mm_210611 # will be used to create covid table based on ccu001 
covid_table_001: ccu003_direfcts_03_covid19_001_mm_210611 # based on ccu001
old_covid:
  covid_ccu013_trajectory_snapshot: ccu003_mm_ccu013_covid_trajectory_snapshot_15May2021
covid_ccu013_trajectory_snapshot: ccu003_direfcts_03_ccu013_covid_trajectory_snapshot_210622
covid_table_basedon_013: ccu003_direfcts_03_covid19_basedon_013_mm_210622 # based on ccu013
cvd_sub_pheno_table: ccu003_direfcts_05_cvd_sub_pheno_table_mm_210623
comorbidity_pheno_table: ccu003_direfcts_06_multimobidity_mm_210624
######################
#### Global views ####
######################
gvw: ccu003_mm_global_view
######################
####  Checkpoints ####
######################
key_chkpnt_1: ccu003_direfcts_dataprep_02_key_chkpnt_1_mm_210528 # contains results of gdppr
key_chkpnt_2: ccu003_direfcts_dataprep_03_key_chkpnt_2_mm_210528 # contains resutls of  sgss(and gdppr)
key_chkpnt_3: ccu003_direfcts_dataprep_03_key_chkpnt_3_mm_210528 # contains resutls of chess (sgss, gdppr)
key_chkpnt_4: ccu003_direfcts_dataprep_04_key_chkpnt_4_mm_210528 # contains resutls of death (chess, sgss, gdppr)
key_chkpnt_5: ccu003_direfcts_dataprep_05_key_chkpnt_5_mm_210528 # contains hes ae and deaths, chess, sgss, gdppr
key_chkpnt_6: ccu003_direfcts_dataprep_06_key_chkpnt_6_mm_210528 # contains hes apc and hes ae, deaths, chess, sgss, gdppr
key_chkpnt_7: ccu003_direfcts_dataprep_07_key_chkpnt_7_mm_210528 # contains hes cc and hes apc, hes ae, deaths, chess, sgss, gdppr
key_chkpnt_8: ccu003_direfcts_dataprep_08_key_chkpnt_8_mm_210528 # contains hes op and hes cc, hes apc, hes ae, deaths, chess, sgss, gdppr
rr_chkpnt_alpha: ccu003_direfcts_rr_chkpnt_alpha_mm_210528 # intermediate analysis table for Cvd and DM. This table includes key table and covid results
rr_chkpnt_beta: ccu003_direfcts_rr_chkpnt_beta_mm_210622 # intermediate analysis table+ sex (other variables will be added later)
rr_chkpnt_gamma: ccu003_direfcts_rr_chkpnt_gamma_mm_210622 # intermediate analysis table+ DM and CVD
rr_chkpnt_delta: ccu003_direfcts_rr_chkpnt_delta_mm_210622 # intermediate analysis table + other multimorbidities
rr_chkpnt_epsilon: ccu003_direfcts_rr_chkpnt_epsilon_mm_210622 # intermediate analysis table + endpoints
rr_chkpnt_zeta: ccu003_direfcts_rr_chkpnt_zeta_mm_210622 # intermediate analysis table + age and periods
rr_chkpnt_eta: ccu003_direfcts_rr_chkpnt_eta_mm_210622 # table with prevalence and incidence points and cohort_flag
lancet_chkpnt_a: ccu003_direfcts_lancet_basedon_eta_chkpnt_a_mm_210723 # table a for lancet validation study
lancet_chkpnt_b: ccu003_direfcts_lancet_basedon_eta_chkpnt_b_mm_210723 # table b for lancet validation study
r_temp_table: ccu003_direfcts_temp_rstudio_table_mm_210723 # to import selected columns to R
## cohort_flag indicates that cvd or dm phenotypes do not have an eventdate and are not suitable for cohort making
## the number of records without eventdate are too many to remove. To eliminate any data collection non-random confounder, the flag is added
# delete these later (temporary work)
temp_chkpnt_a: ccu003_direfcts_temp_chekpoint_mm_a
temp_chkpnt_b: ccu003_direfcts_temp_chekpoint_mm_b
temp_chkpnt_c: ccu003_direfcts_temp_chekpoint_mm_c
temp_chkpnt_d: ccu003_direfcts_temp_chekpoint_mm_d
temp_chkpnt_e: ccu003_direfcts_temp_chekpoint_mm_e
temp_chkpnt_f: ccu003_direfcts_temp_chekpoint_mm_f
temp_chkpnt_g: ccu003_direfcts_temp_chekpoint_mm_g
temp_chkpnt_h: ccu003_direfcts_temp_chekpoint_mm_h
temp_inter_notebook: ccu003_direfcts_temp_chekpoint_mm_inter_notebook #Workaround for global_view (cluster down or to another cluster, e.g. R on dae-tools)
########################
#### Time endpoints ####
########################
t_start_initial: '1940-08-01' # the minimum of death date in data. this will change to t_start 
t_start: '1986-09-01' #(birth would be more complex to find, so t_entry prior to t_start is allowed. t_zero is the ultimate starting point which is earlier than any birthdate)
t_cohort_start: '1978-01-01' # Study specific: 40 years prior to cutoff date. used when making prevalence and incidence cohorts to update t_entry and age_entry.
t_zero: '1887-01-01'
t_cutoff: '2018-01-01'
t_pandemic: '2020-01-01'
t_end: '2021-05-15'
#######################
### Notes
#######################
# age=-99 at a reference point indicates death before the reference point
# age=0 at a reference point indicates a birth date after the reference point
# age is relative to your study, therefore, you might find individuals less than 18 at a reference point. Manage these per study design.
# Individuals less than 18 relative to the ending point (i.e., the last data collection time) are excluded
# t_cohort_entry is the earliest of t_18 and t_cohort_start
# t_exit is the easrliest of t_death or t_end
"""
