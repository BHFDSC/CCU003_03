# Databricks notebook source
# MAGIC %md
# MAGIC # CCU003 Snapshot of tables

# COMMAND ----------

# MAGIC %md
# MAGIC | Item | Value |
# MAGIC |----- | ----- |
# MAGIC | **Description** | Creates a snapshot of tables based on the naming convention recommended by NHS Digital. |
# MAGIC | **Project(s)** | CCU003_03|
# MAGIC | **Author(s)** | Mehrdad Mizani (MM) | 
# MAGIC | **Reviewer(s)** | *ToDo*, To be reviewed |
# MAGIC | **Data input** | GDPPR table (refer to initial widgets), Deaths, HES-ONS, Vaccination |
# MAGIC | **Data output** | Refer to initial widgets |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update after each run
# MAGIC 
# MAGIC | Action | By | Date |
# MAGIC | ---- | ----- | ----- |
# MAGIC | Last updated | MM | 10.11.2021 |
# MAGIC | Last reviewed/tested | - | - |
# MAGIC | Last run | MM | 10.11.2021 |
# MAGIC 
# MAGIC ##### Notes:

# COMMAND ----------

# MAGIC %md
# MAGIC # Load helper functions and classes

# COMMAND ----------

/Workspaces/dars_nic_391419_j3w9t_collab/CCU003/Direct_effects_CVD_DM/Exports/HelperLibrary/function_set_1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set initial YAML parameters and widgets

# COMMAND ----------

/Workspaces/dars_nic_391419_j3w9t_collab/CCU003/Direct_effects_CVD_DM/Exports/HelperLibrary/Variables_YAML_simplified

# COMMAND ----------

gvars = yaml.load(global_vars, Loader=yaml.SafeLoader)
print(gvars.keys()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make a list of required variables in this notebook 
# MAGIC 
# MAGIC - **Re-run the previous two cells in case of any upsates to the YAML file**
# MAGIC -  **Re-run the next three cells in case of any changes to the selected vars list**

# COMMAND ----------

vars = ['db_raw', 'db_collab' , 'key_id',  'gvw', 'gdppr_raw', 'sgss_raw', 'pillar2_raw', 'chess_raw', 'deaths_raw', 'primary_care_meds_raw', 'sus_raw','hes_ons_raw', 'vaccine_status_raw', 'gdppr_freeze', 'sgss_freeze', 'chess_freeze','deaths_freeze','primary_care_meds_freeze', 'sus_freeze', 'hes_ae_all_freeze', 'hes_apc_all_freeze', 'hes_cc_all_freeze', 'ons_all_freeze','hes_op_all_freeze', 'pillar2_freeze' , 'vaccine_status_freeze']

# COMMAND ----------

dbutils.widgets.removeAll()


# COMMAND ----------

for key in vars:
    dbutils.widgets.text(key, gvars.get(key))


def get_wgt(widget_label):
    """Easier extraction of widget string value 
    """
    #todo  add exception
    return(dbutils.widgets.get(widget_label))

def gw(widget_label):
     return(get_wgt(widget_label))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test widgets

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM $db_raw.$gdppr_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN $db_collab;

# COMMAND ----------

# MAGIC %md
# MAGIC # GDPPR on 28/05/2021

# COMMAND ----------

print(gw("db_raw"))
print(gw("db_collab"))
print(gw("gdppr_raw"))
print(gw("gdppr_freeze"))



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE $db_collab.$gdppr_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM $db_raw.$gdppr_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$gdppr_freeze OWNER TO $db_collab

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check the snapshot

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SELECT * FROM $db_collab.$gdppr_freeze limit 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Deaths on 28/05/2021

# COMMAND ----------

# MAGIC %sql 
# MAGIC --SELECT * FROM $db_raw.$deaths_raw limit 2

# COMMAND ----------

# Test widgets of the Deaths table
print(gw("db_raw"))
print(gw("db_collab"))
print(gw("deaths_raw"))
print(gw("deaths_freeze"))


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the deaths table
# MAGIC CREATE TABLE $db_collab.$deaths_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM $db_raw.$deaths_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$deaths_freeze OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM $db_collab.$deaths_freeze limit 3

# COMMAND ----------

# MAGIC %md
# MAGIC # HES_ONS on 28/05/2021

# COMMAND ----------

# Test widgets of the HES ONS table
print(gw("db_raw"))
print(gw("db_collab"))
print(gw("hes_ons_raw"))
print(gw("ons_all_freeze"))


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the hes_ons table
# MAGIC CREATE TABLE $db_collab.$ons_all_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM $db_raw.$hes_ons_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$ons_all_freeze OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM $db_collab.$ons_all_freeze LIMIT 2

# COMMAND ----------

# MAGIC %md
# MAGIC # CHESS on 28/05/2021

# COMMAND ----------

# Test widgets of the CHESS table
print(gw("db_raw"))
print(gw("db_collab"))
print(gw("chess_raw"))
print(gw("chess_freeze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the CHESS table
# MAGIC CREATE TABLE $db_collab.$chess_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM $db_raw.$chess_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$chess_freeze OWNER TO $db_collab

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM $db_collab.$chess_freeze  LIMIT 2

# COMMAND ----------

# MAGIC %md
# MAGIC # SGSS on 28/05/2021

# COMMAND ----------

# Test widgets of the SGSS table
print(gw("db_raw"))
print(gw("db_collab"))
print(gw("sgss_raw"))
print(gw("sgss_freeze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the SGSS table
# MAGIC CREATE TABLE $db_collab.$sgss_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM $db_raw.$sgss_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$sgss_freeze OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM $db_collab.$sgss_freeze LIMIT 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Pillar2 on 28/05/2021

# COMMAND ----------

print(gw("db_raw"))
print(gw("db_collab"))
print(gw("pillar2_raw"))
print(gw("pillar2_freeze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT *  FROM $db_raw.$pillar2_raw limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the pilar2 table
# MAGIC CREATE TABLE $db_collab.$pillar2_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM $db_raw.$pillar2_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$pillar2_freeze OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from $db_collab.$pillar2_freeze limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC # SUS on 28/05/2021

# COMMAND ----------

# Test widgets of the SUS table
print(gw("db_raw"))
print(gw("db_collab"))
print(gw("sus_raw"))
print(gw("sus_freeze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the SUS table
# MAGIC CREATE TABLE $db_collab.$sus_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM $db_raw.$sus_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$sus_freeze OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM $db_collab.$sus_freeze LIMIT 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Vaccination status on 10 Nov 2021

# COMMAND ----------

# Test widgets of the SUS table
print(gw("db_raw"))
print(gw("db_collab"))
print(gw("vaccine_status_raw"))
print(gw("vaccine_status_freeze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the vaccine sttatus table
# MAGIC CREATE TABLE $db_collab.$vaccine_status_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM $db_raw.$vaccine_status_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$vaccine_status_freeze OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %md
# MAGIC ## primary care meds on 28/05/2021
# MAGIC 
# MAGIC Good luck with the long name!

# COMMAND ----------

# Test widgets of the primary care meds table
print(gw("db_raw"))
print(gw("db_collab"))
print(gw("primary_care_meds_raw"))
print(gw("primary_care_meds_freeze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the primary care meds table. 
# MAGIC CREATE TABLE $db_collab.$primary_care_meds_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM $db_raw.$primary_care_meds_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$primary_care_meds_freeze OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM $db_collab.$primary_care_meds_freeze LIMIT 2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Make a single HES_APC view for a snapshot on 28/05/2021

# COMMAND ----------

import databricks.koalas as ks
ks.set_option('compute.default_index_type', 'distributed')

db_name = gw("db_raw")

table_list = ks.sql(f"SHOW TABLES FROM {db_name}").to_koalas()
#table_list


# COMMAND ----------

hes_apc_table_list = table_list[table_list.tableName.str.contains('hes_apc')]['tableName']
hes_apc_table_list = hes_apc_table_list.to_list()
hes_apc_table_list

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###### Todo: Optimisation for koalas based function
# MAGIC The one liner code from Sam's wrangling codes gives a new error not encountered previously. I changed the code to a for loop. The reason for using ks_holder variable is the dictionary change error is ks_all is used. The DAG of this operation looks scary but distributed indexing of koalas makes the operation fast enough. This will shuffle the records in each hes table (per financial year) due to non-deterministic indexing. Running head() might give different resutls but the records have not been changed. 
# MAGIC 
# MAGIC ```
# MAGIC [spark.sql(f"SELECT * FROM {database}.{table}{table_suffix}").to_koalas() for table in table_list]
# MAGIC ```

# COMMAND ----------

in_db_name = gw("db_raw")
ks.set_option('compute.default_index_type', 'distributed')

# Concat all tables in a single koalas dataframe

    
ks_all = union_ks_maker(in_db_name, hes_apc_table_list)

# COMMAND ----------

ks_all.head()

# COMMAND ----------

# test count
ks_all['PERSON_ID_DEID'].count()

# COMMAND ----------

#test of pyspark  function
#df_all = union_df_maker(in_db_name, hes_apc_table_list)

# COMMAND ----------

# Cumulative row counts of all tables in the list
row_count = 0
for table_name in hes_apc_table_list:
    row_count += spark.sql(f"select count(PERSON_ID_DEID) from {in_db_name}.{table_name}").collect()[0][0]

# COMMAND ----------

row_count
#OK

# COMMAND ----------

df_all =ks_all.to_spark()
df_all.createOrReplaceTempView("mm_hes_apc_all_tvw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mm_hes_apc_all_tvw limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- test count just to double check
# MAGIC select count(PERSON_ID_DEID) from mm_hes_apc_all_tvw

# COMMAND ----------

print(gw("db_collab"))
print(gw("hes_apc_all_freeze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the hes apc table from the temporary view 
# MAGIC CREATE TABLE $db_collab.$hes_apc_all_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM mm_hes_apc_all_tvw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$hes_apc_all_freeze OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM $db_collab.$hes_apc_all_freeze limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Make a single HES_OP view for a snapshot on 19/02/2021

# COMMAND ----------

import databricks.koalas as ks
ks.set_option('compute.default_index_type', 'distributed')

db_name = gw("db_raw")

table_list = ks.sql(f"SHOW TABLES FROM {db_name}").to_koalas()
table_list

# COMMAND ----------

hes_op_table_list = table_list[table_list.tableName.str.contains('hes_op')]['tableName']
hes_op_table_list = hes_op_table_list.to_list()
hes_op_table_list

# COMMAND ----------

in_db_name = gw("db_raw")

# Concat all tables in a single koalas dataframe

    
ks_all = union_ks_maker(in_db_name, hes_op_table_list)

# COMMAND ----------

# test count
ks_all['PERSON_ID_DEID'].count()

# COMMAND ----------

# Cumulative row counts of all tables in the list
row_count = 0
for table_name in hes_op_table_list:
    row_count += spark.sql(f"select count(PERSON_ID_DEID) from {in_db_name}.{table_name}").collect()[0][0]
print(row_count)

# COMMAND ----------

# Make a temp view
df_all =ks_all.to_spark()
df_all.createOrReplaceTempView("mm_hes_op_all_tvw")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- check one row
# MAGIC select * from mm_hes_op_all_tvw limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- test count just to double check
# MAGIC select count(PERSON_ID_DEID) from mm_hes_op_all_tvw

# COMMAND ----------

print(gw("db_collab"))
print(gw("hes_op_all_freeze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the hes op table from the temporary view 
# MAGIC CREATE TABLE $db_collab.$hes_op_all_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM mm_hes_op_all_tvw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$hes_op_all_freeze  OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM $db_collab.$hes_op_all_freeze limit 5


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Make a single HES_ae view for a snapshot on 28/05/2021

# COMMAND ----------

import databricks.koalas as ks
ks.set_option('compute.default_index_type', 'distributed')

db_name = gw("db_raw")

table_list = ks.sql(f"SHOW TABLES FROM {db_name}").to_koalas()
table_list

# COMMAND ----------

hes_ae_table_list = table_list[table_list.tableName.str.contains('hes_ae')]['tableName']
hes_ae_table_list = hes_ae_table_list.to_list()
hes_ae_table_list

# COMMAND ----------

in_db_name = gw("db_raw")

# Concat all tables in a single koalas dataframe

ks_all = union_ks_maker(in_db_name, hes_ae_table_list)

# COMMAND ----------

# test count
ks_all['PERSON_ID_DEID'].count()

# COMMAND ----------

# Cumulative row counts of all tables in the list
row_count = 0
for table_name in hes_ae_table_list:
    row_count += spark.sql(f"select count(PERSON_ID_DEID) from {in_db_name}.{table_name}").collect()[0][0]
print(row_count)

# COMMAND ----------

# Make a temp view
df_all =ks_all.to_spark()
df_all.createOrReplaceTempView("mm_hes_ae_all_tvw")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- check one row
# MAGIC select * from mm_hes_ae_all_tvw limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- test count just to double check
# MAGIC select count(PERSON_ID_DEID) from mm_hes_ae_all_tvw

# COMMAND ----------

print(gw("db_collab"))
print(gw("hes_ae_all_freeze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the hes ae table from the temporary view 
# MAGIC CREATE TABLE $db_collab.$hes_ae_all_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM mm_hes_ae_all_tvw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$hes_ae_all_freeze OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM $db_collab.$hes_ae_all_freeze limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Make a single HES_cc view for a snapshot on 28/05/2021

# COMMAND ----------

import databricks.koalas as ks
ks.set_option('compute.default_index_type', 'distributed')

db_name = gw("db_raw")

table_list = ks.sql(f"SHOW TABLES FROM {db_name}").to_koalas()
table_list

# COMMAND ----------

hes_cc_table_list = table_list[table_list.tableName.str.contains('hes_cc')]['tableName']
hes_cc_table_list = hes_cc_table_list.to_list()
hes_cc_table_list

# COMMAND ----------

in_db_name = gw("db_raw")

# Concat all tables in a single koalas dataframe

ks_all = union_ks_maker(in_db_name, hes_cc_table_list)

# COMMAND ----------

# test count
ks_all['PERSON_ID_DEID'].count()

# COMMAND ----------

# Cumulative row counts of all tables in the list
row_count = 0
for table_name in hes_cc_table_list:
    row_count += spark.sql(f"select count(PERSON_ID_DEID) from {in_db_name}.{table_name}").collect()[0][0]
print(row_count)

# COMMAND ----------

# Make a temp view
df_all =ks_all.to_spark()
df_all.createOrReplaceTempView("mm_hes_cc_all_tvw")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- check one row
# MAGIC select * from mm_hes_cc_all_tvw limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- count test
# MAGIC select count('PERSON_ID_DEID') from mm_hes_cc_all_tvw

# COMMAND ----------

print(gw("db_collab"))
print(gw("hes_cc_all_freeze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a snapshot of the hes cc table from the temporary view 
# MAGIC CREATE TABLE $db_collab.$hes_cc_all_freeze AS 
# MAGIC SELECT *, current_date() as production_date FROM mm_hes_cc_all_tvw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workaround to enable dropping the table. not guaranteed to work. 
# MAGIC ALTER TABLE $db_collab.$hes_cc_all_freeze OWNER TO $db_collab 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from $db_collab.$hes_cc_all_freeze limit 5

# COMMAND ----------


