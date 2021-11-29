# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Key table maker, from GDPPR
# MAGIC 
# MAGIC Use this notebook to make a function or object based code

# COMMAND ----------

# MAGIC %md
# MAGIC ## ** Do not overwrite final tables. ** 
# MAGIC 
# MAGIC **Notes**
# MAGIC - If you choose to uncomment any cache command, please unpersist the same df at the end of the notebook
# MAGIC - The spark actions and transformations are not optimised yet. 
# MAGIC 
# MAGIC | Item | Value |
# MAGIC |-------------- | --------- | 
# MAGIC | **Description** | To make a table containig one record per patient with key variabls (primary keys), age, sex, ethnicity, dead status, COVID-19 status, and other essential columns |
# MAGIC | **Project(s)** | CCU003_03 |
# MAGIC | **Authors (initial)** | Mehrdad Mizani (MM) |
# MAGIC | **Reviewers** | *ToDo*, To be reviewed |
# MAGIC | **Data input** | GDPPR|
# MAGIC | **Data output** | One global view or a stored table. Use this as an input for the next notebood /Workspaces/dars_nic_391419_j3w9t_collab/CCU003/Direct_effects_CVD_DM/Exports/1_Data_Preparation/03_RUN_2_key_table_maker_sgss_chess_new | 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update after each run
# MAGIC 
# MAGIC | Action | By | Date |
# MAGIC | ---- | ----- | ----- |
# MAGIC | Last updated | MM | 28.05.2021 |
# MAGIC | Last reviewed/tested | - | - |
# MAGIC | Last run | MM | 28.05.2021 |
# MAGIC 
# MAGIC ##### Notes:

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reference notebooks
# MAGIC - Skinny table logic: [Link](https://db.core.data.digital.nhs.uk/#notebook/1289766/command/1289767)

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU003/Direct_effects_CVD_DM/Exports/HelperLibrary/function_set_1

# COMMAND ----------

# MAGIC %md
# MAGIC # YAML initiallisation

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU003/Direct_effects_CVD_DM/Exports/HelperLibrary/Variables_YAML_simplified

# COMMAND ----------

gvars = yaml.load(global_vars, Loader=yaml.SafeLoader)


# COMMAND ----------

# MAGIC %md
# MAGIC # Make a list of required variables in this notebook

# COMMAND ----------

#select from this list and change the next list
print(gvars.keys())

# COMMAND ----------

vars = ['db_collab',  'gdppr_freeze', 'key_id', 'gdppr_id', 'gvw', 'key_chkpnt_1']

# COMMAND ----------

# MAGIC %md
# MAGIC # Update widgets after each change to the vars list

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
# MAGIC # Initialise Core key table from GDPPR snapshot  

# COMMAND ----------

# create a Spark dataframe
# note- put the whole query in single quote, put widget names in double-quotes. 
gdppr_df = spark.sql(f'SELECT * FROM {gw("db_collab")}.{gw("gdppr_freeze")}')

# COMMAND ----------

print(f'gdppr distinct count=  {gdppr_df.select(gw("key_id")).distinct().count()}')
print(f'gdppr total count=  {gdppr_df.count()}')

# COMMAND ----------

sorted(gdppr_df.columns)

# COMMAND ----------

# select columns
gdppr_df = gdppr_df.select(gw("gdppr_id"), \
                           "SEX", \
                           "YEAR_OF_BIRTH", \
                           "YEAR_OF_DEATH", \
                          "ETHNIC", \
                          "RECORD_DATE")

# COMMAND ----------

gdppr_df.head()

# COMMAND ----------

# create a koalas df
import pandas as pd
import numpy as np
import databricks.koalas as ks
import pyspark.sql.functions as F
import datetime as dt
ks.set_option('compute.default_index_type', 'distributed')

gdppr_ks = gdppr_df.to_koalas()

# COMMAND ----------

gdppr_ks.shape

# COMMAND ----------

print(f'''gdppr id null count= {gdppr_df.filter( (F.col(gw("gdppr_id")).isNull())).count()}''')
print(f'''gdppr id nan count= {gdppr_df.filter( F.isnan((F.col(gw("gdppr_id"))))).count()}''')
print(f'''gdppr id "" count= {gdppr_df.filter( F.col(gw("gdppr_id"))=="").count()}''')
# | (F.isnan(F.col(gw("gdppr_id")))) |(F.col(gw("gdppr_id"))=="")).count()}')


# COMMAND ----------

# in koalas

print(f'gdppr id null count= {gdppr_ks[gw("gdppr_id")].isnull().sum()}')


# COMMAND ----------

# make a flag to indicate duplication
import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import Row
windowSpec = Window.partitionBy(gw("gdppr_id"))
gdppr_df = gdppr_df.withColumn('is_dup_in_gdppr', (F.count('*').over(windowSpec) > 1).cast('int'))
#gdppr_df.count()
gdppr_df.show(10)



# COMMAND ----------

# will be used to keep only distinct patient ids
gdppr_df = gdppr_df\
.withColumn('temp_date', F.to_date("RECORD_DATE", format="%Y-%M-%D"))

gdppr_df.head()

# COMMAND ----------

# no way to link null ids to key table
# drop these rows
import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import Row
gdppr_df = gdppr_df.na.drop(subset=[gw("gdppr_id")])
gdppr_df.where(F.col(gw("gdppr_id")).isNull()).show()

# COMMAND ----------

#drop duplicated ids

from pyspark.sql.types import DateType

temp_col ='dup_id'
window_spec = Window.partitionBy(gdppr_df[gw("gdppr_id")]).orderBy(gdppr_df['temp_date'].asc())
gdppr_df = gdppr_df.withColumn(temp_col, F.row_number().over(window_spec))
gdppr_df = gdppr_df.filter(gdppr_df[temp_col]==1).drop('dup_id').drop('temp_date')

gdppr_df.show(10)

# COMMAND ----------

# add isin flag
gdppr_df=gdppr_df.withColumn("isin_gdppr", F.lit(1))
gdppr_df.show(5)


# COMMAND ----------

# rename columns
gdppr_df = gdppr_df.withColumnRenamed("RECORD_DATE", "gdppr_max_rec_date")\
.withColumnRenamed("SEX", "gdppr_sex")\
.withColumnRenamed("YEAR_OF_BIRTH", "gdppr_yob")\
.withColumnRenamed("YEAR_OF_DEATH", "gdppr_yod")\
.withColumnRenamed("ETHNIC", "gdppr_ethnicity")
gdppr_df.head(5)

# COMMAND ----------

# check duplicated columns before and after join. if duplicated, resolve the issue. 
print(f'gdppr distinct count=  {gdppr_df.select(gw("key_id")).distinct().count()}')
print(f'gdppr total count=  {gdppr_df.count()}')
print(f'''gdppr id null count= {gdppr_df.filter( (F.col(gw("gdppr_id")).isNull())).count()}''')
print(f'''gdppr id nan count= {gdppr_df.filter( F.isnan((F.col(gw("gdppr_id"))))).count()}''')
print(f'''gdppr id "" count= {gdppr_df.filter( F.col(gw("gdppr_id"))=="").count()}''')

# COMMAND ----------

gdppr_df.createOrReplaceGlobalTempView(gw("gvw"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- action
# MAGIC select * from global_temp.$gvw limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC # Save the intermediate table

# COMMAND ----------

print(gw("key_chkpnt_1"))

# COMMAND ----------

save_checkpoint(gdppr_df, database=gw("db_collab"), temp_df= gw("key_chkpnt_1"), global_view_name=gw('gvw'))
