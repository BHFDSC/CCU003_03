'''
A set of basic functions for Spark Dataframes
By: Mehrdad Mizani
Project: CCU003_03
'''

import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.types import DateType
import databricks.koalas as ks


def union_ks_maker(in_db_name, table_list):
    """Used to create a single hes table.  Gets a list of hes tables and returns the union of tables with 
    non-deterministic automatic Koalas index
  
    Args:
        in_db_name (str): Name of the database containing the raw hes tables
        table_list (list): List of table names for each hes table type. For example hes_ae_...
        
    Returns:
        Kolas dataframe: The union of all tables in a Koalas dataframe. 
        
    Note: 
        Koalas is transformed distributely in this function. The auto-index assigned by Koalas per each hes_xyz_... table will be non-deterministic.
        Use an alternative index to get the same resutls for index-based operations (e.g. head, loc)
  
    """
    ks.set_option('compute.default_index_type', 'distributed')
    first_table_name = table_list[0]
    ks_holder = ks.sql(f"SELECT * FROM {in_db_name}.{first_table_name}")
    
    for table_name in table_list[1:]:
        temp_ks = ks.sql(f"SELECT * FROM {in_db_name}.{table_name}")
        ks_holder = ks.concat([ks_holder, temp_ks], axis=0)
    return(ks_holder)


def union_df_maker(in_db_name, table_list):
    """Used to create a single hes table.  Gets a list of hes tables and returns the union of dataframes
  
    Args:
        in_db_name (str): Name of the database containing the raw hes tables
        table_list (list): List of table names for each hes table type. For example hes_ae_...
        
    Returns:
        pyspark dataframe: The union of all tables in a pyspark dataframe. 

    """
    first_table_name = table_list[0]
    df_holder = spark.sql(f"SELECT * FROM {in_db_name}.{first_table_name}")
    
    for table_name in table_list[1:]:
        temp_df = spark.sql(f"SELECT * FROM {in_db_name}.{table_name}")
        df_holder = df_holder.union(temp_df)
    return(df_holder)


def save_checkpoint(df_in, database='dars_nic_391419_j3w9t_collab', temp_df='ccu003_mm_temp_checkpoint', global_view_name="ccu003_mm_temp_checkpoint_gvw"):
    """Makes a user defined checkpoint table in the database and reloads it as a Spark dataframe. 
    The aim is to save data wityout the data lineage. 
    
    Args: 
        df_in (Spark dataframe): Input Spark dataframe to be checkpointed
        
        database: Name of the database to hold the checkpoint
        
        temp_df: name of the checkpoint table on the database
        
        global_view_name: the name used for the intermediate global view ti create the checkpoint. Useful when this function is called simultanously from different notebooks, to prevent global veiw overriding. 
        
    Returns:
        None
    """
    # Todo
    # check df is spark dataframe and not null
    # check database is valid
    # check if temp_df exists or not
    # if drop flag is ture, check if temp_df exists on datasbase as hive table
    # check of table is dropable
    df_in.count()
    global_view_table = global_view_name
    df_in.createOrReplaceGlobalTempView(global_view_table)
    print(f'''global view {global_view_table} created....''')
    query= f'''CREATE TABLE {database}.{temp_df}  AS SELECT * FROM global_temp.{global_view_table}'''
    spark.sql(query)
    print(f'''table {temp_df} created in database {database}''')
    query= f'''ALTER TABLE {database}.{temp_df}  OWNER TO {database}'''
    spark.sql(query)
    print(f'''table {temp_df} is now droppabale''')
    query= f'''REFRESH TABLE {database}.{temp_df}'''
    spark.sql(query)
    print(f'''Done''')



def load_checkpoint(database='dars_nic_391419_j3w9t_collab', temp_df=""):
  
  """Loads a saved table from HIVE into a Spark Dataframe
  
  Args:
      database (str): Name of the database
      temp_df (ste): Name of the table
  """
    # Todo exceptions
    df = spark.sql(f'''SELECT * FROM {database}.{temp_df}''')
    print(f'''table {temp_df} loaded''')
    return(df)

# COMMAND ----------

def drop_checkpoint(database='dars_nic_391419_j3w9t_collab', temp_df=""):
  """Drops a table from the database
  
  Args:
      database (str): Name of the database
      temp_df (ste): Name of the table
  """
    # Todo exceptions
    spark.sql(f'''DROP TABLE {database}.{temp_df}''')
    print(f'''table {temp_df} dropped''')


def sorted_window_duplicate_remover(df_in, patid, window_by, asc=True):
    """Removes the duplicated patient ids by window operation on sorted values. 
    For example, if a patient had several rows with different date values for window_by columns, 
    this function sorts them ascending by default (or descending if asc=False) and keeps only the one with the oldest  date. 
    
    Args:
        df_in (Spark dataframe): The dataframe containing duplicated ids and at least one feature for windowing 
        patid (String): The name of the feature (column) of patient id
        window_by (String): The name of the feature for windowing. This is usually a date type. 
        asc: If true, the first row to be kept will be the smallest value (i.e. the oldest date). Otherwise, the first row to be kept will be the most recent value. 
    
    Returns: 
        df_out (Spark dataframe): The dataframe with unique patient ids.
        
    """


    df_out = df_in

    temp_col ='dup_id'
    if asc:
        window_spec = Window.partitionBy(df_out[patid]).orderBy(df_out[window_by].asc())
    else:
        window_spec = Window.partitionBy(df_out[patid]).orderBy(df_out[window_by].desc())
    df_out = df_out.withColumn(temp_col, F.row_number().over(window_spec))
    df_out = df_out.filter(df_out[temp_col]==1).drop('dup_id')
    return(df_out)


def check_distinct(df, col):
  
  """Prints the total count and distinct counts of the table with regards to a single columns, usually the unique ID (Index)
  Args:
      df (Spark Dataframe): the dataframe to check.
      col (str): the name of the column for counting.
  """
    print(f' distinct count=  {df.select(col).distinct().count()}')
    print(f' total count=  {df.count()}')



