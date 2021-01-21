# -*- coding: utf-8 -*-
"""
Created on Fri Sep 18 18:23:22 2020

@author: WCY8676
"""

import configparser
import sys
import snowflake.connector
import pandas as pd
import os
from snowflake.connector.pandas_tools import write_pandas
import re

###############################################################################################################
# Config file reader
###############################################################################################################
config = configparser.RawConfigParser()
conn_config_file_path = sys.argv[1]
print(conn_config_file_path)
config.read(conn_config_file_path)

###############################################################################################################
# Functions
###############################################################################################################
def get_config_item(dict_item, var):
    config_keyvals = dict(config.items(dict_item))
    val = config_keyvals[var]
    print('INFO: ' + var + ' : ' + val)
    if not var:
        raise Exception('ERROR: ' + var + " cannot be an empty string")
    return val

def create_snowflake_session(username, auth_type, acc, warehouse, database, schema, role):
    try:
        SnowflakeSession = snowflake.connector.connect(
            user= username,
            authenticator=auth_type,
            account=acc,
            warehouse = warehouse,
            database = database,
            schema =schema,
            role = role
        )
        print("Snowflake connected")
        return SnowflakeSession
    except Exception as e:    
       print(f"Snowflake did not connect due to {e}")
       
def export_df_to_snowflake(df, conn, table):
    #preparing sql query to create or replace table based on the df
    cols = str(df.dtypes.to_dict())
    cols = re.sub("{", "", cols)
    cols = re.sub("}", "", cols)
    cols = re.sub(":", "", cols)
    cols = re.sub("dtype\('O'\)", "string", cols)
    cols = re.sub("dtype\('int64'\)", "integer", cols)
    cols = re.sub("dtype\('float64'\)", "double", cols)
    #add any new type convertion here if needed
    cols = re.sub("'", "", cols)
    query = "CREATE OR REPLACE TABLE " + table + ' (' + cols + ')'
    print(query)
    conn.cursor().execute(query)
    write_pandas(conn, df, table.upper())

###############################################################################################################
# Get config values
###############################################################################################################
# Get config values file_properties

#snowflake connection
username = get_config_item('snow_param', 'username')
warehouse = get_config_item('snow_param', 'warehouse')
database = get_config_item('snow_param', 'database')
schema = get_config_item('snow_param', 'schema')
role = get_config_item('snow_param', 'role')
snow_table = get_config_item('snow_param', 'tablename')

#project parameters
data_incoming_foldername = get_config_item('project_param', 'data_incoming_foldername')

#project parameters
file_relative_path = get_config_item('file_param', 'file_relative_path')
file_radc = get_config_item('file_param', 'file_radcname')
file_ext = get_config_item('file_param', 'file_ext')
sheet_name = get_config_item('file_param', 'sheet_name')

##########################main##########################
path_script = os.getcwd()
path_project = os.path.dirname(path_script)
path_incoming = os.path.join(path_project, data_incoming_foldername)
path_file = os.path.join(data_incoming_foldername, file_relative_path, file_radc + file_ext)

conn = create_snowflake_session(username, warehouse, database, schema, role)
df = pd.read_excel(path_file, sheet_name=sheet_name)

export_df_to_snowflake(df, conn, snow_table)
