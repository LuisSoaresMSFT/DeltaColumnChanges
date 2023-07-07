
# The goal of this notebook is to present to the user the changes that occured in a delta table.
# It takes advantange of the delta versioning and dynamically generates the base SQL for the results.
# For this to work, an "information_schema" table needs to be filled with the metadata of each table to process.
# This assumes there's no information schema to get this information from i.e. no Unity Catalog.
# Author: Luis Soares

# https://pypi.org/project/delta-lake-reader/
# https://github.com/jeppe742/DeltaLakeReader/blob/main/deltalake/deltatable.py
# https://towardsdatascience.com/query-pandas-dataframe-with-sql-2bb7a509793d

# pip install delta-lake-reader
# pip install delta-lake-reader[azure]
# pip install SQLAlchemy==1.4.17

import configparser
from adlfs import AzureBlobFileSystem
from deltalake import DeltaTable

from pandasql import sqldf
import pandas as pd

debug = True

config = configparser.ConfigParser()
config.read('credentials.ini')
storageAccountName = config['ADLS']['storageAccountName']
storageAccountKey  = config['ADLS']['storageAccountKey']

# Create the "information_schema" table
# PK columns MUST be 1st in the list
data = [{'table_name': 'students', 'column_name': 'id',    'column_pos': 1, 'is_key': True,  'is_ignore': False},
        {'table_name': 'students', 'column_name': 'name',  'column_pos': 2, 'is_key': False, 'is_ignore': True},
        {'table_name': 'students', 'column_name': 'grade', 'column_pos': 3, 'is_key': False, 'is_ignore': False},
        {'table_name': 'students', 'column_name': 'year',  'column_pos': 4, 'is_key': False, 'is_ignore': False},
        {'table_name': 'students', 'column_name': 'age',   'column_pos': 5, 'is_key': False, 'is_ignore': False}
        ]
information_schema = pd.DataFrame(data)
#print(information_schema)

# This is a data table to be the target of fetching changes
# This is the notebook with the code to create the table:
deltaTablePath     = 'main-container/datasets/DELTA/students-CDF'

fs = AzureBlobFileSystem(
    account_name = storageAccountName,
    credential   = storageAccountKey
)

dt = DeltaTable(deltaTablePath, file_system=fs) # this gets the latest version
versionNew = dt.version
dfNew = dt.to_pandas()
if (debug): print('New version: ', versionNew)

dfOld = dt.as_version(1).to_pandas() # now we get an older version
versionOld = dt.version
if (debug): print('Old version: ', versionOld)

# Add columns to mimic DESCRIBE HISTORY when run in Spark

dfNew['_change_type']      = 'update_postimage' # value to be compatible with Spark
dfOld['_change_type']      = 'update_preimage'  # valye to be compatible with Spark
dfNew['_commit_version']   = versionNew
dfOld['_commit_version']   = versionOld
dfNew['_commit_timestamp'] = 999 # we don't have access to the timestamp
dfOld['_commit_timestamp'] = 1   # we don't have access to the timestamp

if (debug): print('dfNew:\n', dfNew)
if (debug): print('dfNew:\n', dfOld)

V_CHANGE_students = pd.concat([dfNew, dfOld])
if (debug): print('V_CHANGE_students:\n', V_CHANGE_students)

# We have changes when compared to Spark
# SQLITE CONCAT() is ||
# SQLITE NOW() is date('now') || ' ' || time('now')

# TThis is the query we want to generate dynamically
queryDummy = "\
SELECT \
  s1.id, \
  CASE WHEN s1.grade <> s2.grade THEN s1.grade || ' -> ' || s2.grade ELSE '' END AS grade, \
  CASE WHEN s1.year  <> s2.year  THEN s1.year  || ' -> ' || s2.year   ELSE '' END AS year, \
  CASE WHEN s1.age   <> s2.age   THEN s1.age   || ' -> ' || s2.age    ELSE '' END AS age, \
  date('now') || ' ' || time('now') as sysdate \
FROM V_CHANGE_students s1 \
INNER JOIN V_CHANGE_students s2 ON (s2.id = s1.id) \
WHERE s1._change_type IN ('update_preimage', 'insert') AND s2._change_type = 'update_postimage' AND \
      (s1.grade <> s2.grade OR s1.year <> s2.year OR s1.age <> s2.age) \
"
#output = sqldf(queryDummy)
#print(output)

# Dynamically generate the SQL queries for all tables in the "information_schema" table


table_list = sqldf('SELECT DISTINCT(table_name) AS table_name FROM information_schema')

if (debug): print(table_list)

final = []  # list of final results

for indx, tables_row in table_list.iterrows():
    table_name = tables_row['table_name']
    print('Processing table ' + table_name)

    sql = "SELECT column_name, is_key FROM information_schema WHERE table_name = '" + table_name + "' AND is_ignore = false ORDER BY column_pos"
    column_list = sqldf(sql)
    if (debug): print(column_list)

    full_sql = 'SELECT '  # the SQL query for each table
    key_sql = ''  # the condition for the self join
    pred_sql = ''  # the comparison between all value columns
    id_vars = []  # list of PK's for the pivot operation
    value_vars = []  # list of value columns for the pivot operation

    for indx2, column_row in column_list.iterrows():
        column_name = column_row['column_name']
        is_key = column_row['is_key']

        if (is_key):
            full_sql = full_sql + '\ns1.' + column_name + ',\n'
            if (key_sql != ''): key_sql = key_sql + " AND "
            key_sql = key_sql + "s2." + column_name + " = s1." + column_name
            id_vars = id_vars + [column_name]
        else:
            full_sql = full_sql + "CASE WHEN s1." + column_name + " <> s2." + column_name + " THEN " \
                                  "s1." + column_name + " || ' -> ' || s2." + column_name + " " \
                                  "ELSE '' END AS " + column_name + ",\n"
            pred_sql = pred_sql + ("(" if pred_sql == '' else " OR ")
            pred_sql = pred_sql + "s1." + column_name + " <> s2." + column_name
            value_vars = value_vars + [column_name]

    full_sql = full_sql + "date('now') || ' ' || time('now') as sysdate\n" + \
               "FROM V_CHANGE_" + table_name + " s1\n" + \
               "INNER JOIN V_CHANGE_" + table_name + " s2 ON (" + key_sql + ")\n" + \
               "WHERE s1._change_type IN ('update_preimage', 'insert') AND s2._change_type = 'update_postimage' AND\n" + \
               pred_sql + ")"

    if (debug): print('SQL:\n' + full_sql)

    final.append([table_name, full_sql, id_vars, value_vars])

    if (debug): print('id_vars:', id_vars)
    if (debug): print('value_vars:', value_vars)

if (debug): print('final:\n', final)

# Get results for one table, melt (unpivot), clean

table_name, full_sql, id_vars, value_vars = final[0][0], final[0][1], final[0][2], final[0][3]
if(debug): print('table_name:', table_name)
if(debug): print('full_sql:',   full_sql)
if(debug): print('id_vars:',    id_vars)
if(debug): print('value_vars:', value_vars)

# execute SQL quesry
df = sqldf(full_sql)
print('\n', df)

# melt the dataset
df_melted = df.melt(id_vars, value_vars).sort_values(by=id_vars + ['variable'])
print('\n', df_melted)

# clean the dataset
df_melted_clean = df_melted[df_melted['value'] != '']
print('\n', df_melted_clean)


