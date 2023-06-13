-- Databricks notebook source
-- MAGIC %md
-- MAGIC The goal of this notebook is to present to the user the changes that occured in a delta table.
-- MAGIC It takes advantange of the delta versioning and dynamically generates the base SQL for the results.
-- MAGIC For this to work, an "information_schema" table needs to be filled with the metadata of each table to process. This assumes there's no information schema to get this information from i.e. no Unity Catalog.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC debug = True

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Some information was taken from: https://learn.microsoft.com/en-us/azure/databricks/delta/delta-change-data-feed

-- COMMAND ----------

-- Create the "information_schema" table
-- PK columns MUST be 1st in the list
-- 
CREATE OR REPLACE TABLE information_schema(table_name STRING, column_name STRING, column_pos INT, is_key BOOLEAN, is_ignore BOOLEAN)
TBLPROPERTIES (delta.enableChangeDataFeed = false);

INSERT INTO information_schema VALUES('students', 'id',    1, true,  false),
                                     ('students', 'name',  2, false, true),
                                     ('students', 'grade', 3, false, false),
                                     ('students', 'year',  4, false, false),
                                     ('students', 'age',   5, false, false);


-- COMMAND ----------

-- This is a data table to be the target of fetching changes

DROP TABLE students; -- droped to clean any previous delta changes

-- Creation will be change # 0
CREATE OR REPLACE TABLE students(id INT, name STRING, grade DOUBLE, year INT, age INT)
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Insert will be change # 1
INSERT INTO students VALUES(4, "Ted",     4.7, 2020, 20),
                           (5, "Tiffany", 5.5, 2021, 21),
                           (6, "Vini",    6.3, 2022, 22);

-- Update will be change # 2
UPDATE students SET grade = grade + 1, year = year + 1 WHERE name LIKE "T%";

SELECT * FROM students;

-- COMMAND ----------

DESCRIBE EXTENDED students -- Property "Provider" shows it's a delta table

-- COMMAND ----------

DESCRIBE HISTORY default.students -- List delta changes

-- COMMAND ----------

-- Different ways to get delta changes

-- Between 2 delta versions (as ints or longs e.g. changes from version 1 to 2)
-- SELECT * FROM table_changes('students', 1, 2);

-- Between timestamps (as string formatted timestamps)
-- SELECT * FROM table_changes('tableName', '2021-04-21 05:45:46', '2021-05-21 12:00:00')

-- Providing only the starting version/timestamp
-- SELECT * FROM table_changes('students', 0);
SELECT * FROM table_changes('students', 2);

-- With database/schema names inside the string for table name, with backticks for escaping dots and special characters
-- SELECT * FROM table_changes('dbName.`dotted.tableName`', '2021-04-21 06:45:46' , '2021-05-21 12:00:00')

-- With path based tables
-- SELECT * FROM table_changes_by_path('\path', '2021-04-21 05:45:46')


-- COMMAND ----------

-- Create a temporary view to the delta changes

CREATE OR REPLACE TEMPORARY VIEW V_CHANGE_students AS
SELECT * FROM table_changes('students', 2);


-- COMMAND ----------

-- This is the query we want to generate dynamically

SELECT
  s1.id,
  CASE WHEN s1.grade <> s2.grade THEN concat(s1.grade, ' -> ', s2.grade) ELSE '' END AS grade,
  CASE WHEN s1.year  <> s2.year  THEN concat(s1.year,  ' -> ', s2.year)  ELSE '' END AS year,
  CASE WHEN s1.age   <> s2.age   THEN concat(s1.age,   ' -> ', s2.age)   ELSE '' END AS age,
  now() as sysdate
FROM V_CHANGE_students s1
INNER JOIN V_CHANGE_students s2 ON (s2.id = s1.id)
WHERE s1._change_type IN ('update_preimage', 'insert') AND s2._change_type = 'update_postimage' AND
      (s1.grade <> s2.grade OR s1.year <> s2.year OR s1.age <> s2.age) 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Dynamically generate the SQL queries for all tables in the "information_schema" table
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC table_list = spark.sql('SELECT DISTINCT(table_name) AS table_name FROM information_schema').toPandas()
-- MAGIC if(debug): display(table_list)
-- MAGIC
-- MAGIC final = [] # list of final results
-- MAGIC
-- MAGIC for indx, tables_row in table_list.iterrows():
-- MAGIC     table_name = tables_row['table_name']
-- MAGIC     print('Processing table ' + table_name)
-- MAGIC
-- MAGIC     sql = "SELECT column_name, is_key FROM information_schema WHERE table_name = '" + table_name + "' AND is_ignore = false ORDER BY column_pos"
-- MAGIC     column_list = spark.sql(sql).toPandas()
-- MAGIC     if(debug): display(column_list)
-- MAGIC
-- MAGIC     full_sql   = 'SELECT ' # the SQL query for each table
-- MAGIC     key_sql    = ''        # the condition for the self join
-- MAGIC     pred_sql   = ''        # the comparison between all value columns
-- MAGIC     id_vars    = []        # list of PK's for the pivot operation
-- MAGIC     value_vars = []        # list of value columns for the pivot operation
-- MAGIC
-- MAGIC     for indx2, column_row in column_list.iterrows():
-- MAGIC         column_name = column_row['column_name']
-- MAGIC         is_key      = column_row['is_key']
-- MAGIC
-- MAGIC         if(is_key):
-- MAGIC             full_sql = full_sql + '\ns1.' + column_name + ',\n'
-- MAGIC             if(key_sql != ''): key_sql = key_sql + " AND "
-- MAGIC             key_sql = key_sql + "s2." + column_name + " = s1." + column_name
-- MAGIC             id_vars = id_vars + [column_name]
-- MAGIC         else:
-- MAGIC             full_sql = full_sql + "CASE WHEN s1." + column_name + " <> s2." + column_name + " THEN " \
-- MAGIC                                 "concat(s1." + column_name + ", ' -> ', s2. " + column_name + ") " \
-- MAGIC                                 "ELSE '' END AS " + column_name + ",\n"
-- MAGIC             pred_sql = pred_sql + ("(" if pred_sql == '' else " OR ")
-- MAGIC             pred_sql = pred_sql + "s1." + column_name + " <> s2." + column_name
-- MAGIC             value_vars = value_vars + [column_name]
-- MAGIC
-- MAGIC     full_sql = full_sql + "now() as sysdate\n" + \
-- MAGIC                         "FROM V_CHANGE_" + table_name + " s1\n" + \
-- MAGIC                         "INNER JOIN V_CHANGE_" + table_name + " s2 ON (" + key_sql + ")\n" + \
-- MAGIC                         "WHERE s1._change_type IN ('update_preimage', 'insert') AND s2._change_type = 'update_postimage' AND\n" + \
-- MAGIC                         pred_sql + ")"
-- MAGIC     
-- MAGIC     if(debug): print('SQL:\n' + full_sql)
-- MAGIC
-- MAGIC     final.append([table_name, full_sql, id_vars, value_vars])
-- MAGIC
-- MAGIC     if(debug): print('id_vars:', id_vars)
-- MAGIC     if(debug): print('value_vars:', value_vars)
-- MAGIC
-- MAGIC if(debug): print('final:', final)
-- MAGIC

-- COMMAND ----------

SELECT 
s1.id,
CASE WHEN s1.grade <> s2.grade THEN concat(s1.grade, ' -> ', s2. grade) ELSE '' END AS grade,
CASE WHEN s1.year <> s2.year THEN concat(s1.year, ' -> ', s2. year) ELSE '' END AS year,
CASE WHEN s1.age <> s2.age THEN concat(s1.age, ' -> ', s2. age) ELSE '' END AS age,
now() as sysdate
FROM V_CHANGE_students s1
INNER JOIN V_CHANGE_students s2 ON (s2.id = s1.id)
WHERE s1._change_type IN ('update_preimage', 'insert') AND s2._change_type = 'update_postimage' AND
(s1.grade <> s2.grade OR s1.year <> s2.year OR s1.age <> s2.age)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Get results for one table, melt (unpivot), clean
-- MAGIC
-- MAGIC table_name, full_sql, id_vars, value_vars = final[0][0], final[0][1], final[0][2], final[0][3]
-- MAGIC if(debug): print('table_name:', table_name)
-- MAGIC if(debug): print('full_sql:',   full_sql)
-- MAGIC if(debug): print('id_vars:',    id_vars)
-- MAGIC if(debug): print('value_vars:', value_vars)
-- MAGIC
-- MAGIC # execute SQL quesry
-- MAGIC df = spark.sql(full_sql).toPandas()
-- MAGIC display(df)
-- MAGIC
-- MAGIC # melt the dataset
-- MAGIC df_melted = df.melt(id_vars, value_vars).sort_values(by=id_vars + ['variable'])
-- MAGIC display(df_melted)
-- MAGIC
-- MAGIC # clean the dataset
-- MAGIC df_melted_clean = df_melted[df_pivot['value'] != '']
-- MAGIC display(df_melted_clean)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC If we want to select the changes between 2 versions (or timestamps), then we may have several changes applied to the same PK.
-- MAGIC This means we need to extract only the oldest and newest record for each PK

-- COMMAND ----------

-- Select changes between 2 versions

SELECT * FROM table_changes('students', 1, 2) ORDER BY ID, _commit_version

-- COMMAND ----------

-- Create a temporaty view for a result that holds multiple changes for each PK into a table with 1-2 records per PK (oldest and newest)

-- We can recreate the same view as before as it has the same structure
-- This view can also be generated dynamically
CREATE OR REPLACE TEMP VIEW V_CHANGE_students AS
SELECT * FROM table_changes('students', 1, 2) s1
WHERE (
  s1._commit_timestamp = (SELECT MAX(s2._commit_timestamp) FROM table_changes('students', 1, 2) s2 WHERE s2.id = s1.id AND s2._change_type = s2._change_type)
  AND s1._change_type IN ('update_postimage', 'insert')
) OR (
s1._commit_timestamp = (SELECT MIN(s2._commit_timestamp) FROM table_changes('students', 1, 2) s2 WHERE s2.id = s1.id AND s2._change_type = s2._change_type)
AND s1._change_type IN ('update_preimage', 'insert')
)
ORDER BY id, _commit_timestamp DESC;

SELECT * FROM V_CHANGE_students ORDER BY ID, _commit_version

-- We can now use this view as an input to the generation of changes, as before
