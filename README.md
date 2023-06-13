# DeltaColumnChanges

The goal of this project is to present to the user the changes that occured in a delta table. It takes advantange of the delta versioning and dynamically generates the base SQL for the results. For this to work, an "information_schema" table needs to be filled with the metadata of each table to process. This assumes there's no information schema to get this information from i.e. no Unity Catalog.
