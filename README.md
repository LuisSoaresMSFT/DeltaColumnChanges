# DeltaColumnChanges

The goal of this project is to present to the user the changes that occured in a delta table. It takes advantange of the delta versioning and dynamically generates the base SQL for the results. For this to work, an "information_schema" table needs to be filled with the metadata of each table to process. This assumes there's no information schema to get this information from i.e. no Unity Catalog.

The methods for delta source selection can be:
-  Between 2 delta versions
-  Between timestamps
-  Providing only the starting version or timestamp

If the selected method returns more than 2 records per key, only the oldest and newest records are used for the delta comparison, which makes this solution flexible in the way the delta source is selected.

Example. Get from

<img width="806" alt="source" src="https://github.com/LuisSoaresMSFT/DeltaColumnChanges/assets/57713603/e1862542-6319-4b29-a227-1250a54e288c">

to (option 1)

<img width="526" alt="target1" src="https://github.com/LuisSoaresMSFT/DeltaColumnChanges/assets/57713603/bbfca299-9c22-426d-9a52-14279cb23f01">

or (option 2)

<img width="281" alt="target2" src="https://github.com/LuisSoaresMSFT/DeltaColumnChanges/assets/57713603/10267320-e08d-4e91-80cf-769f61fd3336">

or (option 3)

<img width="281" alt="target3" src="https://github.com/LuisSoaresMSFT/DeltaColumnChanges/assets/57713603/4f0af6f1-2b4c-4dc9-9b14-810ab549dfdc">


