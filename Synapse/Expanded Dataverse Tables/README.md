# Detailed Instructions
https://cooptimize.org/easily-modify-dataverse-option-set-integers-to-text-synapse-serverless-edition/

# External Data Source
An external data source is used by Synapse external tables and views to provide access to live Data Lake data leveraging T-SQL.

# External File Format
An external file format is used by Synapse external tables and views to define the structure of the files in the lake. 
For Dataverse data is stored in CSV, thus the file format defines the comma delimiter as a field terminator.

# Expanded Dataverse Table from Data Lake
Modifying integers that represent text is a key part of consuming Dataverse data from a Data Lake. However, many of the published solutions tell you to do this process one integer at a time or don’t create an identical schema to the TDS endpoint.
