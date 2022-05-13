# Dataverse
Code to help reporting from a Data Lake synced to a Dataverse.

# Important Notes
* When you publish your Dataverse Environment to the Lake, use the Synapse Workspace option. This option actually changes the entire structure and names of the Data Lake folders and files. 
* We haven't tested (nor will we) on the folder structure generated when Synapse Workspace is turned off.

#Choice Metadata
One of the challenges of Data Lake reporting is merging in Choices to the raw tables ("Option Sets", "Global Option Sets", "Enums", "Dataverse Integer Fields"). This code helps do this either using M Query or SQL On-Demand.

Here is Microsoft's article on resolving Choice Labels: https://docs.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-choice-labels

Having to merge in labels one field at a time is very time consuming. We've written recursive functions to help automate this process.

# Dataflows Folder
These are JSON files that need to be imported as Dataflows. Once imported, the M functions could be used elsewhere in Power Query.
