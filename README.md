# Dataverse
Code to help reporting from a Data Lake synced to a Dataverse.

# Important Notes
* When you publish your Dataverse Environment to the Lake, use the Synapse Workspace option. This option actually changes the entire structure and names of the Data Lake folders and files. Although this code could be modified to work, it's much easier to deploy a Synapse Workspace.

# Dataflows Folder
These are JSON files that need to be imported as Dataflows. Once imported, the M functions could be used elsewhere in Power Query.
