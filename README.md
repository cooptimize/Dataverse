This repository helps report from a Dataverse Environment synced to a Data Lake.

# Important Note
When you publish your Dataverse Environment to the Lake, use the Synapse Workspace option. This option changes the structure and names of the Data Lake folders and files. We haven't tested (nor will we) on the folder structure generated when Synapse Workspace is turned off.

# Choice Metadata
One of the challenges of Data Lake reporting is merging in Choices to the raw tables ("Option Sets", "Global Option Sets", "Enums", "Dataverse Integer Fields"). This code helps do this either using Power Query (Dataflow or Power BI Desktop) and SQL (On-demand or Dedicated).

Having to merge in labels one field at a time is very time consuming. All the options presented in here use recursive functions to resolve the Option Sets. Which basically means it all happens at once instead of having to do joins one field at a time.

# Folders
| Folder | Purpose |
| --- | ---- |
| Dataflows | JSON files for importing as Dataflows |
| Power BI Desktop | .pbit file |
| Synapse | SQL functions and stored procedures |

# Reference Documentation
- Microsoft Docs [Resolving Choice Labels](https://docs.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-choice-labels)
- Cooptimize Blog [Easily Modify Dataverse Option Set Integers to Text • Power Query Edition](https://cooptimize.org/easily-modify-dataverse-option-set-integers-to-text-power-query-edition/)
- Cooptimize Blog [Easily Modify Dataverse Option Set Integers to Text • Synapse Serverless Edition](https://cooptimize.org/easily-modify-dataverse-option-set-integers-to-text-synapse-serverless-edition/)
