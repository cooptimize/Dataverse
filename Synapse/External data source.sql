-- Create a database master key if one does not already exist, using your own password.
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<create your own password>' ;

-- Create a database scoped credential with Azure storage account key as the secret.
CREATE DATABASE SCOPED CREDENTIAL DataverseCredential
WITH
  IDENTITY = 'SHARED ACCESS SIGNATURE',
  -- Remove ? from the beginning of the SAS token
  SECRET = '<Your Shared Access Signature without the ? prefix>' ;

CREATE EXTERNAL DATA SOURCE Dataverse
WITH
  ( LOCATION = 'https://<your storage account>.dfs.core.windows.net/<your Dataverse container>' ,
    CREDENTIAL = DataverseCredential
  ) ;