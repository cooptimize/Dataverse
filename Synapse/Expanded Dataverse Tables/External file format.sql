/****************************************************************************************************************************************
Run this script to create an external file format, no updates are required.
*****************************************************************************************************************************************/

CREATE EXTERNAL FILE FORMAT [CSVFormat]
WITH (FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS 
(FIELD_TERMINATOR = N',', STRING_DELIMITER = N'"', USE_TYPE_DEFAULT = False))
GO


