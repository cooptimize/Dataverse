/****************************************************************************************************************************************
The purpose of this query is to create external tables and views that automatically resolve Option Sets for Dataverse tables.
Prerequistes include:
	1. Create an external data source to the data lake using Shared Access Signature
	Example script - https://github.com/cooptimize/Dataverse/blob/main/Synapse/External%20data%20source.sql
	
	2. Create an external file format for CSV
	Example script - https://github.com/cooptimize/Dataverse/blob/main/Synapse/External%20file%20format.sql

Run this against the Synapse Serverless pool you want the objects to be created in
*****************************************************************************************************************************************/


/****************************************************************************************************************************************
User Parameters
*****************************************************************************************************************************************/

DECLARE @ExternalDataSource NVARCHAR(128) = 'Dataverse'
DECLARE @ExternalFileFormat NVARCHAR(128) = 'CSVFormat'
DECLARE @Schema NVARCHAR(128) = 'dbo'


DECLARE @ExternalTablePrefix NVARCHAR(128) = ''
DECLARE @ExternalTableSuffix NVARCHAR(128) = 'table'
DECLARE @Table NVARCHAR(MAX) = ''
DECLARE @Columns NVARCHAR(MAX) = ''


/****************************************************************************************************************************************
If you want to store this as a stored procedure delete or comment out the user parameters above and uncomment the section below.
*****************************************************************************************************************************************/

--CREATE PROCEDURE dbo.sp_DataverseExternalTablesandViews

--@ExternalDataSource NVARCHAR(128), 
--@ExternalFileFormat NVARCHAR(128),
--@Schema NVARCHAR(128),


--@ExternalTablePrefix NVARCHAR(128) ,
--@ExternalTableSuffix NVARCHAR(128),
--@Table NVARCHAR(MAX),
--@Columns NVARCHAR(MAX)
--AS

/****************************************************************************************************************************************
Internal Parameters
*****************************************************************************************************************************************/

DECLARE @ExternalTable NVARCHAR(MAX)
DECLARE @ExternalTableCTE NVARCHAR(MAX)
DECLARE @ViewDefinition NVARCHAR(MAX)
DECLARE @Join NVARCHAR(MAX)
DECLARE @JoinOutput NVARCHAR(MAX)
DECLARE @DropTable NVARCHAR(MAX)
DECLARE @ViewLogic NVARCHAR(MAX)
DECLARE @OptionSetsUnion NVARCHAR(MAX)


/****************************************************************************************************************************************
Check to see if supplied external data source exists
*****************************************************************************************************************************************/

IF EXISTS (SELECT 1 FROM sys.external_data_sources WHERE [name] = @ExternalDataSource)
BEGIN


/****************************************************************************************************************************************
Check to see if supplied external file format exists
*****************************************************************************************************************************************/

IF EXISTS (SELECT 1 FROM sys.external_file_formats WHERE [Name] = @ExternalFileFormat)
BEGIN

/****************************************************************************************************************************************
Check to see if external table prefix was supplied
*****************************************************************************************************************************************/


IF (LEN(@ExternalTablePrefix) <> 0 OR LEN(@ExternalTableSuffix) <> 0)
BEGIN

/****************************************************************************************************************************************
Check to see if OptionSetsUnion view already exists, if not create it in schema selected
*****************************************************************************************************************************************/


IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = 'OptionSetsUnion')  
BEGIN


SET @OptionSetsUnion = '
CREATE VIEW [' + @Schema + '].[OptionSetsUnion] AS
SELECT 
  REPLACE(CAST(result.filepath(1) as varchar(100)),''-'','''') AS [Table Name],
  OptionSetName AS [Field Name],
  [Option] AS [Option Id], 
  GlobalOptionSetMetadata.LocalizedLabel AS [Option Field Label],
  ''GlobalOptionSetMetadata'' AS [Metadata Type],
  CAST('''' AS NVARCHAR(10)) AS State,
  CAST('''' AS NVARCHAR(10)) AS Status,
  AttributeMetadata.AttributeType
FROM 
  OPENROWSET(
    BULK ''/Microsoft.Athena.TrickleFeedService/*EntityMetadata.json'', 
    DATA_SOURCE = ''' + @ExternalDataSource + ''', FORMAT = ''csv'', 
    FIELDQUOTE = ''0x0b'', FIELDTERMINATOR = ''0x0b'', 
    ROWTERMINATOR = ''0x0b''
  ) WITH (
    jsonContent varchar(MAX)
  ) AS [result] 
  CROSS APPLY OPENJSON(jsoncontent, ''$.GlobalOptionSetMetadata'') WITH (
    OptionSetName NVARCHAR(20), 
    [Option] bigint, 
    IsUserLocalizedLabel NVARCHAR(20), 
    LocalizedLabelLanguageCode bigint, 
    LocalizedLabel nvarchar(20)
  ) AS GlobalOptionSetMetadata
  CROSS APPLY OPENJSON(jsoncontent, ''$.AttributeMetadata'') WITH (
    EntityName NVARCHAR(128),
	AttributeName NVARCHAR(128),
	AttributeType NVARCHAR(128),
	AttributeTypeCode bigint,
	[Version] bigint,
	[TimeStamp] NVARCHAR(100),
	MetadataId NVARCHAR(100),
	Precision bigint,
	MaxLength bigint
  ) AS AttributeMetadata
  WHERE REPLACE(CAST(result.filepath(1) as varchar(128)),''-'','''') = AttributeMetadata.EntityName 
  AND GlobalOptionSetMetadata.OptionSetName = AttributeMetadata.AttributeName
UNION ALL
SELECT 
  REPLACE(CAST(result.filepath(1) as varchar(100)),''-'','''') AS [Table Name],
  OptionSetName AS [Field Name],
  [Option] AS [Option Id], 
  OptionSetMetadata.LocalizedLabel AS [Option Field Label],
  ''OptionSetMetadata'' AS [Metadata Type],
  CAST('''' AS NVARCHAR(10)) AS State,
  CAST('''' AS NVARCHAR(10)) AS Status,
  AttributeMetadata.AttributeType
FROM 
 OPENROWSET(
    BULK ''/Microsoft.Athena.TrickleFeedService/*EntityMetadata.json'', 
    DATA_SOURCE = ''' + @ExternalDataSource + ''', FORMAT = ''csv'', 
    FIELDQUOTE = ''0x0b'', FIELDTERMINATOR = ''0x0b'', 
    ROWTERMINATOR = ''0x0b''
  ) WITH (
    jsonContent varchar(MAX)
  ) AS [result] 
  CROSS APPLY OPENJSON(jsoncontent, ''$.OptionSetMetadata'') WITH (
    EntityName NVARCHAR(20),
	OptionSetName NVARCHAR(20), 
    [Option] bigint, 
    IsUserLocalizedLabel NVARCHAR(20), 
    LocalizedLabelLanguageCode bigint, 
    LocalizedLabel nvarchar(20)
  ) AS OptionSetMetadata
  CROSS APPLY OPENJSON(jsoncontent, ''$.AttributeMetadata'') WITH (
    EntityName NVARCHAR(128),
	AttributeName NVARCHAR(128),
	AttributeType NVARCHAR(128),
	AttributeTypeCode bigint,
	[Version] bigint,
	[TimeStamp] NVARCHAR(100),
	MetadataId NVARCHAR(100),
	Precision bigint,
	MaxLength bigint
  ) AS AttributeMetadata
  WHERE OptionSetMetadata.EntityName = AttributeMetadata.EntityName 
  AND OptionSetMetadata.OptionSetName = AttributeMetadata.AttributeName'

SET @OptionSetsUnion = @OptionSetsUnion + '
UNION ALL
SELECT 
  REPLACE(CAST(result.filepath(1) as varchar(100)),''-'','''') AS [Table Name],
  ''statecode'' AS [Field Name],
  [State] AS [Option Id], 
  StateMetadata.LocalizedLabel AS [Option Field Label],
  ''StateMetadata'' AS [Metadata Type],
  CAST([State] AS NVARCHAR(10)),
  CAST('''' AS NVARCHAR(10)) AS [Status],
  AttributeMetadata.AttributeType
FROM 
  OPENROWSET(
    BULK ''/Microsoft.Athena.TrickleFeedService/*EntityMetadata.json'', 
    DATA_SOURCE = ''' + @ExternalDataSource + ''', FORMAT = ''csv'', 
    FIELDQUOTE = ''0x0b'', FIELDTERMINATOR = ''0x0b'', 
    ROWTERMINATOR = ''0x0b''
  ) WITH (
    jsonContent varchar(MAX)
  ) AS [result] 
  CROSS APPLY OPENJSON(jsoncontent, ''$.StateMetadata'') WITH (
    EntityName NVARCHAR(20),
	State int, 
    IsUserLocalizedLabel NVARCHAR(20), 
    LocalizedLabelLanguageCode bigint, 
    LocalizedLabel nvarchar(20)
  ) AS StateMetadata
   CROSS APPLY OPENJSON(jsoncontent, ''$.AttributeMetadata'') WITH (
    EntityName NVARCHAR(128),
	AttributeName NVARCHAR(128),
	AttributeType NVARCHAR(128),
	AttributeTypeCode bigint,
	[Version] bigint,
	[TimeStamp] NVARCHAR(100),
	MetadataId NVARCHAR(100),
	Precision bigint,
	MaxLength bigint
  ) AS AttributeMetadata
  WHERE StateMetadata.EntityName = AttributeMetadata.EntityName 
  AND ''statecode'' = AttributeMetadata.AttributeName
UNION ALL
SELECT 
  REPLACE(CAST(result.filepath(1) as varchar(100)),''-'','''') AS [Table Name],
  ''statuscode'' AS [Field Name],
  [State] AS [Option Id], 
  StatusMetadata.LocalizedLabel AS [Option Field Label],
  ''StatusMetadata'' AS [Metadata Type],
  CAST([State] AS NVARCHAR(10)),
  CAST([Status] AS NVARCHAR(10)) AS [Status],
  AttributeMetadata.AttributeType
FROM 
  OPENROWSET(
    BULK ''/Microsoft.Athena.TrickleFeedService/*EntityMetadata.json'', 
    DATA_SOURCE = ''' + @ExternalDataSource + ''', FORMAT = ''csv'', 
    FIELDQUOTE = ''0x0b'', FIELDTERMINATOR = ''0x0b'', 
    ROWTERMINATOR = ''0x0b''
  ) WITH (
    jsonContent varchar(MAX)
  ) AS [result] 
  CROSS APPLY OPENJSON(jsoncontent, ''$.StatusMetadata'') WITH (
    EntityName NVARCHAR(20),
	State int, 
	Status int,
    IsUserLocalizedLabel NVARCHAR(20), 
    LocalizedLabelLanguageCode bigint, 
    LocalizedLabel nvarchar(20)
  ) AS StatusMetadata
   CROSS APPLY OPENJSON(jsoncontent, ''$.AttributeMetadata'') WITH (
    EntityName NVARCHAR(128),
	AttributeName NVARCHAR(128),
	AttributeType NVARCHAR(128),
	AttributeTypeCode bigint,
	[Version] bigint,
	[TimeStamp] NVARCHAR(100),
	MetadataId NVARCHAR(100),
	Precision bigint,
	MaxLength bigint
  ) AS AttributeMetadata
WHERE StatusMetadata.EntityName = AttributeMetadata.EntityName 
  AND ''statuscode'' = AttributeMetadata.AttributeName'


EXEC sp_executesql @OptionSetsUnion 


END


/****************************************************************************************************************************************
Create external tables
*****************************************************************************************************************************************/

SET @ExternalTableCTE = ';WITH CTE AS (
SELECT Entities.name AS [Table Name]
	,CASE WHEN ROW_NUMBER() OVER (PARTITION BY Entities.name ORDER BY Entities.name) = 1
			THEN ''CREATE EXTERNAL TABLE [' + @Schema + '].[' +  @ExternalTablePrefix + ''' + Entities.name + ''' + @ExternalTableSuffix + '] ( ['' + Attributes.[name] + ''] '' 
	 + CASE Attributes.dataType
			WHEN ''guid'' THEN ''VARCHAR(36)''
			WHEN ''int64'' THEN ''bigint''
			WHEN ''dateTime'' THEN ''datetime2(7)''
			WHEN ''decimal'' THEN ''decimal(38,4)''
			WHEN ''double'' THEN ''float''
			WHEN ''boolean'' THEN ''VARCHAR(6)''
			WHEN ''string'' THEN
				CASE 
					WHEN [maxLength] IN (-1,9999) THEN ''varchar(max)''
					WHEN [maxLength] = 6 THEN ''varchar(7)''
					ELSE ''varchar('' + CAST([maxLength] AS varchar(4)) + '')'' 
				END
		END
			ELSE '',['' + Attributes.[name] + ''] '' + CASE Attributes.dataType
			WHEN ''guid'' THEN ''VARCHAR(36)''
			WHEN ''int64'' THEN ''bigint''
			WHEN ''dateTime'' THEN ''datetime2(7)''
			WHEN ''decimal'' THEN ''decimal(38,4)''
			WHEN ''double'' THEN ''float''
			WHEN ''boolean'' THEN ''VARCHAR(6)''
			WHEN ''string'' THEN
				CASE 
					WHEN [maxLength] IN (-1,9999) THEN ''varchar(max)'' 
					ELSE ''varchar('' + CAST([maxLength] AS varchar(4)) + '')'' END
				END
		END AS [SqlText]
		,ROW_NUMBER() OVER (PARTITION BY Entities.name ORDER BY Entities.name) AS Position
FROM 
  OPENROWSET(
    BULK ''/model.json'', 
    DATA_SOURCE = ''' + @ExternalDataSource + ''', FORMAT = ''CSV'', 
    FIELDQUOTE = ''0x0b'', FIELDTERMINATOR = ''0x0b'', 
    ROWTERMINATOR = ''0x0b''
  ) WITH (
    jsonContent varchar(MAX)
  ) AS [result] 
  CROSS APPLY OPENJSON(jsoncontent, ''$.entities'') WITH (
	[name] NVARCHAR(100)
	,[attributes] NVARCHAR(MAX) AS JSON
  ) AS Entities 
   CROSS APPLY OPENJSON(Entities.attributes) WITH (
    [name] NVARCHAR(100),
	[dataType] NVARCHAR(100),
	[maxLength] bigint
  ) AS Attributes
WHERE ((@Tables = '''') OR (Entities.name IN (SELECT [Value] FROM STRING_SPLIT(@Tables,'','')))))

,WithStatement AS (
SELECT [Table Name], [SqlText], [Position]
FROM CTE
UNION ALL
SELECT DISTINCT [Table Name],
'') WITH (DATA_SOURCE = [' + @ExternalDataSource + '],LOCATION = N'''''''''' + [Table Name] + ''/*.csv'''''''',FILE_FORMAT = [' + @ExternalFileFormat + '])'''''',
11111111
FROM CTE )

,ExternalTable AS (
SELECT ''DECLARE @'' + [Table Name] + '' NVARCHAR(MAX) = '''''' + STRING_AGG(CAST([SqlText] AS NVARCHAR(MAX)), CHAR(13)) WITHIN GROUP (ORDER BY [Table Name],[Position]) + CHAR(10) + ''EXEC dbo.sp_executesql @'' + [Table Name] + CHAR(10) AS ExternalTable
FROM WithStatement
GROUP BY [Table Name]
)
SELECT @Output = STRING_AGG(ET.ExternalTable,''; '' + CHAR(13) + CHAR(13))
FROM ExternalTable ET'

EXEC sp_executesql @ExternalTableCTE, N'@Tables NVARCHAR(MAX), @Output NVARCHAR(MAX) OUTPUT', @Tables = @Table, @Output = @ExternalTable OUTPUT

/****************************************************************************************************************************************
Find any existing external tables and drop them prior to recreating
*****************************************************************************************************************************************/

;WITH DROPTABLE AS (
SELECT 'DROP EXTERNAL TABLE [' + S.[name] + '].[' + ET.[name] + ']' AS [SqlText]
FROM sys.external_tables ET
INNER JOIN sys.schemas S ON ET.schema_id = S.schema_id
WHERE S.name = @Schema AND ET.[name] IN ((SELECT @ExternalTablePrefix + [Value] + @ExternalTableSuffix FROM STRING_SPLIT(@Table,',')))
UNION ALL
SELECT 'DROP EXTERNAL TABLE [' + S.[name] + '].[' + ET.[name] + ']' AS [SqlText]
FROM sys.external_tables ET
INNER JOIN sys.schemas S ON ET.schema_id = S.schema_id
WHERE S.name = @Schema AND ET.[name] LIKE '%' + @ExternalTablePrefix + '%' + @ExternalTableSuffix + '%' AND @Table = '' )
SELECT @DROPTABLE = STRING_AGG([SqlText],'; ' + CHAR(19) + CHAR(19))
FROM DROPTABLE

PRINT 'External Table Drops Started' 
EXEC sp_executesql @DROPTABLE
PRINT 'External Table Drops Ended, Creation Starting'
EXEC sp_executesql @ExternalTable
PRINT 'External Tables Created'

/****************************************************************************************************************************************
Create enhanced views
*****************************************************************************************************************************************/

SET @Join = 'SET @Output = 
(SELECT C.[name] AS [Column], ET.[name] AS [Table], C.[column_id] AS [Position], T.name AS [ColumnType], C.[max_length] AS [MaxLength]
FROM sys.columns C
INNER JOIN sys.external_tables ET ON C.object_id = ET.object_id
INNER JOIN sys.schemas S ON ET.schema_id = S.schema_id
INNER JOIN sys.types T ON C.user_type_id = T.user_type_id
WHERE S.name = @Schema AND  ET.[name] LIKE ''%'' + @ExternalTablePrefix + ''%'' + @ExternalTableSuffix + ''%''
FOR JSON PATH)'

EXEC sp_executesql @Join,N'@Schema NVARCHAR(128),@ExternalTablePrefix NVARCHAR(MAX),@ExternalTableSuffix NVARCHAR(MAX),@Output NVARCHAR(MAX) OUTPUT',@Schema = @Schema, @ExternalTablePrefix = @ExternalTablePrefix,@ExternalTableSuffix = @ExternalTableSuffix, @Output = @JoinOutput OUTPUT

SET @ViewLogic = ';WITH ViewLogic AS (
SELECT ''['' + JSON_VALUE(J.value, ''$.Column'') + '']'' AS [Column]
		,JSON_VALUE(J.value, ''$.Table'') AS [Table]
		,CAST(JSON_VALUE(J.value, ''$.Position'') AS INT) AS [Position]
		,CASE WHEN JSON_VALUE(J.value, ''$.Position'') = 1 THEN ''CREATE OR ALTER VIEW ['' + @Schema + ''].['' + REPLACE(REPLACE(JSON_VALUE(J.value, ''$.Table''),@ExternalTablePrefix,''''),@ExternalTableSuffix,'''') + ''] AS WITH '' + JSON_VALUE(J.value, ''$.Table'') + ''CTE AS ( SELECT ['' + JSON_VALUE(J.value, ''$.Column'') + '']'' ELSE '', ['' + JSON_VALUE(J.value, ''$.Column'') + '']'' END AS [SqlText] 
FROM OPENJSON(@JoinOutput) AS J 
INNER JOIN (SELECT JSON_VALUE(value, ''$.Table'') AS [Table]
						,COUNT(JSON_VALUE(value, ''$.Column'')) AS [ColumnCount]
				FROM OPENJSON(@JoinOutput)
				WHERE JSON_VALUE(value, ''$.ColumnType'') = ''varchar'' AND JSON_VALUE(value, ''$.MaxLength'') = 6
				GROUP BY JSON_VALUE(value, ''$.Table'')) AS T ON JSON_VALUE(J.value, ''$.Table'') = T.[Table]
UNION ALL
SELECT ''['' + JSON_VALUE(J.value, ''$.Column'') + '']'' AS [Column]
		,JSON_VALUE(J.value, ''$.Table'') AS [Table]
		,CAST(11111111 + JSON_VALUE(J.value, ''$.Position'') AS INT) AS [Position]
		,'', CAST(['' + JSON_VALUE(J.value, ''$.Column'') + ''] AS bit) AS ['' + JSON_VALUE(J.value, ''$.Column'') + ''JoinColumn]'' AS [SqlText]
FROM OPENJSON(@JoinOutput) AS J
WHERE JSON_VALUE(value, ''$.ColumnType'') = ''varchar'' AND JSON_VALUE(value, ''$.MaxLength'') = 6
UNION ALL
SELECT DISTINCT CAST('''' AS NVARCHAR(130))
		,JSON_VALUE(J.value, ''$.Table'') AS [Table]
		,CAST(22222222 AS INT) AS [Position]
		,'' FROM ['' + @Schema + ''].['' + JSON_VALUE(J.value, ''$.Table'') + ''])'' AS [SqlText]
FROM OPENJSON(@JoinOutput) J
INNER JOIN (SELECT JSON_VALUE(value, ''$.Table'') AS [Table]
						,COUNT(JSON_VALUE(value, ''$.Column'')) AS [ColumnCount]
				FROM OPENJSON(@JoinOutput)
				WHERE JSON_VALUE(value, ''$.ColumnType'') = ''varchar'' AND JSON_VALUE(value, ''$.MaxLength'') = 6
				GROUP BY JSON_VALUE(value, ''$.Table'')) AS T ON JSON_VALUE(J.value, ''$.Table'') = T.[Table]
UNION ALL
SELECT ''['' + JSON_VALUE(J.value, ''$.Column'') + '']'' AS [Column]
		,JSON_VALUE(J.value, ''$.Table'') AS [Table]
		,CAST(33333333 + JSON_VALUE(J.value, ''$.Position'') AS INT) AS [Position]
		,CASE WHEN T.[Table] IS NULL AND JSON_VALUE(J.value, ''$.Position'') = 1 THEN ''CREATE OR ALTER VIEW ['' + @Schema + ''].['' + REPLACE(REPLACE(JSON_VALUE(J.value, ''$.Table''),@ExternalTablePrefix,''''),@ExternalTableSuffix,'''') + ''] AS SELECT ['' + JSON_VALUE(J.value, ''$.Column'') + '']'' ELSE
		 CASE WHEN JSON_VALUE(J.value, ''$.Position'') = 1 THEN ''SELECT ['' + JSON_VALUE(J.value, ''$.Column'') + '']'' ELSE '', ['' +  JSON_VALUE(J.value, ''$.Column'') + '']'' END END AS [SqlText] 
FROM OPENJSON(@JoinOutput) AS J
LEFT OUTER JOIN (SELECT JSON_VALUE(value, ''$.Table'') AS [Table]
						,COUNT(JSON_VALUE(value, ''$.Column'')) AS [ColumnCount]
				FROM OPENJSON(@JoinOutput)
				WHERE JSON_VALUE(value, ''$.ColumnType'') = ''varchar'' AND JSON_VALUE(value, ''$.MaxLength'') = 6
				GROUP BY JSON_VALUE(value, ''$.Table'')) AS T ON JSON_VALUE(J.value, ''$.Table'') = T.[Table]
UNION ALL
SELECT [Field Name] AS [JoinColumn], 
	@ExternalTablePrefix + [Table Name] + @ExternalTableSuffix AS [Table], 
	CAST(44444444 AS INT) AS [Position], 
	'', ['' + [Field Name] + ''] AS ['' + [Field Name] + ''name]'' AS [SqlText]
FROM (SELECT Distinct [Field Name], [AttributeType], [Table Name]
	 FROM [' + @Schema + '].[OptionSetsUnion]
	 WHERE ((@Table = '''') OR [Table Name] IN (SELECT [Value] FROM STRING_SPLIT(@Table,'',''))) AND
	 ((@Columns = '''') OR [Field Name] IN (SELECT [Value] FROM STRING_SPLIT(@Columns,'','')))
	 )  T '

SET @ViewLogic = @ViewLogic + '
UNION ALL
SELECT DISTINCT CAST('''' AS NVARCHAR(130))
		,JSON_VALUE(J.value, ''$.Table'') AS [Table]
		,CAST(55555555 AS INT) AS [Position]
		,CASE WHEN T.[Table] IS NULL THEN ''FROM '' + JSON_VALUE(J.value, ''$.Table'') + '' AS T'' ELSE ''FROM '' + JSON_VALUE(J.value, ''$.Table'') + ''CTE AS T'' END AS [SqlText]
FROM OPENJSON(@JoinOutput) J
LEFT OUTER JOIN (SELECT JSON_VALUE(value, ''$.Table'') AS [Table]
						,COUNT(JSON_VALUE(value, ''$.Column'')) AS [ColumnCount]
				FROM OPENJSON(@JoinOutput)
				WHERE JSON_VALUE(value, ''$.ColumnType'') = ''varchar'' AND JSON_VALUE(value, ''$.MaxLength'') = 6
				GROUP BY JSON_VALUE(value, ''$.Table'')) AS T ON JSON_VALUE(J.value, ''$.Table'') = T.[Table]
UNION ALL
SELECT [Field Name] AS [JoinColumn], 
	@ExternalTablePrefix + [Table Name] + @ExternalTableSuffix AS [Table], 
	CAST(66666666 AS INT) AS [Position], 
	CASE WHEN [AttributeType] = ''Boolean'' THEN ''LEFT OUTER JOIN ['' + @Schema + ''].[OptionSetsunion] AS ['' + [Field Name] + '']'' + CHAR(13) + CHAR(9) + '' ON ['' + [Field Name] + ''].[Table Name] = '''''' + CHAR(39) + [Table Name] + CHAR(39) + '''''''' + CHAR(13) + CHAR(9) + '' AND ['' + [Field Name] + ''].[Field Name] = '''''' + CHAR(39) + [Field Name] + CHAR(39) + ''''''''  + CHAR(13) + CHAR(9) + '' AND [T].['' + [Field Name] + ''JoinColumn] = ['' + [Field Name] + ''].[Option ID]'' ELSE 
		''LEFT OUTER JOIN ['' + @Schema + ''].[OptionSetsUnion] AS ['' + [Field Name] + '']'' + CHAR(13) + CHAR(9) + '' ON ['' + [Field Name] + ''].[Table Name] = '''''' + CHAR(39)  + [Table Name] + CHAR(39) + '''''''' + CHAR(13) + CHAR(9) + '' AND ['' + [Field Name] + ''].[Field Name] = '''''' + CHAR(39) + [Field Name] + CHAR(39) + '''''''' + CHAR(13) + CHAR(9) + '' AND [T].['' + [Field Name] + ''] = ['' + [Field Name] + ''].[Option ID]'' END AS [SqlText]
FROM (SELECT Distinct [Field Name], [AttributeType], [Table Name]
	 FROM [' + @Schema + '].[OptionSetsUnion]
	 WHERE  ((@Table = '''') OR [Table Name] IN (SELECT [Value] FROM STRING_SPLIT(@Table,'',''))) AND
	 ((@Columns = '''') OR [Field Name] IN (SELECT [Value] FROM STRING_SPLIT(@Columns,'','')))
	 )  T
)
,ViewDefinition AS (
SELECT ''DECLARE @'' + [Table] + '' NVARCHAR(MAX) = '''''' + STRING_AGG(CONVERT(NVARCHAR(MAX),SqlText),CHAR(10)) WITHIN GROUP (ORDER BY [Table], [Position]) + '''''''' + CHAR(10) + ''EXEC sp_executesql @'' + [Table] + CHAR(10) AS ViewDefinition
FROM ViewLogic
GROUP BY [Table])
SELECT @ViewDefinition = STRING_AGG(ViewDefinition, '';'' + CHAR(10) + CHAR(10))
FROM ViewDefinition'



EXEC sp_executesql @ViewLogic, N'@JoinOutput NVARCHAR(MAX),@Table NVARCHAR(MAX),@Schema NVARCHAR(MAX), @Columns NVARCHAR(MAX),@ExternalTablePrefix NVARCHAR(MAX),@ExternalTableSuffix NVARCHAR(MAX),@ViewDefinition NVARCHAR(MAX) OUTPUT',@JoinOutput = @JoinOutput, @Table = @Table, @Schema = @Schema, @Columns = @Columns, @ExternalTablePrefix = @ExternalTablePrefix,@ExternalTableSuffix = @ExternalTableSuffix,@ViewDefinition = @ViewDefinition OUTPUT
PRINT 'View Creation Started'
EXEC sp_executesql @ViewDefinition
PRINT 'View Creation Complete'

END 
ELSE
BEGIN

/****************************************************************************************************************************************
Return if no table prefix or suffix is supplied
*****************************************************************************************************************************************/



SELECT '/*********************************************************************************************' + CHAR(13) AS [Error]
UNION ALL
SELECT 'Table prefix or suffix parameter is required' + CHAR(13)
UNION ALL
SELECT '*********************************************************************************************/' + CHAR(13)


END
END
ELSE 
BEGIN


/****************************************************************************************************************************************
Return if supplied external file format does not exist
*****************************************************************************************************************************************/

SELECT '/*********************************************************************************************' + CHAR(13) AS [Error]
UNION ALL
SELECT 'The supplied external file format does not exist' + CHAR(13)
UNION ALL
SELECT '*********************************************************************************************/' + CHAR(13)

END
END
ELSE
BEGIN

/****************************************************************************************************************************************
Return if supplied external data source does not exist
*****************************************************************************************************************************************/

SELECT '/*********************************************************************************************' + CHAR(13) AS [Error]
UNION ALL
SELECT 'The supplied external data source does not exist' + CHAR(13)
UNION ALL
SELECT '*********************************************************************************************/' + CHAR(13)


END


