/*

Extended-AdventureWorksDW.sql

Author: Derik Hammer
Website: www.sqlhammer.com
Revision date: 7/8/2018

Description: This script will create larger FactInternetSales and FactResellerSales tables to a configurable size. It will include extended associated dimensions as well.

Tested on:
	@@VERSION: Microsoft Azure SQL Data Warehouse - 10.0.9999.0 Jun 23 2018 00:50:42 Copyright (c) Microsoft Corporation
	AdventureWorksDW DBVersion: 10.00.3590.01
	AdventureWorksDW VersioNDate: 2012-07-07 19:49:42.000

Estimated run-time:
	1000 cDWU: x mins x secs
	2000 cDWU: x mins x secs
	3000 cDWU: x mins x secs
	4000 cDWU: x mins x secs
	5000 cDWU: x mins x secs
	6000 cDWU: x mins x secs

Instructions:





*/

/*

DROP EXTENDED OBJECTS

*/

IF OBJECT_ID('Extended.FactInternetSales','U') IS NOT NULL
	DROP TABLE Extended.FactInternetSales
IF OBJECT_ID('Extended.FactResellerSales','U') IS NOT NULL
	DROP TABLE Extended.FactResellerSales
IF OBJECT_ID('Extended.FactInternetSalesReason','U') IS NOT NULL
	DROP TABLE Extended.FactInternetSalesReason
IF OBJECT_ID('Extended.numbers') IS NOT NULL
	DROP TABLE Extended.numbers
IF EXISTS (SELECT * FROM sys.schemas AS s WHERE s.name = 'Extended')
	DROP SCHEMA Extended;

/*

CREATE NEW EXTENDED OBJECTS

*/

GO
CREATE SCHEMA Extended;
GO

CREATE TABLE Extended.FactInternetSales
WITH
(
    DISTRIBUTION = HASH ( [ProductKey] ),
	CLUSTERED COLUMNSTORE INDEX,
	PARTITION
	(
		[OrderDateKey] RANGE RIGHT FOR VALUES (20000101, 20010101, 20020101, 20030101, 20040101, 20050101, 20060101, 20070101, 20080101, 20090101, 20100101, 20110101, 20120101, 20130101, 20140101, 20150101, 20160101, 20170101, 20180101, 20190101, 20200101, 20210101, 20220101, 20230101, 20240101, 20250101, 20260101, 20270101, 20280101, 20290101)
	)
)
AS SELECT * FROM dbo.FactInternetSales;

CREATE TABLE Extended.FactResellerSales
WITH
(
	DISTRIBUTION = HASH ( [ProductKey] ),
	CLUSTERED COLUMNSTORE INDEX,
	PARTITION
	(
		[OrderDateKey] RANGE RIGHT FOR VALUES (20000101, 20010101, 20020101, 20030101, 20040101, 20050101, 20060101, 20070101, 20080101, 20090101, 20100101, 20110101, 20120101, 20130101, 20140101, 20150101, 20160101, 20170101, 20180101, 20190101, 20200101, 20210101, 20220101, 20230101, 20240101, 20250101, 20260101, 20270101, 20280101, 20290101)
	)
)
AS SELECT * FROM dbo.FactResellerSales;

CREATE TABLE Extended.FactInternetSalesReason
WITH
(
	DISTRIBUTION = HASH ( [SalesOrderNumber] ),
	CLUSTERED COLUMNSTORE INDEX
)
AS SELECT * FROM dbo.FactInternetSalesReason;

CREATE TABLE Extended.numbers 
(
	number BIGINT NOT NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED COLUMNSTORE INDEX
);

INSERT INTO Extended.numbers (number)
SELECT number
FROM (	SELECT TOP (999999999) number
		FROM 
		(
			SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) number
			FROM sys.columns c1
			CROSS APPLY sys.columns c2
		) numbers
		ORDER BY NEWID() DESC 
		UNION
		SELECT TOP (999999999) number
		FROM 
		(
			SELECT (ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) number
			FROM sys.columns c1
			CROSS APPLY sys.columns c2
		) numbers
		ORDER BY NEWID() DESC 
		UNION
		SELECT TOP (999999999) number
		FROM 
		(
			SELECT (ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) number
			FROM sys.columns c1
			CROSS APPLY sys.columns c2
		) numbers
		ORDER BY NEWID() DESC 
		UNION
		SELECT TOP (999999999) number
		FROM 
		(
			SELECT (ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) number
			FROM sys.columns c1
			CROSS APPLY sys.columns c2
		) numbers
		ORDER BY NEWID() DESC 
	) AS t1

SELECT COUNT_BIG(*) FROM Extended.numbers
--SELECT * FROM Extended.numbers

/*

EXTEND THE EXTENDED OBJECTS

*/

GO
BEGIN TRANSACTION

	DECLARE @MaxSalesOrderNumber BIGINT

	SELECT @MaxSalesOrderNumber = MAX(CAST(SUBSTRING(SalesOrderNumber,3,20) AS BIGINT))
	FROM [Extended].[FactInternetSales]

	INSERT INTO Extended.[FactInternetSales]
           ([ProductKey]
           ,[OrderDateKey]
           ,[DueDateKey]
           ,[ShipDateKey]
           ,[CustomerKey]
           ,[PromotionKey]
           ,[CurrencyKey]
           ,[SalesTerritoryKey]
           ,[SalesOrderNumber]
           ,[SalesOrderLineNumber]
           ,[RevisionNumber]
           ,[OrderQuantity]
           ,[UnitPrice]
           ,[ExtendedAmount]
           ,[UnitPriceDiscountPct]
           ,[DiscountAmount]
           ,[ProductStandardCost]
           ,[TotalProductCost]
           ,[SalesAmount]
           ,[TaxAmt]
           ,[Freight]
           ,[CarrierTrackingNumber]
           ,[CustomerPONumber])
	SELECT [ProductKey]
			,new_odk.DateKey
			,due_odk.DateKey
			,ship_odk.DateKey
			,[CustomerKey]
			,[PromotionKey]
			,[CurrencyKey]
			,[SalesTerritoryKey]
			,CAST('SO' + CAST((@MaxSalesOrderNumber + n.number) AS NVARCHAR(18)) AS NVARCHAR(20))
			,[SalesOrderLineNumber]
			,[RevisionNumber]
			,[OrderQuantity]
			,[UnitPrice]
			,[ExtendedAmount]
			,[UnitPriceDiscountPct]
			,[DiscountAmount]
			,[ProductStandardCost]
			,[TotalProductCost]
			,[SalesAmount]
			,[TaxAmt]
			,[Freight]
			,[CarrierTrackingNumber]
			,[CustomerPONumber]
		FROM [dbo].[FactInternetSales] AS fis
		CROSS APPLY Extended.numbers AS n
		INNER JOIN [dbo].[DimDate] AS odk ON odk.DateKey = fis.OrderDateKey
		INNER JOIN [dbo].[DimDate] AS new_odk ON new_odk.FullDateAlternateKey = DATEADD(DAY,(n.number%1825),odk.FullDateAlternateKey)
		INNER JOIN [dbo].[DimDate] AS due_odk ON due_odk.FullDateAlternateKey = DATEADD(DAY,(n.number%1825)+7,odk.FullDateAlternateKey)
		INNER JOIN [dbo].[DimDate] AS ship_odk ON ship_odk.FullDateAlternateKey = DATEADD(DAY,(n.number%1825)+(n.number%5),odk.FullDateAlternateKey)
		LEFT JOIN Extended.FactInternetSales AS fis2 ON fis.SalesOrderNumber = CAST('SO' + CAST((@MaxSalesOrderNumber + n.number) AS NVARCHAR(18)) AS NVARCHAR(20))
		WHERE fis2.SalesOrderNumber IS NULL

	SELECT COUNT_BIG(*) [TableCount], @@ROWCOUNT [InsertCount] FROM Extended.FactInternetSales

COMMIT
GO 10


/*

REBUILD INDEXES / OPTIMIZATIONS

*/



/*

CLEAN UP

*/

--IF OBJECT_ID('Extended.numbers') IS NOT NULL
--	DROP TABLE Extended.numbers

