-- Listing 2-1
CREATE TABLE dbo.Example
(
    Col1 INT
);
INSERT INTO dbo.Example
(
    Col1
)
VALUES
(1  );
SELECT e.Col1
FROM dbo.Example AS e;


--Listing 2-2
SELECT soh.AccountNumber,
       soh.OrderDate,
       soh.PurchaseOrderNumber,
       soh.SalesOrderNumber
FROM Sales.SalesOrderHeader AS soh
WHERE soh.SalesOrderID
BETWEEN 62500 AND 62550;


--Listing 2-3
SELECT soh.SalesOrderNumber,
       sod.OrderQty,
       sod.LineTotal,
       sod.UnitPrice,
       sod.UnitPriceDiscount,
       p.Name AS ProductName,
       p.ProductNumber,
       ps.Name AS ProductSubCategoryName,
       pc.Name AS ProductCategoryName
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
    JOIN Production.Product AS p
        ON sod.ProductID = p.ProductID
    JOIN Production.ProductModel AS pm
        ON p.ProductModelID = pm.ProductModelID
    JOIN Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    JOIN Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE soh.CustomerID = 29658;


--Listing 2-4
SELECT deqoi.counter,
       deqoi.occurrence,
       deqoi.value
FROM sys.dm_exec_query_optimizer_info AS deqoi;


--Listing 2-5
USE master;
EXEC sp_configure 'show advanced option', '1';
RECONFIGURE;
EXEC sp_configure 'max degree of parallelism', 2;
RECONFIGURE;


--Listing 2-6
SELECT e.ID,
       e.SomeValue
FROM dbo.Example AS e
WHERE e.ID = 42
OPTION (MAXDOP 2);


--Listing 2-7
USE master;
EXEC sp_configure 'show advanced option', '1';
RECONFIGURE;
EXEC sp_configure 'cost threshold for parallelism', 35;
RECONFIGURE;


--Listing 3-1
SELECT dest.text,
       deqp.query_plan,
       der.cpu_time,
       der.logical_reads,
       der.writes
FROM sys.dm_exec_requests AS der
    CROSS APPLY sys.dm_exec_query_plan(der.plan_handle) AS deqp
    CROSS APPLY sys.dm_exec_sql_text(der.plan_handle) AS dest;


--Listing 3-2
SELECT dest.text,
       deqp.query_plan,
       deqs.execution_count,
       deqs.min_logical_writes,
       deqs.max_logical_reads,
       deqs.total_logical_reads,
       deqs.total_elapsed_time,
       deqs.last_elapsed_time
FROM sys.dm_exec_query_stats AS deqs
    CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp
    CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest;


--Listing 3-3
CREATE EVENT SESSION [QueryPerformanceMetrics]
ON SERVER
    ADD EVENT sqlserver.rpc_completed
    (SET collect_statement = (1)
     WHERE ([sqlserver].[database_name] = N'Adventureworks')
    ),
    ADD EVENT sqlserver.sql_batch_completed
    (WHERE ([sqlserver].[database_name] = N'Adventureworks'))
    ADD TARGET package0.event_file
    (SET filename = N'QueryPerformanceMetrics', max_file_size = (2048));
GO


--Listing 3-4
ALTER EVENT SESSION QueryPerformanceMetrics ON SERVER STATE = START;

ALTER EVENT SESSION QueryPerformanceMetrics ON SERVER STATE = STOP;


--Listing 3-5
SELECT fx.object_name,
       fx.file_name,
       fx.event_data
FROM sys.fn_xe_file_target_read_file('.\QueryPerformanceMetrics_*.xel', NULL, NULL, NULL) AS fx;


USE AdventureWorks;
DBCC FREEPROCCACHE();


--Listing 4-1
SELECT soh.SalesOrderNumber,
       p.Name,
       sod.OrderQty
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON sod.SalesOrderID = soh.SalesOrderID
    JOIN Production.Product AS p
        ON p.ProductID = sod.ProductID
WHERE soh.CustomerID = 30052;


--Listing 4-2
SELECT dest.text,
       deqp.query_plan,
       deqs.execution_count,
       deqs.total_elapsed_time,
       deqs.last_elapsed_time
FROM sys.dm_exec_query_stats AS deqs
    CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp
    CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
WHERE dest.text LIKE 'SELECT soh.SalesOrderNumber,
       p.Name,%';


--Listing 4-3
SELECT qsq.query_id,
       qsq.query_hash,
       CAST(qsp.query_plan AS XML) AS QueryPlan
FROM sys.query_store_query AS qsq
    JOIN sys.query_store_plan AS qsp
        ON qsp.query_id = qsq.query_id
    JOIN sys.query_store_query_text AS qsqt
        ON qsqt.query_text_id = qsq.query_text_id
WHERE qsqt.query_sql_text LIKE 'SELECT soh.SalesOrderNumber,
       p.Name,%';


--Listing 4-4
SELECT pv.OnOrderQty,
       a.City
FROM Purchasing.ProductVendor AS pv,
     Person.Address AS a
WHERE a.City = 'Tulsa';


--Listing 4-5
SELECT *
FROM Production.UnitMeasure AS um;


--Listing 5-1
IF
(
    SELECT OBJECT_ID('Test1')
) IS NOT NULL
    DROP TABLE dbo.Test1;
GO
CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 INT IDENTITY
);

SELECT TOP 1500
       IDENTITY(INT, 1, 1) AS n
INTO #Nums
FROM master.dbo.syscolumns AS sC1,
     master.dbo.syscolumns AS sC2;

INSERT INTO dbo.Test1
(
    C1
)
SELECT n
FROM #Nums;

DROP TABLE #Nums;

CREATE NONCLUSTERED INDEX i1 ON dbo.Test1 (C1);


--Listing 5-2
SELECT t.C1,
       t.C2
FROM dbo.Test1 AS t
WHERE t.C1 = 2;
GO 50


--Listing 5-3
CREATE EVENT SESSION [Statistics]
ON SERVER
    ADD EVENT sqlserver.auto_stats
    (ACTION
     (
         sqlserver.sql_text
     )
     WHERE (sqlserver.database_name = N'AdventureWorks')
    ),
    ADD EVENT sqlserver.sql_batch_completed
    (WHERE (sqlserver.database_name = N'AdventureWorks'));
GO
ALTER EVENT SESSION [Statistics] ON SERVER STATE = START;


--Listing 5-4
INSERT INTO dbo.Test1
(
    C1
)
VALUES
(2  );

--Listing 5-5
SELECT TOP 1500
       IDENTITY(INT, 1, 1) AS n
INTO #Nums
FROM master.dbo.syscolumns AS scl,
     master.dbo.syscolumns AS sC2;
INSERT INTO dbo.Test1
(
    C1
)
SELECT 2
FROM #Nums;
DROP TABLE #Nums;


--Listing 5-6
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS OFF;


--Listing 5-7
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS ON;


--Listing 5-8
IF
(
    SELECT OBJECT_ID('dbo.Test1')
) IS NOT NULL
    DROP TABLE dbo.Test1;
GO

CREATE TABLE dbo.Test1
(
    Test1_C1 INT IDENTITY,
    Test1_C2 INT
);

INSERT INTO dbo.Test1
(
    Test1_C2
)
VALUES
(1  );

SELECT TOP 10000
       IDENTITY(INT, 1, 1) AS n
INTO #Nums
FROM master.dbo.syscolumns AS scl,
     master.dbo.syscolumns AS sC2;

INSERT INTO dbo.Test1
(
    Test1_C2
)
SELECT 2
FROM #Nums;
GO

CREATE CLUSTERED INDEX i1 ON dbo.Test1 (Test1_C1);

--Create second table with 10001 rows, -- but opposite data distribution 
IF
(
    SELECT OBJECT_ID('dbo.Test2')
) IS NOT NULL
    DROP TABLE dbo.Test2;
GO

CREATE TABLE dbo.Test2
(
    Test2_C1 INT IDENTITY,
    Test2_C2 INT
);

INSERT INTO dbo.Test2
(
    Test2_C2
)
VALUES
(2  );

INSERT INTO dbo.Test2
(
    Test2_C2
)
SELECT 1
FROM #Nums;
DROP TABLE #Nums;
GO

CREATE CLUSTERED INDEX il ON dbo.Test2 (Test2_C1);


--Listing 5-9
SELECT DATABASEPROPERTYEX('AdventureWorks', 'IsAutoCreateStatistics');


--Listing 5-10
ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS ON;




ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;


--Listing 5-11
SELECT t1.Test1_C2,
       t2.Test2_C2
FROM dbo.Test1 AS t1
    JOIN dbo.Test2 AS t2
        ON t1.Test1_C2 = t2.Test2_C2
WHERE t1.Test1_C2 = 2;
GO 50

--Listing 5-12
SELECT s.name,
       s.auto_created,
       s.user_created
FROM sys.stats AS s
WHERE object_id = OBJECT_ID('Test1');


--Listing 5-13
SELECT t1.Test1_C2,
       t2.Test2_C2
FROM dbo.Test1 AS t1
    JOIN dbo.Test2 AS t2
        ON t1.Test1_C2 = t2.Test2_C2
WHERE t1.Test1_C2 = 1;


--Listing 5-14
ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS OFF;

ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS ON;


--Listing 5-15
IF
(
    SELECT OBJECT_ID('dbo.Test1')
) IS NOT NULL
    DROP TABLE dbo.Test1;
GO

CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 INT IDENTITY
);

INSERT INTO dbo.Test1
(
    C1
)
VALUES
(1  );

SELECT TOP 10000
       IDENTITY(INT, 1, 1) AS n
INTO #Nums
FROM master.dbo.syscolumns sc1,
     master.dbo.syscolumns sc2;

INSERT INTO dbo.Test1
(
    C1
)
SELECT 2
FROM #Nums;

DROP TABLE #Nums;

CREATE NONCLUSTERED INDEX FirstIndex ON dbo.Test1 (C1);


--Listing 5-16
DBCC SHOW_STATISTICS(Test1,FirstIndex);


--Listing 5-17
SELECT 1.0 / COUNT(DISTINCT C1)
FROM dbo.Test1;


--Listing 5-18
DBCC SHOW_STATISTICS('Sales.SalesOrderDetail','IX_SalesOrderDetail_ProductID');


--Listing 5-19
CREATE EVENT SESSION [CardinalityEstimation]
ON SERVER
    ADD EVENT sqlserver.auto_stats
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.query_optimizer_estimate_cardinality
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sql_batch_completed
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sql_batch_starting
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks'))
    ADD TARGET package0.event_file
    (SET filename = N'cardinalityestimation')
WITH
(
    TRACK_CAUSALITY = ON
);
GO


--Listing 5-20
SELECT so.Description,
       p.Name AS ProductName,
       p.ListPrice,
       p.Size,
       pv.AverageLeadTime,
       pv.MaxOrderQty,
       v.Name AS VendorName
FROM Sales.SpecialOffer AS so
    JOIN Sales.SpecialOfferProduct AS sop
        ON sop.SpecialOfferID = so.SpecialOfferID
    JOIN Production.Product AS p
        ON p.ProductID = sop.ProductID
    JOIN Purchasing.ProductVendor AS pv
        ON pv.ProductID = p.ProductID
    JOIN Purchasing.Vendor AS v
        ON v.BusinessEntityID = pv.BusinessEntityID
WHERE so.DiscountPct > .15;


--Listing 5-21
SELECT p.Name,
       p.Class
FROM Production.Product AS p
WHERE p.Color = 'Red'
      AND p.DaysToManufacture > 2;


--Listing 5-22
SELECT s.name,
       s.auto_created,
       s.user_created,
       s.filter_definition,
       sc.column_id,
       c.name AS ColumnName
FROM sys.stats AS s
    JOIN sys.stats_columns AS sc
        ON sc.stats_id = s.stats_id
           AND sc.object_id = s.object_id
    JOIN sys.columns AS c
        ON c.column_id = sc.column_id
           AND c.object_id = s.object_id
WHERE s.object_id = OBJECT_ID('Production.Product');

--Listing 5-23
CREATE NONCLUSTERED INDEX FirstIndex
ON dbo.Test1 (
                 C1,
                 C2
             )
WITH (DROP_EXISTING = ON);


DBCC SHOW_STATISTICS(Test1,FirstIndex);


--Listing 5-24
CREATE INDEX IX_Test ON Sales.SalesOrderHeader (PurchaseOrderNumber);

DBCC SHOW_STATISTICS('sales.SalesOrderHeader','IX_Test');


--Listing 5-25
CREATE INDEX IX_Test
ON Sales.SalesOrderHeader (PurchaseOrderNumber)
WHERE PurchaseOrderNumber IS NOT NULL
WITH (DROP_EXISTING = ON);


--Listing 5-26
DROP INDEX Sales.SalesOrderHeader.IX_Test;


--Listing 5-27
ALTER DATABASE AdventureWorks SET COMPATIBILITY_LEVEL = 110;


--Listing 5-28
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = ON;


--Listing 5-29
SELECT p.Name,
       p.Class
FROM Production.Product AS p
WHERE p.Color = 'Red'
      AND p.DaysToManufacture > 15
OPTION (USE HINT ('FORCE_LEGACY_CARDINALITY_ESTIMATION'));


--Listing 5-30
ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS OFF;


--Listing 5-31
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS OFF;


--Listing 5-32
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS_ASYNC ON;


--Listing 5-33
USE AdventureWorks;
EXEC sp_autostats 
    'HumanResources.Department',
    'OFF';


--Listing 5-34
EXEC sp_autostats 
    'HumanResources.Department',
    'OFF',
    AK_Department_Name;


--Listing 5-35
EXEC sp_autostats 'HumanResources.Department';


--Listing 5-36
EXEC sp_autostats 
    'HumanResources.Department',
    'ON';
EXEC sp_autostats 
    'HumanResources.Department',
    'ON',
    AK_Department_Name;


--Listing 5-37
UPDATE STATISTICS dbo.bigProduct
WITH RESAMPLE,
     INCREMENTAL = ON;

--Listing 5-38
ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS OFF;
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS OFF;
GO

IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'dbo.Test1')
)
    DROP TABLE dbo.Test1;
GO

CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 INT,
    C3 CHAR(50)
);
INSERT INTO dbo.Test1
(
    C1,
    C2,
    C3
)
VALUES
(51, 1, 'C3'),
(52, 1, 'C3');

CREATE NONCLUSTERED INDEX iFirstIndex ON dbo.Test1 (C1, C2);

SELECT TOP 10000
       IDENTITY(INT, 1, 1) AS n
INTO #Nums
FROM master.dbo.syscolumns AS scl,
     master.dbo.syscolumns AS sC2;

INSERT INTO dbo.Test1
(
    C1,
    C2,
    C3
)
SELECT n % 50,
       n,
       'C3'
FROM #Nums;
DROP TABLE #Nums;


--Listing 5-39
SELECT t.C1,
       t.C2,
	   t.C3
FROM dbo.Test1 AS t
WHERE t.C2 = 1;
GO 50
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE


--Listing 5-40
CREATE STATISTICS Stats1 ON Test1(C2);


--Listing 5-41
DECLARE @Planhandle VARBINARY(64);

SELECT @Planhandle = deqs.plan_handle
FROM sys.dm_exec_query_stats AS deqs
    CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
WHERE dest.text = 'SELECT  *
FROM    dbo.Test1
WHERE   C2 = 1;';

IF @Planhandle IS NOT NULL
BEGIN
    DBCC FREEPROCCACHE(@Planhandle);
END;
GO

--Listing 5-42
DBCC SHOW_STATISTICS (Test1, iFirstIndex);


--Listing 5-43
SELECT C1,
       C2,
       C3
FROM dbo.Test1
WHERE C1 = 51;
GO 50

--Listing 5-44
UPDATE STATISTICS Test1 iFirstIndex
WITH FULLSCAN;

--Listing 5-45
ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS ON;
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS ON;


--Listing 6-1
ALTER DATABASE AdventureWorks SET QUERY_STORE = ON;


--Listing 6-2
SELECT qsq.query_id,
       qsq.object_id,
       qsqt.query_sql_text,
	   qsp.plan_id,
       CAST(qsp.query_plan AS XML) AS QueryPlan
FROM sys.query_store_query AS qsq
    JOIN sys.query_store_query_text AS qsqt
        ON qsq.query_text_id = qsqt.query_text_id
    JOIN sys.query_store_plan AS qsp
        ON qsp.query_id = qsq.query_id
WHERE qsq.object_id = OBJECT_ID('dbo.ProductTransactionHistoryByReference');

GO


--Listing 6-3
CREATE OR ALTER PROC dbo.ProductTransactionHistoryByReference
(@ReferenceOrderID int)
AS
BEGIN
    SELECT p.Name,
           p.ProductNumber,
           th.ReferenceOrderID
    FROM Production.Product AS p
        JOIN Production.TransactionHistory AS th
            ON th.ProductID = p.ProductID
    WHERE th.ReferenceOrderID = @ReferenceOrderID;
END;


GO

--Listing 6-4
SELECT a.AddressID,
       a.AddressLine1
FROM Person.Address AS a
WHERE a.AddressID = 72;


--Listing 6-5
SELECT qsq.query_id,
       qsq.query_hash,
       qsqt.query_sql_text,
       qsq.query_parameterization_type
FROM sys.query_store_query_text AS qsqt
    JOIN sys.query_store_query AS qsq
        ON qsq.query_text_id = qsqt.query_text_id
    JOIN sys.fn_stmt_sql_handle_from_sql_stmt(
             'SELECT a.AddressID,
       a.AddressLine1
FROM Person.Address AS a
WHERE a.AddressID = 72;',
             2)  AS fsshfss
        ON fsshfss.statement_sql_handle = qsqt.statement_sql_handle;


--Listing 6-6
DECLARE @CompareTime DATETIME = '2021-11-28 15:55';

SELECT CAST(qsp.query_plan AS XML),
       qsrs.count_executions,
       qsrs.avg_duration,
       qsrs.stdev_duration,
       qsws.wait_category_desc,
       qsws.avg_query_wait_time_ms,
       qsws.stdev_query_wait_time_ms
FROM sys.query_store_plan AS qsp
    JOIN sys.query_store_runtime_stats AS qsrs
        ON qsrs.plan_id = qsp.plan_id
    JOIN sys.query_store_runtime_stats_interval AS qsrsi
        ON qsrsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
    JOIN sys.query_store_wait_stats AS qsws
        ON qsws.plan_id = qsrs.plan_id
           AND qsws.plan_id = qsrs.plan_id
           AND qsws.execution_type = qsrs.execution_type
           AND qsws.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
WHERE qsp.plan_id = 329
      AND @CompareTime
      BETWEEN qsrsi.start_time AND qsrsi.end_time;


SELECT GETDATE();


--Listing 6-7
WITH QSAggregate
AS (SELECT qsrs.plan_id,
           SUM(qsrs.count_executions) AS CountExecutions,
           AVG(qsrs.avg_duration) AS AvgDuration,
           AVG(qsrs.stdev_duration) AS StDevDuration,
           qsws.wait_category_desc,
           AVG(qsws.avg_query_wait_time_ms) AS AvgQueryWaitTime,
           AVG(qsws.stdev_query_wait_time_ms) AS StDevQueryWaitTime
    FROM sys.query_store_runtime_stats AS qsrs
        LEFT JOIN sys.query_store_wait_stats AS qsws
            ON qsws.plan_id = qsrs.plan_id
               AND qsws.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
    GROUP BY qsrs.plan_id,
             qsws.wait_category_desc)
SELECT CAST(qsp.query_plan AS XML),
       qsa.*
FROM sys.query_store_plan AS qsp
    JOIN QSAggregate AS qsa
        ON qsa.plan_id = qsp.plan_id
WHERE qsp.plan_id = 329;

EXEC sys.sp_query_store_flush_db

--Listing 6-8
ALTER DATABASE AdventureWorks SET QUERY_STORE CLEAR;


--Listing 6-9
EXEC sys.sp_query_store_remove_query @query_id = @QueryId;
EXEC sys.sp_query_store_remove_plan @plan_id = @PlanID;


--Listing 6-10
EXEC sys.sp_query_store_flush_db;


--Listing 6-11
SELECT *
FROM sys.database_query_store_options AS dqso;


--Listing 6-12
ALTER DATABASE AdventureWorks SET QUERY_STORE (MAX_STORAGE_SIZE_MB = 200);


--Listing 6-13
EXEC sys.sp_query_store_force_plan 550, 339;
EXEC sys.sp_query_store_unforce_plan 550, 339;



--Listing 6-14
EXEC sp_query_store_set_hints 550, N'OPTION(OPTIMIZE FOR UNKOWN)';


--Listing 6-15
SELECT qsqh.query_hint_id,
       qsqh.query_id,
       qsqh.query_hint_text,
       qsqh.source_desc
FROM sys.query_store_query_hints AS qsqh;


--Listing 6-16
EXEC sp_query_store_clear_hints @query_id = 550;


--Listing 7-1
SELECT decp.refcounts,
       decp.usecounts,
       decp.size_in_bytes,
       decp.cacheobjtype,
       decp.objtype,
       decp.plan_handle
FROM sys.dm_exec_cached_plans AS decp;


--Listing 7-2
SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID = 29690
      AND sod.ProductID = 711;



ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE


--Listing 7-3
SELECT c.usecounts,
       c.cacheobjtype,
       c.objtype
FROM sys.dm_exec_cached_plans AS c
    CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t
WHERE t.text = 'SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID = 29690
      AND sod.ProductID = 711;';


--Listing 7-4
SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID = 29500
      AND sod.ProductID = 711;


--Listing 7-5
SELECT c.usecounts,
       c.cacheobjtype,
       c.objtype,
       t.text,
       c.plan_handle
FROM sys.dm_exec_cached_plans AS c
    CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t
WHERE t.text LIKE 'SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID%';

--Listing 7-6
EXEC sys.sp_configure 'show advanced option', '1';
GO
RECONFIGURE;
GO
EXEC sys.sp_configure 'optimize for ad hoc workloads', 1;
GO
RECONFIGURE;



SELECT c.usecounts,
       c.cacheobjtype,
       c.objtype,
	   c.size_in_bytes
FROM sys.dm_exec_cached_plans AS c
    CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t
WHERE t.text = 'SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID = 29690
      AND sod.ProductID = 711;';


--Listing 7-7
EXEC sp_configure 'optimize for ad hoc workloads', 0;
GO
RECONFIGURE;
GO
EXEC sp_configure 'show advanced option', '0';
GO
RECONFIGURE;


--Listing 7-8
DBCC FREEPROCCACHE();
GO
SELECT a.AddressLine1,
       a.City,
       a.StateProvinceID
FROM Person.Address AS a
WHERE a.AddressID = 42;
GO
SELECT c.usecounts,
       c.cacheobjtype,
       c.objtype,
       t.text
FROM sys.dm_exec_cached_plans AS c
    CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t;


--Listing 7-9
DBCC FREEPROCCACHE();
GO
SELECT a.AddressLine1,
       a.City,
       a.StateProvinceID
FROM Person.Address AS a
WHERE a.AddressID = 42;
GO
SELECT a.AddressLine1,
       a.City,
       a.StateProvinceID
FROM Person.Address AS a
WHERE a.AddressID = 32509;
GO
SELECT c.usecounts,
       c.cacheobjtype,
       c.objtype,
       t.text
FROM sys.dm_exec_cached_plans AS c
    CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t;


--Listing 7-10
DBCC FREEPROCCACHE();
GO
SELECT a.AddressLine1,
       a.City,
       a.StateProvinceID
FROM Person.Address AS a
WHERE a.AddressID = 42;
GO
SELECT a.AddressLine1,
       a.City,
       a.StateProvinceID
FROM Person.Address AS a
WHERE a.AddressID = 32509;
GO
SELECT a.AddressLine1,
       a.City,
       a.StateProvinceID
FROM Person.Address AS a
WHERE a.AddressID = 56;
GO
SELECT c.usecounts,
       c.cacheobjtype,
       c.objtype,
       t.text
FROM sys.dm_exec_cached_plans AS c
    CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t;



--Listing 7-11
DBCC FREEPROCCACHE();
GO
SELECT a.AddressLine1,
       a.PostalCode
FROM Person.Address AS a
WHERE a.AddressID
BETWEEN 40 AND 60;
GO
SELECT c.usecounts,
       c.cacheobjtype,
       c.objtype,
       t.text
FROM sys.dm_exec_cached_plans AS c
    CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t;


--Listing 7-12
DBCC FREEPROCCACHE();
GO
SELECT a.AddressLine1,
       a.PostalCode
FROM Person.Address AS a
WHERE a.AddressID
BETWEEN 40 AND 60;
GO
SELECT a.AddressLine1,
       a.PostalCode
FROM Person.Address AS a
WHERE a.AddressID >= 40
      AND a.AddressID <= 60;
GO
SELECT c.usecounts,
       c.cacheobjtype,
       c.objtype,
       t.text
FROM sys.dm_exec_cached_plans AS c
    CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t;


--Listing 7-13
ALTER DATABASE AdventureWorks SET PARAMETERIZATION FORCED;


--Listing 7-14
DBCC FREEPROCCACHE();
GO
SELECT ea.EmailAddress,
       e.BirthDate,
       a.City
FROM Person.Person AS p
    JOIN HumanResources.Employee AS e
        ON p.BusinessEntityID = e.BusinessEntityID
    JOIN Person.BusinessEntityAddress AS bea
        ON e.BusinessEntityID = bea.BusinessEntityID
    JOIN Person.Address AS a
        ON bea.AddressID = a.AddressID
    JOIN Person.StateProvince AS sp
        ON a.StateProvinceID = sp.StateProvinceID
    JOIN Person.EmailAddress AS ea
        ON p.BusinessEntityID = ea.BusinessEntityID
WHERE ea.EmailAddress LIKE 'david%'
      AND sp.StateProvinceCode = 'WA';
GO
SELECT c.usecounts,
       c.cacheobjtype,
       c.objtype,
       t.text
FROM sys.dm_exec_cached_plans AS c
    CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t;


--Listing 7-15
ALTER DATABASE AdventureWorks SET PARAMETERIZATION SIMPLE;


GO
--Listing 7-16
CREATE OR ALTER PROC dbo.BasicSalesInfo
    @ProductID INT,
    @CustomerID INT
AS
SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID = @CustomerID
      AND sod.ProductID = @ProductID;


GO
--Listing 7-17
EXEC dbo.BasicSalesInfo @CustomerID = 29690, @ProductID = 711;

DBCC FREEPROCCACHE();


--Listing 7-18
CREATE OR ALTER PROCEDURE dbo.MyNewProc
AS
SELECT MyID
FROM dbo.NotHere; --Table dbo.NotHere doesn't exist


--Listing 7-19
DECLARE @query NVARCHAR(MAX),
        @paramlist NVARCHAR(MAX);

SET @query
    = N'SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID = @CustomerID
      AND sod.ProductID = @ProductID';

SET @paramlist = N'@CustomerID INT, @ProductID INT';

EXEC sys.sp_executesql @query,
                       @paramlist,
                       @CustomerID = 29690,
                       @ProductID = 711;


DBCC FREEPROCCACHE();

--Listing 7-20
SELECT c.usecounts,
       c.cacheobjtype,
       c.objtype,
       t.text
FROM sys.dm_exec_cached_plans AS c
    CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t
WHERE text LIKE '(@CustomerID%';

DECLARE @query NVARCHAR(MAX),
        @paramlist NVARCHAR(MAX);

SET @query
    = N'SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID = @CustomerID
      AND sod.ProductID = @ProductID';

SET @paramlist = N'@CustomerID INT, @ProductID INT';

EXEC sys.sp_executesql @query,
                       @paramlist,
                       @CustomerID = 29690,
                       @ProductID = 777;


DECLARE @query NVARCHAR(MAX),
        @paramlist NVARCHAR(MAX);

SET @query
    = N'SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID = @CustomerID
      AND sod.ProductID = @ProductID';

SET @paramlist = N'@customerid INT, @ProductID INT';

EXEC sys.sp_executesql @query,
                       @paramlist,
                       @CustomerID = 29690,
                       @ProductID = 711;


--Listing 7-21
SELECT p.Name AS ProductName,
       ps.Name AS SubCategory,
       pc.Name AS Category
FROM Production.Product AS p
    JOIN Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    JOIN Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE pc.Name = 'Bikes'
      AND ps.Name = 'Touring Bikes';

SELECT p.Name AS ProductName,
       ps.Name AS SubCategory,
       pc.Name AS Category
FROM Production.Product AS p
    JOIN Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    JOIN Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
where pc.Name = 'Bikes'
      and ps.Name = 'Road Bikes';

DBCC FREEPROCCACHE();

--Listing 7-22
SELECT deqs.execution_count,
       deqs.query_hash,
       deqs.query_plan_hash,
       dest.text
FROM sys.dm_exec_query_stats AS deqs
    CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) AS dest
WHERE dest.text LIKE 'SELECT p.Name AS ProductName%';


--Listing 7-23
SELECT p.Name AS ProductName
FROM Production.Product AS p
    JOIN Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    JOIN Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE pc.Name = 'Bikes'
      AND ps.Name = 'Touring Bikes';


--Listing 7-24
SELECT p.Name,
       tha.TransactionDate,
       tha.TransactionType,
       tha.Quantity,
       tha.ActualCost
FROM Production.TransactionHistoryArchive AS tha
    JOIN Production.Product AS p
        ON tha.ProductID = p.ProductID
WHERE p.ProductID = 461;


SELECT p.Name,
       tha.TransactionDate,
       tha.TransactionType,
       tha.Quantity,
       tha.ActualCost
FROM Production.TransactionHistoryArchive AS tha
    JOIN Production.Product AS p
        ON tha.ProductID = p.ProductID
WHERE p.ProductID = 712;



SELECT deqs.execution_count,
       deqs.query_hash,
       deqs.query_plan_hash,
       dest.text
FROM sys.dm_exec_query_stats AS deqs
    CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) AS dest
WHERE dest.text LIKE 'SELECT p.Name,%';



--Listing 8-1
CREATE OR ALTER PROCEDURE dbo.WorkOrder
AS
SELECT wo.WorkOrderID,
       wo.ProductID,
       wo.StockedQty
FROM Production.WorkOrder AS wo
WHERE wo.StockedQty
BETWEEN 500 AND 700;


EXEC dbo.WorkOrder;
DROP PROCEDURE dbo.WorkOrder;


--Listing 8-2
CREATE INDEX IX_Test ON Production.WorkOrder (StockedQty, ProductID);


--Listing 8-3
DROP INDEX Production.WorkOrder.IX_Test;


--Listing 8-4
CREATE OR ALTER PROCEDURE dbo.WorkOrderAll
AS
--intentionally using SELECT * as an example
SELECT *
FROM Production.WorkOrder AS wo;


--Listing 8-5
CREATE EVENT SESSION [QueryAndRecompile]
ON SERVER
    ADD EVENT sqlserver.rpc_completed
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.rpc_starting
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sp_statement_completed
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sp_statement_starting
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sql_batch_completed
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sql_batch_starting
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sql_statement_completed
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sql_statement_recompile
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sql_statement_starting
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks'))
    ADD TARGET package0.event_file
    (SET filename = N'QueryAndRecompile')
WITH
(
    TRACK_CAUSALITY = ON
);
GO


--Listing 8-6
EXEC dbo.WorkOrderAll;
GO
CREATE INDEX IX_Test ON Production.WorkOrder(StockedQty,ProductID);
GO
EXEC dbo.WorkOrderAll; --After creation of index IX_Test


DROP INDEX Production.WorkOrder.IX_Test;


--Listing 8-7
SELECT dxmv.map_value
FROM sys.dm_xe_map_values AS dxmv
WHERE dxmv.name = 'statement_recompile_cause';


--Listing 8-8
CREATE OR ALTER PROC dbo.RecompileTable
AS
CREATE TABLE dbo.ProcTest1
(
    C1 INT
);
SELECT *
FROM dbo.ProcTest1;
DROP TABLE dbo.ProcTest1;
GO

EXEC dbo.RecompileTable; --First execution 
EXEC dbo.RecompileTable; --Second execution



--Listing 8-9
CREATE OR ALTER PROC dbo.RecompileTemp
AS
CREATE TABLE #TempTable
(
    C1 INT
);
INSERT INTO #TempTable
(
    C1
)
VALUES
(42 );
GO

EXEC dbo.RecompileTemp;
EXEC dbo.RecompileTemp;



GO
--Listing 8-10
CREATE OR ALTER PROC dbo.TempTable
AS
--All statements are compiled initially
CREATE TABLE #MyTempTable
(
    ID INT,
    Dsc NVARCHAR(50)
);
--This statement must be recompiled
INSERT INTO #MyTempTable
(
    ID,
    Dsc
)
SELECT pm.ProductModelID,
       pm.Name
FROM Production.ProductModel AS pm; 

--This statement must be recompiled
SELECT mtt.ID,
       mtt.Dsc
FROM #MyTempTable AS mtt;
CREATE CLUSTERED INDEX iTest ON #MyTempTable (ID);

--Creating index causes a recompile
SELECT mtt.ID,
       mtt.Dsc
FROM #MyTempTable AS mtt;

CREATE TABLE #t2
(
    c1 INT
);
--Recompile from a new table
SELECT c1
FROM #t2;
GO

EXEC dbo.TempTable;


GO
--Listing 8-11
IF
(
    SELECT OBJECT_ID('dbo.Test1')
) IS NOT NULL
    DROP TABLE dbo.Test1;
GO
CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 CHAR(50)
);
INSERT INTO dbo.Test1
VALUES
(1, '2');
CREATE NONCLUSTERED INDEX IndexOne ON dbo.Test1 (C1);
GO
--Create a stored procedure referencing the previous table 
CREATE OR ALTER PROC dbo.TestProc
AS
SELECT t.C1,
       t.C2
FROM dbo.Test1 AS t
WHERE t.C1 = 1
OPTION (KEEPFIXED PLAN);
GO

--First execution of stored procedure with 1 row in the table 
EXEC dbo.TestProc; --First execution

--Add many rows to the table to cause statistics change 
WITH Nums
AS (SELECT 1 AS n
    UNION ALL
    SELECT Nums.n + 1
    FROM Nums
    WHERE Nums.n < 1000)
INSERT INTO dbo.Test1
(
    C1,
    C2
)
SELECT 1,
       Nums.n
FROM Nums
OPTION (MAXRECURSION 1000);
GO

--Reexecute the stored procedure with a change in statistics 
EXEC dbo.TestProc;


--Listing 8-12
EXEC sys.sp_autostats 'dbo.Test1', 'OFF';


--Listing 8-13
CREATE TABLE #TempTable
(
    C1 INT PRIMARY KEY
);

SET @Count = 1;
WHILE @Count < 8
BEGIN
    INSERT INTO #TempTable
    (
        C1
    )
    VALUES
    (@Count );

    SELECT tt.C1
    FROM #TempTable AS tt
	JOIN Production.ProductModel AS pm
	ON pm.ProductModelID = tt.C1
	WHERE tt.C1 < @Count;

    SET @Count += 1;
END;

DROP TABLE #TempTable;


--Listing 8-14
DECLARE @TempTable TABLE
(
    C1 INT PRIMARY KEY
);

DECLARE @Count TINYINT = 1;
WHILE @Count < 8
BEGIN
    INSERT INTO @TempTable
    (
        C1
    )
    VALUES
    (@Count );

    SELECT tt.C1
    FROM @TempTable AS tt
	JOIN Production.ProductModel AS pm
	ON pm.ProductModelID = tt.C1
	WHERE tt.C1 < @Count;

    SET @Count += 1;
END;

GO

--Listing 8-15
CREATE OR ALTER PROC dbo.OuterProc
AS
CREATE TABLE #Scope
(ID INT PRIMARY KEY,
ScopeName VARCHAR(50));

EXEC dbo.InnerProc
GO

CREATE OR ALTER PROC dbo.InnerProc
AS

INSERT INTO #Scope
(
    ID,
    ScopeName
)
VALUES
(   1,   -- ID - int
    'InnerProc' -- ScopeName - varchar(50)
    );

SELECT s.ScopeName
FROM #Scope AS s;
GO

EXEC dbo.OuterProc;
EXEC dbo.OuterProc;


DBCC FREEPROCCACHE;
GO


--Listing 8-16
CREATE OR ALTER PROC dbo.TestProc
AS
SELECT 'a' + NULL + 'b'; --1st 
SET CONCAT_NULL_YIELDS_NULL OFF;
SELECT 'a' + NULL + 'b'; --2nd 
SET ANSI_NULLS OFF;
SELECT 'a' + NULL + 'b';--3rd 
GO

EXEC dbo.TestProc; --First execution 
EXEC dbo.TestProc; --Second execution


GO
--Listing 8-17
CREATE OR ALTER PROCEDURE dbo.CustomerList @CustomerID INT
AS
SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID >= @CustomerID
OPTION (OPTIMIZE FOR (@CustomerID = 1));


--Listing 8-18
EXEC dbo.CustomerList @CustomerID = 7920 WITH RECOMPILE;
EXEC dbo.CustomerList @CustomerID = 30118 WITH RECOMPILE;


--Listing 8-19
CREATE OR ALTER PROCEDURE dbo.CustomerList @CustomerID INT
AS
SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID >= @CustomerID;


--Listing 8-20
sp_create_plan_guide @name = N'MyGuide',
                     @stmt = N'SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID >= @CustomerID;',
                     @type = N'OBJECT',
                     @module_or_batch = N'dbo.CustomerList',
                     @params = NULL,
                     @hints = N'OPTION (OPTIMIZE FOR (@CustomerID = 1))';


--Listing 8-21
SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID >= 1;


--Listing 8-22
EXECUTE sp_create_plan_guide @name = N'MySQLGuide',
                             @stmt = N'SELECT soh.SalesOrderNumber,
       soh.OrderDate,
       sod.OrderQty,
       sod.LineTotal
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.CustomerID >= 1;',
                             @type = N'SQL',
                             @module_or_batch = NULL,
                             @params = NULL,
                             @hints = N'OPTION  (TABLE HINT(soh,  FORCESEEK))';



--Listing 8-23
EXECUTE sp_control_plan_guide @operation = 'Drop', @name = N'MySQLGuide';
EXECUTE sp_control_plan_guide @operation = 'Drop', @name = N'MyGuide';



DBCC FREEPROCCACHE;

--Listing 8-24
DECLARE @plan_handle VARBINARY(64),
        @start_offset INT;

SELECT @plan_handle = deqs.plan_handle,
       @start_offset = deqs.statement_start_offset
FROM sys.dm_exec_query_stats AS deqs
    CROSS APPLY sys.dm_exec_sql_text(sql_handle)
    CROSS APPLY sys.dm_exec_text_query_plan(deqs.plan_handle, deqs.statement_start_offset, deqs.statement_end_offset) AS qp
WHERE text LIKE N'SELECT soh.SalesOrderNumber%';

EXECUTE sp_create_plan_guide_from_handle @name = N'ForcedPlanGuide',
                                         @plan_handle = @plan_handle,
                                         @statement_start_offset = @start_offset;


--Lising 9-1
SELECT TOP 10
       p.ProductID,
       p.[Name],
       p.StandardCost,
       p.[Weight],
       ROW_NUMBER() OVER (ORDER BY p.NAME DESC) AS RowNumber
FROM Production.Product p
ORDER BY p.NAME DESC;



SELECT TOP 10
       p.ProductID,
       p.[Name],
       p.StandardCost,
       p.[Weight],
       ROW_NUMBER() OVER (ORDER BY p.NAME DESC) AS RowNumber
FROM Production.Product p
ORDER BY p.StandardCost DESC;


--Listing 9-2
IF
(
    SELECT OBJECT_ID('IndexTest')
) IS NOT NULL
    DROP TABLE dbo.IndexTest;
GO
CREATE TABLE dbo.IndexTest
(
    C1 INT,
    C2 INT,
    C3 VARCHAR(50)
);

WITH Nums
AS (SELECT TOP (10000)
           ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n
    FROM MASTER.sys.all_columns ac1
        CROSS JOIN MASTER.sys.all_columns ac2)
INSERT INTO dbo.IndexTest
(
    C1,
    C2,
    C3
)
SELECT n,
       n,
       'C3'
FROM Nums;


--Listing 9-3
UPDATE dbo.IndexTest
SET C1 = 1,
    C2 = 1
WHERE C2 = 1;


--Listing 9-4
CREATE CLUSTERED INDEX iTest 
ON dbo.IndexTest(C1);


--Listing 9-5
CREATE INDEX iTest2 
ON dbo.IndexTest(C2);


--Listing 9-6
SELECT p.ProductID,
       p.NAME,
       p.StandardCost,
       p.Weight
FROM Production.Product p;


--Listing 9-7
SELECT p.ProductID,
       p.NAME,
       p.StandardCost,
       p.Weight
FROM Production.Product AS p
WHERE p.ProductID = 738;


--Listing 9-8
IF
(
    SELECT OBJECT_ID('Test1')
) IS NOT NULL
    DROP TABLE dbo.Test1;
GO
CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 INT
);

WITH Nums
AS (SELECT 1 AS n
    UNION ALL
    SELECT n + 1
    FROM Nums
    WHERE n < 20)
INSERT INTO dbo.Test1
(
    C1,
    C2
)
SELECT n,
       2
FROM Nums;

CREATE INDEX iTest ON dbo.Test1 (C1);


--Listing 9-9
SELECT i.NAME,
       i.type_desc,
       ddips.page_count,
       ddips.record_count,
       ddips.index_level
FROM sys.indexes i
    JOIN sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks'), OBJECT_ID(N'dbo.Test1'), NULL, NULL, 'DETAILED') AS ddips
        ON i.index_id = ddips.index_id
WHERE i.OBJECT_ID = OBJECT_ID(N'dbo.Test1');

--Listing 9-10
DROP INDEX dbo.Test1.iTest;
ALTER TABLE dbo.Test1 ALTER COLUMN C1 CHAR(500);
CREATE INDEX iTest ON dbo.Test1 (C1);



--Listing 9-11
SELECT COUNT(DISTINCT E.MaritalStatus) AS DistinctColValues,
       COUNT(E.MaritalStatus) AS NumberOfRows,
       (CAST(COUNT(DISTINCT E.MaritalStatus) AS DECIMAL) / CAST(COUNT(E.MaritalStatus) AS DECIMAL)) AS Selectivity,
       (1.0 / (COUNT(DISTINCT E.MaritalStatus))) AS Density
FROM HumanResources.Employee AS E;


--Listing 9-12
SELECT e.BusinessEntityID,
       e.MaritalStatus,
       e.BirthDate
FROM HumanResources.Employee AS e
WHERE e.MaritalStatus = 'M'
      AND e.BirthDate = '1982-02-11';
GO 50

--Listing 9-13
CREATE INDEX IX_Employee_Test ON HumanResources.Employee (MaritalStatus);


--Listing 9-14
SELECT e.BusinessEntityID,
       e.MaritalStatus,
       e.BirthDate
FROM HumanResources.Employee AS e WITH (INDEX(IX_Employee_Test))
WHERE e.MaritalStatus = 'M'
      AND e.BirthDate = '1982-02-11';
GO 50

--Listing 9-15
CREATE INDEX IX_Employee_Test
ON HumanResources.Employee (
                               BirthDate,
                               MaritalStatus
                           )
WITH DROP_EXISTING;


--Listing 9-16
DROP INDEX IF EXISTS IX_Employee_Test ON HumanResources.Employee;


--Listing 9-17
CREATE INDEX IX_Address_Test ON Person.ADDRESS (City, PostalCode);


--Listing 9-18
SELECT A.AddressID,
       A.City,
       A.PostalCode
FROM Person.ADDRESS AS A
WHERE A.City = 'Dresden';
GO 50


--Listing 9-19
SELECT A.AddressID,
       A.City,
       A.PostalCode
FROM Person.ADDRESS AS A
WHERE A.PostalCode = '01071';
GO 50

--Listing 9-20
DROP INDEX IF EXISTS IX_Address_Test ON Person.ADDRESS;



--Listing 9-21
SELECT dl.DatabaseLogID,
       dl.PostTime
FROM dbo.DatabaseLog AS dl
WHERE dl.DatabaseLogID = 115;


--Listing 9-22
SELECT d.DepartmentID,
       d.ModifiedDate
FROM HumanResources.Department AS d
WHERE d.DepartmentID = 10;




--Listing 9-23
IF
(
    SELECT OBJECT_ID('Test1')
) IS NOT NULL
    DROP TABLE dbo.Test1;
GO
CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 INT
);

WITH Nums
AS (SELECT TOP (20)
           ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n
    FROM MASTER.sys.all_columns ac1
        CROSS JOIN MASTER.sys.all_columns ac2)
INSERT INTO dbo.Test1
(
    C1,
    C2
)
SELECT n,
       n + 1
FROM Nums;

CREATE CLUSTERED INDEX iClustered ON dbo.Test1 (C2);

CREATE NONCLUSTERED INDEX iNonClustered ON dbo.Test1 (C1);


--Listing 9-24
SELECT i.NAME,
       i.type_desc,
       s.page_count,
       s.record_count,
       s.index_level
FROM sys.indexes i
    JOIN sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks'), OBJECT_ID(N'dbo.Test1'), NULL, NULL, 'DETAILED') AS s
        ON i.index_id = s.index_id
WHERE i.OBJECT_ID = OBJECT_ID(N'dbo.Test1');



--Listing 9-25
DROP INDEX dbo.Test1.iClustered;
ALTER TABLE dbo.Test1 ALTER COLUMN C2 CHAR(500);
CREATE CLUSTERED INDEX iClustered ON dbo.Test1 (C2);


--Listing 9-26
IF
(
    SELECT OBJECT_ID('od')
) IS NOT NULL
    DROP TABLE dbo.od;
GO
SELECT pod.PurchaseOrderID,
       pod.PurchaseOrderDetailID,
       pod.DueDate,
       pod.OrderQty,
       pod.ProductID,
       pod.UnitPrice,
       pod.LineTotal,
       pod.ReceivedQty,
       pod.RejectedQty,
       pod.StockedQty,
       pod.ModifiedDate
INTO dbo.od
FROM Purchasing.PurchaseOrderDetail AS pod;


--Listing 9-27
SELECT od.PurchaseOrderID,
       od.PurchaseOrderDetailID,
       od.DueDate,
       od.OrderQty,
       od.ProductID,
       od.UnitPrice,
       od.LineTotal,
       od.ReceivedQty,
       od.RejectedQty,
       od.StockedQty,
       od.ModifiedDate
FROM dbo.od
WHERE od.ProductID
BETWEEN 500 AND 510
ORDER BY od.ProductID;
GO 50



--Listing 9-28
CREATE CLUSTERED INDEX i1 ON od (ProductID);


--Listing 9-29
BEGIN TRAN;
SET STATISTICS IO ON;
UPDATE Sales.SpecialOfferProduct
SET ProductID = 345
WHERE SpecialOfferID = 1
      AND ProductID = 720;
SET STATISTICS IO OFF;
ROLLBACK TRAN;


--Listing 9-30
CREATE NONCLUSTERED INDEX ixTest
ON Sales.SpecialOfferProduct (ModifiedDate);


--Listing 9-31
DROP INDEX ixTest ON Sales.SpecialOfferProduct;


--Listing 9-32
SET STATISTICS IO ON;
SELECT bp.NAME AS ProductName,
       COUNT(bth.ProductID),
       SUM(bth.Quantity),
       AVG(bth.ActualCost)
FROM dbo.bigProduct AS bp
    JOIN dbo.bigTransactionHistory AS bth
        ON bth.ProductID = bp.ProductID
GROUP BY bp.NAME;
GO 50
SET STATISTICS IO OFF


--Listing 9-33
CREATE NONCLUSTERED COLUMNSTORE INDEX ix_csTest
ON dbo.bigTransactionHistory (
                                 ProductID,
                                 Quantity,
                                 ActualCost
                             );



--Listing 10-1
SELECT A.PostalCode
FROM Person.ADDRESS AS A
WHERE A.StateProvinceID = 42;
GO 50


--Listing 10-2
CREATE NONCLUSTERED INDEX IX_Address_StateProvinceID
ON Person.ADDRESS (StateProvinceID ASC)
INCLUDE (PostalCode)
WITH (DROP_EXISTING = ON);


--Listing 10-3
CREATE NONCLUSTERED INDEX IX_Address_StateProvinceID
ON Person.ADDRESS (StateProvinceID ASC)
WITH (DROP_EXISTING = ON);


--Listing 10-4
SELECT soh.SalesPersonID,
       soh.OrderDate
FROM Sales.SalesOrderHeader AS soh
WHERE soh.SalesPersonID = 276
      AND soh.OrderDate
      BETWEEN '4/1/2013' AND '7/1/2013';
GO 50


--Listing 10-5
CREATE NONCLUSTERED INDEX IX_Test
ON Sales.SalesOrderHeader (OrderDate ASC);



--Listing 10-6
CREATE NONCLUSTERED INDEX IX_Test
ON Sales.SalesOrderHeader (
                              SalesPersonID,
                              OrderDate ASC
                          )
WITH DROP_EXISTING;


--Listing 10-7
DROP INDEX IX_Test ON Sales.SalesOrderHeader;




--Listing 10-8
SELECT poh.PurchaseOrderID,
       poh.RevisionNumber
FROM Purchasing.PurchaseOrderHeader AS poh
WHERE poh.EmployeeID = 261
      AND poh.VendorID = 1500;



--Listing 10-9
SELECT soh.PurchaseOrderNumber,
       soh.OrderDate,
       soh.ShipDate,
       soh.SalesPersonID
FROM Sales.SalesOrderHeader AS soh
WHERE PurchaseOrderNumber LIKE 'PO5%'
      AND soh.SalesPersonID IS NOT NULL;
GO 50



--Listing 10-10
CREATE NONCLUSTERED INDEX IX_Test
ON Sales.SalesOrderHeader (
                              PurchaseOrderNumber,
                              SalesPersonID
                          )
INCLUDE (
            OrderDate,
            ShipDate
        )
WITH drop_existing



--Listing 10-11
CREATE NONCLUSTERED INDEX IX_Test
ON Sales.SalesOrderHeader (
                              PurchaseOrderNumber,
                              SalesPersonID
                          )
INCLUDE (
            OrderDate,
            ShipDate
        )
WHERE PurchaseOrderNumber IS NOT NULL
      AND SalesPersonID IS NOT NULL
WITH (DROP_EXISTING = ON);


--Listing 10-12
DROP INDEX IX_Test ON Sales.SalesOrderHeader;


--Listing 10-13
--SET STATISTICS IO ON;
SELECT p.[Name] AS ProductName,
       SUM(pod.OrderQty) AS OrderOty,
       SUM(pod.ReceivedQty) AS ReceivedOty,
       SUM(pod.RejectedQty) AS RejectedOty
FROM Purchasing.PurchaseOrderDetail AS pod
    JOIN Production.Product AS p
        ON p.ProductID = pod.ProductID
GROUP BY p.[Name];
GO 50

SELECT p.[Name] AS ProductName,
       SUM(pod.OrderQty) AS OrderOty,
       SUM(pod.ReceivedQty) AS ReceivedOty,
       SUM(pod.RejectedQty) AS RejectedOty
FROM Purchasing.PurchaseOrderDetail AS pod
    JOIN Production.Product AS p
        ON p.ProductID = pod.ProductID
GROUP BY p.[Name]
HAVING (SUM(pod.RejectedQty) / SUM(pod.ReceivedQty)) > .08;
GO 50

SELECT p.[Name] AS ProductName,
       SUM(pod.OrderQty) AS OrderQty,
       SUM(pod.ReceivedQty) AS ReceivedQty,
       SUM(pod.RejectedQty) AS RejectedQty
FROM Purchasing.PurchaseOrderDetail AS pod
    JOIN Production.Product AS p
        ON p.ProductID = pod.ProductID
WHERE p.[Name] LIKE 'Chain%'
GROUP BY p.[Name];
GO 50
--SET STATISTICS IO OFF;



--Listing 10-14
CREATE OR ALTER VIEW Purchasing.IndexedView
WITH SCHEMABINDING
AS
SELECT pod.ProductID,
       SUM(pod.OrderQty) AS OrderQty,
       SUM(pod.ReceivedQty) AS ReceivedQty,
       SUM(pod.RejectedQty) AS RejectedQty,
       COUNT_BIG(*) AS COUNT
FROM Purchasing.PurchaseOrderDetail AS pod
GROUP BY pod.ProductID;
GO
CREATE UNIQUE CLUSTERED INDEX iv ON Purchasing.IndexedView (ProductID);


--Listing 10-15
DROP VIEW Purchasing.IndexedView;


--Listing 10-16
CREATE NONCLUSTERED INDEX IX_Test
ON Person.ADDRESS (
                      City ASC,
                      PostalCode ASC
                  );



--Listing 10-17
CREATE NONCLUSTERED INDEX IX_CompRow_Test
ON Person.ADDRESS (
                      City,
                      PostalCode
                  )
WITH (DATA_COMPRESSION = ROW);

CREATE NONCLUSTERED INDEX IX_CompPage_Test
ON Person.ADDRESS (
                      City,
                      PostalCode
                  )
WITH (DATA_COMPRESSION = PAGE);


--Listing 10-18
SELECT i.NAME,
       i.type_desc,
       s.page_count,
       s.record_count,
       s.index_level,
       s.compressed_page_count
FROM sys.indexes AS i
    JOIN sys.dm_db_index_physical_stats(DB_ID(N'AdventureWorks'), OBJECT_ID(N'Person.Address'), NULL, NULL, 'DETAILED') AS s
        ON i.index_id = s.index_id
WHERE i.OBJECT_ID = OBJECT_ID(N'Person.Address');


--Listing 10-19
DROP INDEX IX_Test ON Person.ADDRESS;
DROP INDEX IX_CompRow_Test ON Person.ADDRESS;
DROP INDEX IX_CompPage_Test ON Person.ADDRESS;


--Listing 10-20
CREATE NONCLUSTERED INDEX IX_Test
ON Person.ADDRESS (
                      City ASC,
                      PostalCode DESC
                  );


---Listing 10-21
BEGIN TRAN
CREATE NONCLUSTERED INDEX IX_Test 
ON Person.Address(City);
ROLLBACK TRAN



--Listing 11-1
SELECT p.NAME,
       AVG(sod.LineTotal)
FROM Sales.SalesOrderDetail AS sod
    JOIN Production.Product AS p
        ON sod.ProductID = p.ProductID
WHERE sod.ProductID = 776
GROUP BY sod.CarrierTrackingNumber,
         p.NAME
HAVING MAX(sod.OrderQty) > 1
ORDER BY MIN(sod.LineTotal);



--Listing 11-2
SELECT *
FROM Sales.SalesOrderDetail AS sod
WHERE sod.ProductID = 793;
GO 50


--Listing 11-3
SELECT *
FROM Sales.SalesOrderDetail AS sod WITH (INDEX(IX_SalesOrderDetail_ProductID))
WHERE sod.ProductID = 793;
GO 50


--Listing 11-4
SELECT NationalIDNumber,
       JobTitle,
       HireDate
FROM HumanResources.Employee AS E
WHERE E.NationalIDNumber = '693168613';


--Listing 11-5
DBCC SHOW_STATISTICS('HumanResources.Employee', 'AK_Employee_NationalIDNumber') WITH DENSITY_VECTOR;


--Listing 11-6
CREATE UNIQUE NONCLUSTERED INDEX AK_Employee_NationalIDNumber
ON [HumanResources].[Employee] (
                                   NationalIDNumber ASC,
                                   JobTitle,
                                   HireDate
                               )
WITH DROP_EXISTING;


--Listing 11-7
CREATE UNIQUE NONCLUSTERED INDEX AK_Employee_NationalIDNumber
ON [HumanResources].[Employee] (NationalIDNumber ASC)
INCLUDE (
            JobTitle,
            HireDate
        )
WITH DROP_EXISTING;


--Listing 11-8
CREATE UNIQUE NONCLUSTERED INDEX AK_Employee_NationalIDNumber
ON [HumanResources].[Employee] (NationalIDNumber ASC)
WITH DROP_EXISTING;


--Listing 11-9
SELECT NationalIDNumber,
       E.BusinessEntityID
FROM HumanResources.Employee AS E
WHERE E.NationalIDNumber = '693168613';



--Listing 11-10
SELECT poh.PurchaseOrderID,
       poh.VendorID,
       poh.OrderDate
FROM Purchasing.PurchaseOrderHeader AS poh
WHERE VendorID = 1636
      AND poh.OrderDate = '2014/6/24';
GO 50


--Listing 11-11
CREATE NONCLUSTERED INDEX IX_TEST
ON Purchasing.PurchaseOrderHeader (OrderDate);


--Listing 12-1
DROP TABLE IF EXISTS dbo.SplitTest;
GO

CREATE TABLE dbo.SplitTest
(
    C1 INT,
    C2 CHAR(999),
    C3 VARCHAR(10)
);
INSERT INTO dbo.SplitTest
(
    C1,
    C2,
    C3
)
VALUES
(100, 'C2', ''),
(200, 'C2', ''),
(300, 'C2', ''),
(400, 'C2', ''),
(500, 'C2', ''),
(600, 'C2', ''),
(700, 'C2', ''),
(800, 'C2', '');

CREATE CLUSTERED INDEX iClustered ON dbo.SplitTest (C1);



--Listing 12-2
SELECT ddips.avg_fragmentation_in_percent,
       ddips.fragment_count,
       ddips.page_count,
       ddips.avg_page_space_used_in_percent,
       ddips.record_count,
       ddips.avg_record_size_in_bytes
FROM sys.dm_db_index_physical_stats(DB_ID('AdventureWorks'), OBJECT_ID(N'dbo.SplitTest'), NULL, NULL, 'Sampled') AS ddips;


--Listing 12-3
UPDATE dbo.SplitTest
SET C3 = 'Add data'
WHERE C1 = 200;


--Listing 12-4
INSERT INTO dbo.SplitTest
VALUES
(110, 'C2', '');


--Listing 12-5
ALTER TABLE dbo.bigTransactionHistory
DROP CONSTRAINT pk_bigTransactionHistory;

CREATE CLUSTERED COLUMNSTORE INDEX cci_bigTransactionHistory
ON dbo.bigTransactionHistory;


--Listing 12-6
SELECT OBJECT_NAME(i.OBJECT_ID) AS TableName,
       i.NAME AS IndexName,
       csrg.row_group_id,
       csrg.state_description,
       csrg.total_rows,
       csrg.deleted_rows,
       100 * (total_rows - ISNULL(deleted_rows, 0)) / total_rows AS PercentFull
FROM sys.indexes AS i
    JOIN sys.column_store_row_groups AS csrg
        ON i.OBJECT_ID = csrg.OBJECT_ID
           AND i.index_id = csrg.index_id
WHERE NAME = 'cci_bigTransactionHistory'
ORDER BY OBJECT_NAME(i.OBJECT_ID),
         i.NAME,
         row_group_id;



--Listing 12-7
DELETE dbo.bigTransactionHistory
WHERE Quantity = 13;


--Listing 12-8
DROP TABLE IF EXISTS dbo.FragTest;
GO
CREATE TABLE dbo.FragTest
(
    C1 INT,
    C2 INT,
    C3 INT,
    c4 CHAR(2000)
);

CREATE CLUSTERED INDEX iClustered ON dbo.FragTest (C1);

WITH Nums
AS (SELECT TOP (10000)
           ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n
    FROM MASTER.sys.all_columns AS ac1
        CROSS JOIN MASTER.sys.all_columns AS ac2)
INSERT INTO dbo.FragTest
(
    C1,
    C2,
    C3,
    c4
)
SELECT n,
       n,
       n,
       'a'
FROM Nums;

WITH Nums
AS (SELECT 1 AS n
    UNION ALL
    SELECT n + 1
    FROM Nums
    WHERE n < 10000)
INSERT INTO dbo.FragTest
(
    C1,
    C2,
    C3,
    c4
)
SELECT 10000 - n,
       n,
       n,
       'a'
FROM Nums
OPTION (MAXRECURSION 10000);


--Listing 12-9
--Reads 6 rows
SELECT ft.C1,
       ft.C2,
       ft.C3,
       ft.c4
FROM dbo.FragTest AS ft
WHERE C1
BETWEEN 21 AND 23;
GO 50


--Reads all rows
SELECT ft.C1,
       ft.C2,
       ft.C3,
       ft.c4
FROM dbo.FragTest AS ft
WHERE C1
BETWEEN 1 AND 10000;
GO 50


--Listing 12-10
ALTER INDEX iClustered ON dbo.FragTest REBUILD;


--Listing 12-11
SELECT bth.Quantity,
       AVG(bth.ActualCost)
FROM dbo.bigTransactionHistory AS bth
WHERE bth.Quantity
BETWEEN 8 AND 15
GROUP BY bth.Quantity;
GO 50


--Listing 12-12
DELETE dbo.bigTransactionHistory
WHERE Quantity
BETWEEN 9 AND 12;


--Listing 12-13
--Intentionally using SELECT *
SELECT ddips.*
FROM sys.dm_db_index_physical_stats(DB_ID('AdventureWorks'), OBJECT_ID(N'dbo.FragTest'), NULL, NULL, 'Detailed') AS ddips;


--Listing 12-14
CREATE UNIQUE CLUSTERED INDEX PK_EmailAddress_BusinessEntityID_EmailAddressID
ON Person.EmailAddress (
                           BusinessEntityID,
                           EmailAddressID
                       )
WITH (DROP_EXISTING = ON);


--Listing 12-15
ALTER INDEX ALL ON dbo.FragTest REBUILD;




SELECT ddips.avg_fragmentation_in_percent,
       ddips.fragment_count,
       ddips.page_count,
       ddips.avg_page_space_used_in_percent,
       ddips.record_count,
       ddips.avg_record_size_in_bytes
FROM sys.dm_db_index_physical_stats(DB_ID('AdventureWorks'), OBJECT_ID(N'dbo.FragTest'), NULL, NULL, 'Sampled') AS ddips;


--Listing 12-16:
ALTER INDEX iClustered ON dbo.FragTest REORGANIZE;


--Listing 12-17:
DELETE dbo.bigTransactionHistory
WHERE Quantity
BETWEEN 8 AND 17;


SELECT OBJECT_NAME(i.OBJECT_ID) AS TableName,
       i.NAME AS IndexName,
       csrg.row_group_id,
       csrg.state_description,
       csrg.total_rows,
       csrg.deleted_rows,
       100 * (total_rows - ISNULL(deleted_rows, 0)) / total_rows AS PercentFull
FROM sys.indexes AS i
    JOIN sys.column_store_row_groups AS csrg
        ON i.OBJECT_ID = csrg.OBJECT_ID
           AND i.index_id = csrg.index_id
WHERE NAME = 'cci_bigTransactionHistory'
ORDER BY OBJECT_NAME(i.OBJECT_ID),
         i.NAME,
         row_group_id;

--Listing 12-18
ALTER INDEX cci_bigTransactionHistory ON dbo.bigTransactionHistory REORGANIZE;


--Listing 12-19
ALTER INDEX cci_bigTransactionHistory
ON dbo.bigTransactionHistory
REORGANIZE
WITH (COMPRESS_ALL_ROW_GROUPS = ON);


--Listing 12-20
ALTER INDEX i1 ON dbo.Test1 REBUILD PARTITION = ALL WITH (ONLINE = ON);

--Listing 12-21
ALTER INDEX i1 ON dbo.Test1 REBUILD PARTITION = 1 WITH (ONLINE = ON);

--Listing 12-22
ALTER INDEX i1
ON dbo.Test1
REBUILD PARTITION = 1
WITH (   ONLINE = ON
         (
             WAIT_AT_LOW_PRIORITY
             (
                 MAX_DURATION = 20,
                 ABORT_AFTER_WAIT = SELF
             )
         )
     );


--Listing 12-23
DROP TABLE IF EXISTS dbo.Test1;
GO
CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 CHAR(999)
);

WITH Nums
AS (SELECT 1 AS n
    UNION ALL
    SELECT n + 1
    FROM Nums
    WHERE n < 24)
INSERT INTO dbo.Test1
(
    C1,
    C2
)
SELECT n * 100,
       'a'
FROM Nums;


--Listing 12-24
CREATE CLUSTERED INDEX FillIndex ON Test1(C1);


--Listing 12-25
ALTER INDEX FillIndex ON dbo.Test1 REBUILD 
WITH  (FILLFACTOR= 75);


--Listing 12-26
INSERT  INTO dbo.Test1
VALUES  (110, 'a'),  --25th row     
        (120, 'a') ;  --26th row


--Listing 12-27
INSERT  INTO dbo.Test1
VALUES  (130, 'a') ;  --27th row



GO
--Listing 13-1
CREATE OR ALTER PROC dbo.ProductTransactionHistoryByReference
(@ReferenceOrderID INT)
AS
BEGIN
    SELECT p.NAME,
           p.ProductNumber,
           th.ReferenceOrderID
    FROM Production.Product AS p
        JOIN Production.TransactionHistory AS th
            ON th.ProductID = p.ProductID
    WHERE th.ReferenceOrderID = @ReferenceOrderID;
END;




--Listing 13-2
EXEC dbo.ProductTransactionHistoryByReference @ReferenceOrderID = 53465;


--Listing 13-3
DECLARE @planhandle VARBINARY(64);

SELECT @planhandle = deps.plan_handle FROM sys.dm_exec_procedure_stats AS deps
WHERE deps.object_id = OBJECT_ID('dbo.ProductTransactionHistoryByReference')

IF @planhandle IS NOT NULL
DBCC FREEPROCCACHE(@planhandle);


--Listing 13-4
DECLARE @ReferenceOrderID INT = 53465;
SELECT p.Name,
       p.ProductNumber,
       th.ReferenceOrderID
FROM Production.Product AS p
    JOIN Production.TransactionHistory AS th
        ON th.ProductID = p.ProductID
WHERE th.ReferenceOrderID = @ReferenceOrderID;


--Listing 13-5
SELECT deps.EXECUTION_COUNT,
       deps.total_elapsed_time,
       deps.total_logical_reads,
       deps.total_logical_writes,
       deqp.query_plan
FROM sys.dm_exec_procedure_stats AS deps
    CROSS APPLY sys.dm_exec_query_plan(deps.plan_handle) AS deqp
WHERE deps.OBJECT_ID = OBJECT_ID('dbo.ProductTransactionHistoryByReference');



--Listing 13-6
SELECT SUM(qsrs.count_executions) AS ExecutionCount,
       AVG(qsrs.avg_duration) AS AvgDuration,
       AVG(qsrs.avg_logical_io_reads) AS AvgReads,
       AVG(qsrs.avg_logical_io_writes) AS AvgWrites,
       CAST(qsp.query_plan AS XML) AS QueryPlan,
       qsp.query_id,
       qsp.plan_id
FROM sys.query_store_query AS qsq
    JOIN sys.query_store_plan AS qsp
        ON qsp.query_id = qsq.query_id
    JOIN sys.query_store_runtime_stats AS qsrs
        ON qsrs.plan_id = qsp.plan_id
WHERE qsq.OBJECT_ID = OBJECT_ID('dbo.ProductTransactionHistoryByReference')
GROUP BY qsp.query_plan,
         qsp.query_id,
         qsp.plan_id;


--Listing 13-7
CREATE EVENT SESSION [ExecutionPlans]
ON SERVER
    ADD EVENT sqlserver.query_post_execution_showplan
    (WHERE (
               [sqlserver].[equal_i_sql_unicode_string]([sqlserver].[database_name], N'AdventureWorks')
               AND [object_name] = N'ProductTransactionHistoryByReference'
           )
    ),
    ADD EVENT sqlserver.rpc_completed
    (WHERE (
               [sqlserver].[equal_i_sql_unicode_string]([sqlserver].[database_name], N'AdventureWorks')
               AND [object_name] = N'ProductTransactionHistoryByReference'
           )
    )
    ADD TARGET package0.event_file
    (SET FILENAME = N'ExecutionPlans')
WITH
(
    TRACK_CAUSALITY = ON
);


--Listing 13-8
DECLARE @KeyValue INT = 816;
WITH histolow
AS (SELECT ddsh.step_number,
           ddsh.range_high_key,
           ddsh.range_rows,
           ddsh.equal_rows,
           ddsh.average_range_rows
    FROM sys.dm_db_stats_histogram(OBJECT_ID('Production.TransactionHistory'), 3) AS ddsh ),
     histojoin
AS (SELECT h1.step_number,
           h1.range_high_key,
           h2.range_high_key AS range_high_key_step1,
           h1.range_rows,
           h1.equal_rows,
           h1.average_range_rows
    FROM histolow AS h1
        LEFT JOIN histolow AS h2
            ON h1.step_number = h2.step_number + 1)
SELECT hj.range_high_key,
       hj.equal_rows,
       hj.average_range_rows
FROM histojoin AS hj
WHERE hj.range_high_key >= @KeyValue
      AND
      (
          hj.range_high_key_step1 < @KeyValue
          OR hj.range_high_key_step1 IS NULL
      );



--Listing 13-9
DBCC TRACEON (4136,-1);
DBCC TRACEOFF (4136,-1);


--Listing 13-10
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = OFF;

GO
--Listing 13-11
CREATE OR ALTER PROC dbo.ProductTransactionHistoryByReference
(@ReferenceOrderID INT)
AS
BEGIN
    SELECT p.NAME,
           p.ProductNumber,
           th.ReferenceOrderID
    FROM Production.Product AS p
        JOIN Production.TransactionHistory AS th
            ON th.ProductID = p.ProductID
    WHERE th.ReferenceOrderID = @ReferenceOrderID
    OPTION (USE HINT ('DISABLE_PARAMETER_SNIFFING'));
END;





go
--Listing 13-12
CREATE OR ALTER PROC dbo.AddressByCity
(@City VARCHAR(30))
AS
BEGIN
    DECLARE @LocalCity VARCHAR(30) = @City;

    SELECT A.AddressID,
           A.PostalCode,
           sp.NAME,
           A.City
    FROM Person.ADDRESS AS A
        JOIN Person.StateProvince AS sp
            ON sp.StateProvinceID = A.StateProvinceID
    WHERE A.City = @LocalCity;
END;
GO


--Listing 13-13
CREATE OR ALTER PROC dbo.ProductTransactionHistoryByReference
(@ReferenceOrderID INT)
AS
BEGIN
    SELECT p.NAME,
           p.ProductNumber,
           th.ReferenceOrderID
    FROM Production.Product AS p
        JOIN Production.TransactionHistory AS th
            ON th.ProductID = p.ProductID
    WHERE th.ReferenceOrderID = @ReferenceOrderID
    OPTION (RECOMPILE);
END;

go


--Listing 13-14
CREATE OR ALTER PROC dbo.ProductTransactionHistoryByReference
(@ReferenceOrderID INT)
AS
BEGIN
    SELECT p.NAME,
           p.ProductNumber,
           th.ReferenceOrderID
    FROM Production.Product AS p
        JOIN Production.TransactionHistory AS th
            ON th.ProductID = p.ProductID
    WHERE th.ReferenceOrderID = @ReferenceOrderID
    OPTION (OPTIMIZE FOR (@ReferenceOrderID = 53465));
END;


--Listing 13-15
EXEC dbo.ProductTransactionHistoryByReference @ReferenceOrderID = 816;


GO 
--Listing 13-16
CREATE OR ALTER PROC dbo.ProductTransactionHistoryByReference
(@ReferenceOrderID INT)
AS
BEGIN
    SELECT p.NAME,
           p.ProductNumber,
           th.ReferenceOrderID
    FROM Production.Product AS p
        JOIN Production.TransactionHistory AS th
            ON th.ProductID = p.ProductID
    WHERE th.ReferenceOrderID = @ReferenceOrderID
    OPTION (OPTIMIZE FOR (@ReferenceOrderID UNKNOWN));
END;


--Listing 13-17
CREATE OR ALTER PROC dbo.ProductTransactionHistoryByReference
(@ReferenceOrderID INT)
AS
BEGIN
    SELECT p.NAME,
           p.ProductNumber,
           th.ReferenceOrderID
    FROM Production.Product AS p
        JOIN Production.TransactionHistory AS th
            ON th.ProductID = p.ProductID
    WHERE th.ReferenceOrderID = @ReferenceOrderID
    OPTION (OPTIMIZE FOR (@ReferenceOrderID UNKNOWN));
END;


--Listing 13-18
--Modify data to get 100K rows
UPDATE dbo.bigTransactionHistory
SET ProductID = 1319
WHERE ProductID IN ( 28417, 28729, 11953, 35521, 11993, 29719, 20431, 29531, 29749, 7913, 29947, 10739, 26921, 20941,
                     27767, 27941, 47431, 31847, 32411, 39383, 39511, 35531, 28829, 35759, 29713, 29819, 16001, 29951,
                     10453, 34967, 16363, 41347, 39719, 39443, 39829, 38917, 41759, 16453, 16963, 17453, 16417, 17473,
                     17713, 10729, 21319, 21433, 21473, 29927, 21859, 16477
                   );
GO

--Add a single row to both tables
INSERT INTO dbo.bigProduct
(
    ProductID,
    Name,
    ProductNumber,
    SafetyStockLevel,
    ReorderPoint,
    DaysToManufacture,
    SellStartDate
)
VALUES
(43, 'FarbleDing', 'CA-2222-1000', 0, 0, 0, GETDATE());
INSERT INTO dbo.bigTransactionHistory
(
    TransactionID,
    ProductID,
    TransactionDate,
    Quantity,
    ActualCost
)
VALUES
(31263602, 42, GETDATE(), 42, 42);
GO

--Create an index for testing
CREATE INDEX ProductIDTransactionDate
ON dbo.bigTransactionHistory (
                                 ProductID,
                                 TransactionDate
                             );
GO

--Create a procedure
CREATE PROC dbo.TransactionInfo
(@ProductID INT)
AS
BEGIN
    SELECT bp.Name,
           bp.ProductNumber,
           bth.TransactionDate
    FROM dbo.bigTransactionHistory AS bth
        JOIN dbo.bigProduct AS bp
            ON bp.ProductID = bth.ProductID
    WHERE bth.ProductID = @ProductID;
END;

--Execute the Queries
EXEC dbo.TransactionInfo @ProductID = 1319;
EXEC dbo.TransactionInfo @ProductID = 42;


--Listing 13-19
SELECT deqs.query_hash,
       deqs.query_plan_hash,
       dest.text,
       deqp.query_plan
FROM sys.dm_exec_query_stats AS deqs
    CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp
    CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
WHERE dest.text LIKE '%SELECT bp.Name,
           bp.ProductNumber,
           bth.TransactionDate
    FROM dbo.bigTransactionHistory AS bth%';


--Listing 13-19
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SENSITIVE_PLAN_OPTIMIZATION = OFF;


--Listing 14-1
SELECT NAME,
       TerritoryID
FROM Sales.SalesTerritory AS st
WHERE st.NAME = 'Australia';
GO 50

SELECT *
FROM Sales.SalesTerritory AS st
WHERE st.NAME = 'Australia';
GO 50


--Listing 14-2
SELECT sod.CarrierTrackingNumber,
       sod.OrderQty
FROM Sales.SalesOrderDetail AS sod
WHERE sod.SalesOrderID IN ( 51825, 51826, 51827, 51828 );
GO 50

--Listing 14-3
SET STATISTICS IO ON
SELECT sod.CarrierTrackingNumber,
       sod.OrderQty
FROM Sales.SalesOrderDetail AS sod
WHERE sod.SalesOrderID = 51825
      OR sod.SalesOrderID = 51826
      OR sod.SalesOrderID = 51827
      OR sod.SalesOrderID = 51828;
GO 50

--Listing 14-4
SET STATISTICS IO ON;
SELECT sod.CarrierTrackingNumber,
       sod.OrderQty
FROM Sales.SalesOrderDetail AS sod
WHERE sod.SalesOrderID
BETWEEN 51825 AND 51828;
GO 50

SET STATISTICS IO OFF;


--Listing 14-5
SELECT C.CurrencyCode
FROM Sales.Currency AS C
WHERE C.NAME LIKE 'Ice%';


--Listing 14-6
SELECT poh.TotalDue,
       poh.Freight
FROM Purchasing.PurchaseOrderHeader AS poh
WHERE poh.PurchaseOrderID >= 2975;
SELECT poh.TotalDue,
       poh.Freight
FROM Purchasing.PurchaseOrderHeader AS poh
WHERE poh.PurchaseOrderID !< 2975;


--Listing 14-7
SELECT poh.EmployeeID,
       poh.OrderDate
FROM Purchasing.PurchaseOrderHeader AS poh
WHERE poh.PurchaseOrderID * 2 = 3400;
GO 50

--Listing 14-8
SELECT poh.EmployeeID,
       poh.OrderDate
FROM Purchasing.PurchaseOrderHeader AS poh
WHERE poh.PurchaseOrderID = 3400 / 2;
GO 50


--Listing 14-9
IF EXISTS
(
    SELECT *
    FROM sys.indexes
    WHERE OBJECT_ID = OBJECT_ID(N'[Sales].[SalesOrderHeader]')
          AND NAME = N'IndexTest'
)
    DROP INDEX IndexTest ON Sales.SalesOrderHeader;
GO
CREATE INDEX IndexTest ON Sales.SalesOrderHeader (OrderDate);



--Listing 14-10
SELECT soh.SalesOrderID,
       soh.OrderDate
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE DATEPART(yy, soh.OrderDate) = 2008
      AND DATEPART(mm, soh.OrderDate) = 4;
GO 50


--Listing 14-11
SELECT soh.SalesOrderID,
       soh.OrderDate
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE soh.OrderDate >= '2008-04-01'
      AND soh.OrderDate < '2008-05-01';
GO 50


--Listing 14-12
DROP INDEX Sales.SalesOrderHeader.IndexTest;

go
--Listing 14-13
CREATE OR ALTER FUNCTION dbo.ProductStandardCost
(
    @ProductID INT
)
RETURNS MONEY
AS
BEGIN
    DECLARE @Cost MONEY;
    SELECT TOP 1
           @Cost = pch.StandardCost
    FROM Production.ProductCostHistory AS pch
    WHERE pch.ProductID = @ProductID
    ORDER BY pch.StartDate DESC;

    IF @Cost IS NULL
        SET @Cost = 0;
    RETURN @Cost;
END;

go
--Listing 14-14
SELECT p.NAME,
       dbo.ProductStandardCost(p.ProductID)
FROM Production.Product AS p
WHERE p.ProductNumber LIKE 'HL%';
GO 50


--Listing 14-15
SELECT p.NAME,
       pc.StandardCost
FROM Production.Product AS p
    CROSS APPLY
(
    SELECT TOP 1
           pch.StandardCost
    FROM Production.ProductCostHistory AS pch
    WHERE pch.ProductID = p.ProductID
    ORDER BY pch.StartDate DESC
) AS pc
WHERE p.ProductNumber LIKE 'HL%';
GO 50

--Listing 14-16
SELECT s.NAME AS StoreName,
       p.LastName + ', ' + p.FirstName
FROM Sales.Store AS s
    JOIN Sales.SalesPerson AS sp
        ON s.SalesPersonID = sp.BusinessEntityID
    JOIN HumanResources.Employee AS E
        ON sp.BusinessEntityID = E.BusinessEntityID
    JOIN Person.Person AS p
        ON E.BusinessEntityID = p.BusinessEntityID;
GO 50


--Listing 14-17
SELECT s.NAME AS StoreName,
       p.LastName + ',   ' + p.FirstName
FROM Sales.Store AS s
    JOIN Sales.SalesPerson AS sp
        ON s.SalesPersonID = sp.BusinessEntityID
    JOIN HumanResources.Employee AS E
        ON sp.BusinessEntityID = E.BusinessEntityID
    JOIN Person.Person AS p
        ON E.BusinessEntityID = p.BusinessEntityID
OPTION (LOOP JOIN);
GO 50


--Listing 14-18
SELECT s.NAME AS StoreName,
       p.LastName + ',   ' + p.FirstName
FROM Sales.Store AS s
    INNER LOOP JOIN Sales.SalesPerson AS sp
        ON s.SalesPersonID = sp.BusinessEntityID
    JOIN HumanResources.Employee AS E
        ON sp.BusinessEntityID = E.BusinessEntityID
    JOIN Person.Person AS p
        ON E.BusinessEntityID = p.BusinessEntityID;
GO 50

--Listing 14-19
SELECT poh.EmployeeID,
       poh.OrderDate
FROM Purchasing.PurchaseOrderHeader AS poh WITH (INDEX(PK_PurchaseOrderHeader_PurchaseOrderID))
WHERE poh.PurchaseOrderID * 2 = 3400;
GO 50


--Listing 14-20
SELECT p.FirstName
FROM Person.Person AS p
WHERE p.FirstName < 'B'
      OR p.FirstName >= 'C';

SELECT p.MiddleName
FROM Person.Person AS p
WHERE p.MiddleName < 'B'
      OR p.MiddleName >= 'C';

--Listing 14-21
SELECT p.MiddleName
FROM Person.Person AS p
WHERE p.MiddleName < 'B'
      OR p.MiddleName >= 'C'
      OR p.MiddleName IS NULL;



SELECT p.FirstName
FROM Person.Person AS p
WHERE p.FirstName < 'B'
      OR p.FirstName >= 'C';
go
SELECT p.MiddleName
FROM Person.Person AS p
WHERE p.MiddleName < 'B'
      OR p.MiddleName >= 'C'
      OR p.MiddleName IS NULL;
GO

--Listing 14-22
CREATE INDEX TestIndex1 ON Person.Person (MiddleName);

CREATE INDEX TestIndex2 ON Person.Person (FirstName);


CREATE INDEX TestIndex1
ON Person.Person (MiddleName)
WHERE MiddleName IS NOT NULL
WITH DROP_EXISTING;


--Listing 14-23
DROP INDEX TestIndex1 ON Person.Person;
DROP INDEX TestIndex2 ON Person.Person;


--Listing 14-24
ALTER TABLE Sales.SalesOrderDetail WITH CHECK
ADD CONSTRAINT CK_SalesOrderDetail_UnitPrice CHECK ((
                                             UnitPrice >= (0.00)
                                                   ));




---Listing 14-25
SELECT soh.OrderDate,
       soh.ShipDate,
       sod.OrderQty,
       sod.UnitPrice,
       p.Name AS ProductName
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON sod.SalesOrderID = soh.SalesOrderID
    JOIN Production.Product AS p
        ON p.ProductID = sod.ProductID
WHERE p.Name = 'Water Bottle - 30 oz.'
      AND sod.UnitPrice < $0.0;


--Listing 14-26
IF EXISTS
(
    SELECT *
    FROM sys.foreign_keys
    WHERE OBJECT_ID = OBJECT_ID(N'[Person].[FK_Address_StateProvince_StateProvinceID]')
          AND parent_object_id = OBJECT_ID(N'[Person].[Address]')
)
    ALTER TABLE Person.ADDRESS
    DROP CONSTRAINT FK_Address_StateProvince_StateProvinceID;

--Listing 14-27
SELECT A.AddressID,
       sp.StateProvinceID
FROM Person.ADDRESS AS A
    JOIN Person.StateProvince AS sp
        ON A.StateProvinceID = sp.StateProvinceID
WHERE A.AddressID = 27234;

--NOTE, Address.StateProvinceID
SELECT A.AddressID,
       A.StateProvinceID
FROM Person.ADDRESS AS A
    JOIN Person.StateProvince AS sp
        ON A.StateProvinceID = sp.StateProvinceID
WHERE A.AddressID = 27234;


--Listing 14-28
ALTER TABLE Person.ADDRESS WITH CHECK
ADD CONSTRAINT FK_Address_StateProvince_StateProvinceID
    FOREIGN KEY (StateProvinceID)
    REFERENCES Person.StateProvince (StateProvinceID);

GO
--Listing 15-1
DROP TABLE IF EXISTS dbo.Test1;

CREATE TABLE dbo.Test1
(
    Id INT IDENTITY(1, 1),
    MyKey VARCHAR(50),
    MyValue VARCHAR(50)
);
CREATE UNIQUE CLUSTERED INDEX Test1PrimaryKey ON dbo.Test1 (ID ASC);
CREATE UNIQUE NONCLUSTERED INDEX TestIndex ON dbo.Test1 (MyKey);

WITH Tally
AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS num
    FROM MASTER.dbo.syscolumns AS A
        CROSS JOIN MASTER.dbo.syscolumns AS B)
INSERT INTO dbo.Test1
(
    MyKey,
    MyValue
)
SELECT TOP 10000
       'UniqueKey' + CAST(Tally.num AS VARCHAR),
       'Description'
FROM Tally;

SELECT t.MyValue
FROM dbo.Test1 AS t
WHERE t.MyKey = 'UniqueKey333';
--go 50
SELECT t.MyValue
FROM dbo.Test1 AS t
WHERE t.MyKey = N'UniqueKey333';
--go 50


--Listing 15-2
DECLARE @n INT;
SELECT @n = COUNT(*)
FROM Sales.SalesOrderDetail AS sod
WHERE sod.OrderQty = 1;
IF @n > 0
    PRINT 'Record Exists';
--go 50


--Listing 15-3
IF EXISTS
(
    SELECT sod.OrderQty
    FROM Sales.SalesOrderDetail AS sod
    WHERE sod.OrderQty = 1
)
    PRINT 'Record Exists';
--go 50


--Listing 15-4
SELECT sod.ProductID,
       sod.SalesOrderID
FROM Sales.SalesOrderDetail AS sod
WHERE sod.ProductID = 934
UNION
SELECT sod.ProductID,
       sod.SalesOrderID
FROM Sales.SalesOrderDetail AS sod
WHERE sod.ProductID = 932
UNION
SELECT sod.ProductID,
       sod.SalesOrderID
FROM Sales.SalesOrderDetail AS sod
WHERE sod.ProductID = 708;
go 50

SELECT sod.ProductID,
       sod.SalesOrderID
FROM Sales.SalesOrderDetail AS sod
WHERE sod.ProductID = 934
UNION ALL
SELECT sod.ProductID,
       sod.SalesOrderID
FROM Sales.SalesOrderDetail AS sod
WHERE sod.ProductID = 932
UNION ALL
SELECT sod.ProductID,
       sod.SalesOrderID
FROM Sales.SalesOrderDetail AS sod
WHERE sod.ProductID = 708;
go 50

--Listing 15-5
SELECT MIN(sod.UnitPrice)
FROM Sales.SalesOrderDetail AS sod;


--Listing 15-6
CREATE INDEX TestIndex ON Sales.SalesOrderDetail (UnitPrice ASC);


--Listing 15-7
DROP INDEX IF EXISTS TestIndex ON dbo.Test1;


--Listing 15-8
DECLARE @Id INT = 67260;

SELECT p.Name,
       p.ProductNumber,
       th.ReferenceOrderID
FROM Production.Product AS p
    JOIN Production.TransactionHistory AS th
        ON th.ProductID = p.ProductID
WHERE th.ReferenceOrderID = @Id;
--go 50

--Listing 15-9
SELECT p.Name,
       p.ProductNumber,
       th.ReferenceOrderID
FROM Production.Product AS p
    JOIN Production.TransactionHistory AS th
        ON th.ProductID = p.ProductID
WHERE th.ReferenceOrderID = 67260;
--go 50


--Listing 15-10
SET NOCOUNT ON;

--Listing 15-11
DROP TABLE IF EXISTS dbo.Test1;

CREATE TABLE dbo.Test1
(
    C1 TINYINT
);
GO
DBCC SQLPERF(LOGSPACE);
--Insert 10000 rows
DECLARE @Count INT = 1;
WHILE @Count <= 10000
BEGIN
    INSERT INTO dbo.Test1
    (
        C1
    )
    VALUES
    (@Count % 256);
    SET @Count = @Count + 1;
END;
DBCC SQLPERF(LOGSPACE);


--Listing 15-12
DECLARE @Count INT = 1;
DBCC SQLPERF(LOGSPACE);
BEGIN TRANSACTION;
WHILE @Count <= 10000
BEGIN
    INSERT INTO dbo.Test1
    (
        C1
    )
    VALUES
    (@Count % 256);
    SET @Count = @Count + 1;
END;
COMMIT;
DBCC SQLPERF(LOGSPACE);

--Listing 15-13
DBCC SQLPERF(LOGSPACE);
BEGIN TRANSACTION;
WITH Tally
AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS num
    FROM master.dbo.syscolumns AS A
        CROSS JOIN master.dbo.syscolumns AS B)
INSERT INTO dbo.Test1
(
    C1
)
SELECT TOP 1000
       (Tally.num % 256)
FROM Tally;
COMMIT;
DBCC SQLPERF(LOGSPACE);

--Listing 15-14
SELECT * FROM <TableName> WITH(PAGLOCK);  --Use page level lock

--Listing 15-15
ALTER DATABASE <DatabaseName> SET READ_ONLY;

--Listing 15-18
DELETE Sales.SalesOrderDetail
FROM Sales.SalesOrderDetail AS sod WITH (NOLOCK)
    JOIN Production.Product AS p WITH (NOLOCK)
        ON sod.ProductID = p.ProductID
           AND p.ProductID = 0;


--Listing 16-1
DROP TABLE IF EXISTS dbo.ProductTest;
GO
CREATE TABLE dbo.ProductTest
(
    ProductID INT
        CONSTRAINT ValueEqualsOne CHECK (ProductID = 1)
);
GO
--All ProductIDs are added into ProductTest as a logical unit of work
INSERT INTO dbo.ProductTest
SELECT p.ProductID
FROM Production.Product AS p;
GO
SELECT pt.ProductID
FROM dbo.ProductTest AS pt; --Returns 0 rows


--Listing 16-2
BEGIN TRAN;
--Start:  Logical unit of work
--First:
INSERT INTO dbo.ProductTest
SELECT p.ProductID
FROM Production.Product AS p;
--Second:
INSERT INTO dbo.ProductTest
VALUES
(1);
COMMIT; --End:   Logical unit of work
GO

--Listing 16-3
SET XACT_ABORT ON;
GO
BEGIN TRAN;
--Start:  Logical unit of work
--First:
INSERT INTO dbo.ProductTest
SELECT p.ProductID
FROM Production.Product AS p;
--Second:
INSERT INTO dbo.ProductTest
VALUES
(1  );
COMMIT;
--End:   Logical unit of work GO
SET XACT_ABORT OFF;
GO

--Listing 16-4
BEGIN TRY
    BEGIN TRAN;
    --Start: Logical unit of work
    --First:
    INSERT INTO dbo.ProductTest
    SELECT p.ProductID
    FROM Production.Product AS p;
    Second:
    INSERT INTO dbo.ProductTest
    (
        ProductID
    )
    VALUES
    (1  );
    COMMIT; --End: Logical unit of work
END TRY
BEGIN CATCH
    ROLLBACK;
    PRINT 'An error occurred';
    RETURN;
END CATCH;


--Listing 16-5
DROP TABLE IF EXISTS dbo.LockTest;
CREATE TABLE dbo.LockTest
(
    C1 INT
);
INSERT INTO dbo.LockTest
VALUES
(1);
GO
BEGIN TRAN;
DELETE dbo.LockTest
WHERE C1 = 1;
SELECT dtl.request_session_id,
       dtl.resource_database_id,
       dtl.resource_associated_entity_id,
       dtl.resource_type,
       dtl.resource_description,
       dtl.request_mode,
       dtl.request_status
FROM sys.dm_tran_locks AS dtl
WHERE dtl.request_session_id = @@SPID;
ROLLBACK;


--Listing 16-6
SELECT OBJECT_NAME(404196490),
       DB_NAME(5);


--Listing 16-7
CREATE CLUSTERED INDEX TestIndex ON dbo.LockTest (C1);


--Listing 16-8
BEGIN TRAN;
DELETE dbo.LockTest
WHERE C1 = 1;
SELECT dtl.request_session_id,
       dtl.resource_database_id,
       dtl.resource_associated_entity_id,
       dtl.resource_type,
       dtl.resource_description,
       dtl.request_mode,
       dtl.request_status
FROM sys.dm_tran_locks AS dtl
WHERE dtl.request_session_id = @@SPID;
ROLLBACK;


--Listing 16-9
ALTER TABLE schema.table
SET (LOCK_ESCALATION = DISABLE);


--Table 16-1
--Multiple scripts to be run in multiple T-SQL windows to show multiple connections
--Technically Listing 16-10, but since the code is carefully broken out in a table
--I'm continuing the numbering scheme below
--Window 1 - Connection 1
BEGIN TRANSACTION LockTran2;
--Retain an  (S) lock on the resource
SELECT *
FROM Sales.Currency AS c WITH (REPEATABLEREAD)
WHERE c.CurrencyCode = 'EUR';
--Allow DMVs to be executed before second step of
-- UPDATE statement is executed by transaction LockTran1
WAITFOR DELAY '00:00:10';
COMMIT;

--Window 2 - Connection 2
BEGIN TRANSACTION LockTran1;
UPDATE Sales.Currency
SET Name = 'Euro'
WHERE CurrencyCode = 'EUR';
-- NOTE: We're not committing yet


--To be run separately from above
--after 10 second delay
COMMIT;


--Window 3 - Connection 3
SELECT dtl.request_session_id,
       dtl.resource_database_id,
       dtl.resource_associated_entity_id,
       dtl.resource_type,
       dtl.resource_description,
       dtl.request_mode,
       dtl.request_status
FROM sys.dm_tran_locks AS dtl
ORDER BY dtl.request_session_id;



--Listing 16-10
BEGIN TRAN;
DELETE Sales.Currency
WHERE CurrencyCode = 'ALL';
SELECT tl.request_session_id,
       tl.resource_database_id,
       tl.resource_associated_entity_id,
       tl.resource_type,
       tl.resource_description,
       tl.request_mode,
       tl.request_status
FROM sys.dm_tran_locks AS tl;
ROLLBACK TRAN;


--Listing 16-11
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

--Listing 16-12
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

--Listing 16-13
ALTER DATABASE AdventureWorks SET READ_COMMITTED_SNAPSHOT ON;

--Listing 16-14
BEGIN TRANSACTION;
SELECT p.Color
FROM Production.Product AS p
WHERE p.ProductID = 711;
--COMMIT TRANSACTION

--Listing 16-15
BEGIN TRANSACTION;
UPDATE Production.Product
SET Color = 'Coyote'
WHERE ProductID = 711;
--test that change
SELECT p.Color
FROM Production.Product AS p
WHERE p.ProductID = 711;
--COMMIT TRANSACTION

--Listing 16-16
ALTER DATABASE AdventureWorks SET READ_COMMITTED_SNAPSHOT OFF;


--Listing 16-17
DROP TABLE IF EXISTS dbo.MyProduct;
GO
CREATE TABLE dbo.MyProduct
(
    ProductID INT,
    Price MONEY
);
INSERT INTO dbo.MyProduct
VALUES
(1, 15.0),
(2, 22.0),
(3, 9.99);


--Listing 16-18
DECLARE @Price INT;
BEGIN TRAN NormailizePrice;
SELECT @Price = mp.Price
FROM dbo.MyProduct AS mp
WHERE mp.ProductID = 1;
/*Allow transaction 2 to execute*/
WAITFOR DELAY '00:00:10';
IF @Price > 10
    UPDATE dbo.MyProduct
    SET Price = Price - 10
    WHERE ProductID = 1;
COMMIT;

--Transaction 2 from Connection 2
BEGIN TRAN ApplyDiscount;
UPDATE dbo.MyProduct
SET Price = Price * 0.6 --Discount = 40%
WHERE Price > 10;
COMMIT;

--Listing 16-19
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
GO
--Transaction 1 from Connection 1
DECLARE @Price INT;
BEGIN TRAN NormalizePrice;
SELECT @Price = Price
FROM dbo.MyProduct AS mp
WHERE mp.ProductID = 1;
/*Allow transaction 2 to execute*/
WAITFOR DELAY '00:00:10';
IF @Price > 10
    UPDATE dbo.MyProduct
    SET Price = Price - 10
    WHERE ProductID = 1;
COMMIT;
GO
SET TRANSACTION ISOLATION LEVEL READ COMMITTED; --Back to default
GO

--Listing 16-20
DROP TABLE IF EXISTS dbo.MyEmployees;
GO
CREATE TABLE dbo.MyEmployees
(
    EmployeeID INT,
    GroupID INT,
    Salary MONEY
);
CREATE CLUSTERED INDEX GroupIDIndex ON dbo.MyEmployees (GroupID);
INSERT INTO dbo.MyEmployees
VALUES
(1, 10, 1000),
(2, 10, 1000),
(3, 20, 1000),
(4, 9, 1000);


--Listing 16-21
--Transaction 1 from Connection 1
DECLARE @Fund MONEY = 100,
        @Bonus MONEY,
        @NumberOfEmployees INT;
BEGIN TRAN PayBonus;
SELECT @NumberOfEmployees = COUNT(*)
FROM dbo.MyEmployees
WHERE GroupID = 10;
/*Allow transaction 2 to execute*/
WAITFOR DELAY '00:00:10';
IF @NumberOfEmployees > 0
BEGIN
    SET @Bonus = @Fund / @NumberOfEmployees;
    UPDATE dbo.MyEmployees
    SET Salary = Salary + @Bonus
    WHERE GroupID = 10;
    PRINT 'Fund balance =
' + CAST((@Fund - (@@ROWCOUNT * @Bonus)) AS VARCHAR(6)) + '   $';
END;
COMMIT;

--Transaction 2 from Connect 2
BEGIN TRAN NewEmployee;
INSERT INTO dbo.MyEmployees
VALUES
(5, 10, 1000);
COMMIT;

--Listing 16-22
DROP TABLE IF EXISTS dbo.LockTest;
GO
CREATE TABLE dbo.LockTest
(
    C1 INT,
    C2 DATETIME
);
INSERT INTO dbo.LockTest
VALUES
(1, GETDATE());

CREATE NONCLUSTERED INDEX TestIndex ON dbo.LockTest (C1);

--Listing 16-23
BEGIN TRAN LockBehavior;
UPDATE dbo.LockTest WITH (REPEATABLEREAD) --Hold all acquired locks
SET C2 = GETDATE()
WHERE C1 = 1;
--Observe lock behavior from another connection
WAITFOR DELAY '00:00:10';
COMMIT;

--Listing 16-24
ALTER INDEX TestIndex
ON dbo.LockTest
SET (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF);


--Listing 16-25
CREATE CLUSTERED INDEX TestIndex ON dbo.LockTest (C1) WITH DROP_EXISTING;

--Listing 16-26
SELECT dtl.request_session_id AS WaitingSessionID,
       der.blocking_session_id AS BlockingSessionID,
       dowt.resource_description,
       der.wait_type,
       dowt.wait_duration_ms,
       DB_NAME(dtl.resource_database_id) AS DatabaseName,
       dtl.resource_associated_entity_id AS WaitingAssociatedEntity,
       dtl.resource_type AS WaitingResourceType,
       dtl.request_type AS WaitingRequestType,
       dest.[text] AS WaitingTSql,
       dtlbl.request_type BlockingRequestType,
       destbl.[text] AS BlockingTsql
FROM sys.dm_tran_locks AS dtl
    JOIN sys.dm_os_waiting_tasks AS dowt
        ON dtl.lock_owner_address = dowt.resource_address
    JOIN sys.dm_exec_requests AS der
        ON der.session_id = dtl.request_session_id
    CROSS APPLY sys.dm_exec_sql_text(der.sql_handle) AS dest
    LEFT JOIN sys.dm_exec_requests derbl
        ON derbl.session_id = dowt.blocking_session_id
    OUTER APPLY sys.dm_exec_sql_text(derbl.sql_handle) AS destbl
    LEFT JOIN sys.dm_tran_locks AS dtlbl
        ON derbl.session_id = dtlbl.request_session_id;

--Listing 16-27
DROP TABLE IF EXISTS dbo.BlockTest;
GO
CREATE TABLE dbo.BlockTest
(
    C1 INT,
    C2 INT,
    C3 DATETIME
);
INSERT INTO dbo.BlockTest
VALUES
(11, 12, GETDATE()),
(21, 22, GETDATE());

--Listing 16-28
--First connection, executed first
BEGIN TRAN User1;
UPDATE dbo.BlockTest
SET C3 = GETDATE();
--rollback transaction

--Second connection, executed second
BEGIN TRAN User2;
SELECT C2
FROM dbo.BlockTest
WHERE C1 = 11;
COMMIT;

--Listing 16-29
EXEC sp_configure 'show advanced option', '1';
RECONFIGURE;
EXEC sp_configure 'blocked process threshold', 5;
RECONFIGURE;

--Listing 16-30
CREATE EVENT SESSION BlockedProcess
ON SERVER
    ADD EVENT sqlserver.blocked_process_report;

--Listing 17-1
SET DEADLOCK_PRIORITY LOW;

--Listing 17-2
DBCC TRACEON (1222, -1);

--Listing 17-3
DECLARE @path NVARCHAR(260);
--to retrieve the local path of system_health files
SELECT @path = dosdlc.PATH
FROM sys.dm_os_server_diagnostics_log_configurations AS dosdlc;
SELECT @path = @path + N'system_health_*';
WITH fxd
AS (SELECT CAST(fx.event_data AS XML) AS Event_Data
    FROM sys.fn_xe_file_target_read_file(@path, NULL, NULL, NULL) AS fx )
SELECT dl.deadlockgraph
FROM
(
    SELECT dl.query('.') AS deadlockgraph
    FROM fxd
        CROSS APPLY event_data.nodes('(/event/data/value/deadlock)') AS d(dl)
) AS dl;

--Listing 17-4
--Run from connection 1
BEGIN TRANSACTION PODSecond;
UPDATE Purchasing.PurchaseOrderHeader
SET Freight = Freight * 0.9 --9% discount on shipping
WHERE PurchaseOrderID = 1255;

--Run from connection 2
BEGIN TRANSACTION PODFirst;
UPDATE Purchasing.PurchaseOrderDetail
SET OrderQty = 2
WHERE ProductID = 448
      AND PurchaseOrderID = 1255;

--Run from connection 1
UPDATE Purchasing.PurchaseOrderDetail
SET OrderQty = 4
WHERE ProductID = 448
      AND PurchaseOrderID = 1255;

--probably not needed
--ROLLBACK

--Listing 17-5
SELECT OBJECT_NAME(object_id)
FROM sys.partitions
WHERE hobt_id = 72057594050969600;

--Listing 17-6
DECLARE @retry AS TINYINT = 1,
        @retrymax AS TINYINT = 2,
        @retrycount AS TINYINT = 0;
WHILE @retry = 1 AND @retrycount <= @retrymax
BEGIN
    SET @retry = 0;
    BEGIN TRY
        UPDATE HumanResources.Employee
        SET LoginID = '54321'
        WHERE BusinessEntityID = 100;
    END TRY
    BEGIN CATCH
        IF (ERROR_NUMBER() = 1205)
        BEGIN
            SET @retrycount = @retrycount + 1;
            SET @retry = 1;
        END;
    END CATCH;
END;

--Listing 18-1
DECLARE MyCursor CURSOR READ_ONLY FOR
SELECT adt.NAME
FROM Person.AddressType AS adt
WHERE adt.AddressTypeID = 1;

--Listing 18-2
DECLARE MyCursor CURSOR OPTIMISTIC FOR
SELECT adt.NAME
FROM Person.AddressType AS adt
WHERE adt.AddressTypeID = 1;

--Listing 18-3
DECLARE MyCursor CURSOR SCROLL_LOCKS FOR
SELECT adt.NAME
FROM Person.AddressType AS adt
WHERE adt.AddressTypeID = 1;

--Listing 18-4
DECLARE MyCursor CURSOR FAST_FORWARD FOR
SELECT adt.NAME
FROM Person.AddressType AS adt
WHERE adt.AddressTypeID = 1;

--Listing 18-5
DECLARE MyCursor CURSOR STATIC FOR
SELECT adt.NAME
FROM Person.AddressType AS adt
WHERE adt.AddressTypeID = 1;

--Listing 18-6
DECLARE MyCursor CURSOR KEYSET FOR
SELECT adt.NAME
FROM Person.AddressType AS adt
WHERE adt.AddressTypeID = 1;

--Listing 18-7
DECLARE MyCursor CURSOR DYNAMIC FOR
SELECT adt.NAME
FROM Person.AddressType AS adt
WHERE adt.AddressTypeID = 1;

--Listing 18-8
--A table for identifying SalesOrderID values based on iteration
DECLARE @LoopTable TABLE
(
    LoopID INT IDENTITY(1, 1),
    SalesOrderDetailID INT
);

--defining our data set through a query
INSERT INTO @LoopTable
(
    SalesOrderDetailID
)
SELECT sod.SalesOrderDetailID
FROM Sales.SalesOrderDetail AS sod
WHERE sod.OrderQty > 23
ORDER BY sod.SalesOrderDetailID DESC;

DECLARE @MaxRow INT,
        @Count INT,
        @SalesOrderDetailID INT;

--retrieving the limit of the data set
SELECT @MaxRow = MAX(lt.LoopID),
       @Count = 1
FROM @LoopTable AS lt;

--looping through the results
WHILE @Count <= @MaxRow
BEGIN
    SELECT @SalesOrderDetailID = lt.SalesOrderDetailID
    FROM @LoopTable AS lt
    WHERE lt.LoopID = @Count;

    SELECT sod.OrderQty
    FROM Sales.SalesOrderDetail AS sod
    WHERE sod.SalesOrderDetailID = @SalesOrderDetailID;

    SET @Count += 1;

END;

--Listing 18-9
DROP TABLE IF EXISTS dbo.Test1;
GO
CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 CHAR(996)
);
CREATE CLUSTERED INDEX Test1Index ON dbo.Test1 (C1);
INSERT INTO dbo.Test1
VALUES
(1, '1'),
(2, '2');
GO

--Listing 18-10
--Powershell, not T-SQL
$AdoConn = New-Object -comobject ADODB.Connection
$AdoRecordset = New-Object -comobject ADODB.Recordset
##Change the Data Source to your server
$AdoConn.Open("Provider= SQLOLEDB; Data Source=(localhost); Initial Catalog=AdventureWorks; User id=sa; password=$cthulhu1988")
$AdoRecordset.Open("SELECT * FROM dbo.Test1", $AdoConn)
do {
    $C1 = $AdoRecordset.Fields.Item("C1").Value
    $C2 = $AdoRecordset.Fields.Item("C2").Value
    Write-Output "C1 = $C1 and C2 = $C2"
    $AdoRecordset.MoveNext()
    } until     ($AdoRecordset.EOF -eq $True)
$AdoRecordset.Close()
$AdoConn.Close()


--Listing 18-11
SELECT TOP 100000
       IDENTITY(INT, 1, 1) AS n
INTO #Tally
FROM MASTER.dbo.syscolumns AS scl,
     MASTER.dbo.syscolumns AS sc2;
INSERT INTO dbo.Test1
(
    C1,
    C2
)
SELECT n,
       n
FROM #Tally AS t;

GO
--Listing 18-12
CREATE OR ALTER PROC dbo.TotalLossCursorBased
AS 
DECLARE ScrappedProducts CURSOR FOR
SELECT p.ProductID,
       wo.ScrappedQty,
       p.ListPrice
FROM Production.WorkOrder AS wo
    JOIN Production.ScrapReason AS sr
        ON wo.ScrapReasonID = sr.ScrapReasonID
    JOIN Production.Product AS p
        ON wo.ProductID = p.ProductID;
--Open the cursor to process one product at a time
OPEN ScrappedProducts;
DECLARE @MoneyLostPerProduct MONEY = 0,
        @TotalLoss MONEY = 0;
--Calculate money lost per product by processing one product
--at a time
DECLARE @ProductId INT,
        @UnitsScrapped SMALLINT,
        @ListPrice MONEY;
FETCH NEXT FROM ScrappedProducts
INTO @ProductId,
     @UnitsScrapped,
     @ListPrice;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @MoneyLostPerProduct = @UnitsScrapped * @ListPrice; --Calculate total loss
    SET @TotalLoss = @TotalLoss + @MoneyLostPerProduct;
    FETCH NEXT FROM ScrappedProducts
    INTO @ProductId,
         @UnitsScrapped,
         @ListPrice;
END;
--Determine status
IF (@TotalLoss > 5000)
    SELECT 'We are bankrupt!' AS STATUS;
ELSE
    SELECT 'We are safe!' AS STATUS;
--Close the cursor and release all resources assigned to the cursor
CLOSE ScrappedProducts;
DEALLOCATE ScrappedProducts;
GO

--Listing 18-13
EXEC dbo.TotalLossCursorBased;
GO 50

--Listing 18-14
CREATE OR ALTER PROC dbo.TotalLoss
AS
SELECT CASE --Determine status based on following computation
           WHEN SUM(MoneyLostPerProduct) > 5000 THEN
               'We are bankrupt!'
           ELSE
               'We are safe!'
       END AS STATUS
FROM
( --Calculate total money lost for all discarded products
    SELECT SUM(wo.ScrappedQty * p.ListPrice) AS MoneyLostPerProduct
    FROM Production.WorkOrder AS wo
        JOIN Production.ScrapReason AS sr
            ON wo.ScrapReasonID = sr.ScrapReasonID
        JOIN Production.Product AS p
            ON wo.ProductID = p.ProductID
    GROUP BY p.ProductID
) AS DiscardedProducts;
GO

EXEC dbo.TotalLoss;
GO 50


--Listing 19-1
CREATE DATABASE InMemoryTest
ON PRIMARY
       (
           NAME = N'InMemoryTest_Data',
           FILENAME = '/var/opt/mssql/data/inmemorytest_data.mdf',
           SIZE = 5GB
       )
LOG ON
    (
        NAME = N'InMemoryTest_Log',
        FILENAME = '/var/opt/mssql/data/inmemorytest_log.ldf'
    );


--Listing 19-2
ALTER DATABASE InMemoryTest
ADD FILEGROUP InMemoryTest_InMemoryData
CONTAINS MEMORY_OPTIMIZED_DATA;
ALTER DATABASE InMemoryTest
ADD FILE
    (
        NAME = 'InMemoryTest_InMemoryData',
        FILENAME = '/var/opt/mssql/data/inmemorytest_inmemorydata.ndf'
    )
TO FILEGROUP InMemoryTest_InMemoryData;


--Listing 19-3
USE InMemoryTest;
GO
CREATE TABLE dbo.ADDRESS
(
    AddressID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY NONCLUSTERED HASH
                                          WITH (BUCKET_COUNT = 50000),
    AddressLine1 NVARCHAR(60) NOT NULL,
    AddressLine2 NVARCHAR(60) NULL,
    City NVARCHAR(30) NOT NULL,
    StateProvinceID INT NOT NULL,
    PostalCode NVARCHAR(15) NOT NULL,
    --[SpatialLocation geography NULL,
    --rowguid uniqueidentifier ROWGUIDCOL  NOT NULL CONSTRAINT DF_Address_rowguid  DEFAULT (newid()),
    ModifiedDate DATETIME NOT NULL
        CONSTRAINT DF_Address_ModifiedDate
            DEFAULT (GETDATE())
)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);

--Listing 19-4
CREATE TABLE dbo.AddressStaging
(
    AddressLine1 NVARCHAR(60) NOT NULL,
    AddressLine2 NVARCHAR(60) NULL,
    City NVARCHAR(30) NOT NULL,
    StateProvinceID INT NOT NULL,
    PostalCode NVARCHAR(15) NOT NULL
);
INSERT dbo.AddressStaging
(
    AddressLine1,
    AddressLine2,
    City,
    StateProvinceID,
    PostalCode
)
SELECT A.AddressLine1,
       A.AddressLine2,
       A.City,
       A.StateProvinceID,
       A.PostalCode
FROM AdventureWorks.Person.ADDRESS AS A;
INSERT dbo.ADDRESS
(
    AddressLine1,
    AddressLine2,
    City,
    StateProvinceID,
    PostalCode
)
SELECT A.AddressLine1,
       A.AddressLine2,
       A.City,
       A.StateProvinceID,
       A.PostalCode
FROM dbo.AddressStaging AS A;
DROP TABLE dbo.AddressStaging;

--Listing 19-5
CREATE TABLE dbo.StateProvince
(
    StateProvinceID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY NONCLUSTERED HASH
                                                WITH (BUCKET_COUNT = 10000),
    StateProvinceCode NCHAR(3) COLLATE Latin1_General_100_BIN2 NOT NULL,
    CountryRegionCode NVARCHAR(3) NOT NULL,
    NAME VARCHAR(50) NOT NULL,
    TerritoryID INT NOT NULL,
    ModifiedDate DATETIME NOT NULL
        CONSTRAINT DF_StateProvince_ModifiedDate
            DEFAULT (GETDATE())
)
WITH (MEMORY_OPTIMIZED = ON);
CREATE TABLE dbo.CountryRegion
(
    CountryRegionCode NVARCHAR(3) NOT NULL,
    NAME VARCHAR(50) NOT NULL,
    ModifiedDate DATETIME NOT NULL
        CONSTRAINT DF_CountryRegion_ModifiedDate
            DEFAULT (GETDATE()),
    CONSTRAINT PK_CountryRegion_CountryRegionCode
        PRIMARY KEY CLUSTERED (CountryRegionCode ASC)
);
GO
SELECT sp.StateProvinceCode,
       sp.CountryRegionCode,
       sp.NAME,
       sp.TerritoryID
INTO dbo.StateProvinceStaging
FROM AdventureWorks.Person.StateProvince AS sp;
INSERT dbo.StateProvince
(
    StateProvinceCode,
    CountryRegionCode,
    NAME,
    TerritoryID
)
SELECT StateProvinceCode,
       CountryRegionCode,
       NAME,
       TerritoryID
FROM dbo.StateProvinceStaging;
DROP TABLE dbo.StateProvinceStaging;
INSERT dbo.CountryRegion
(
    CountryRegionCode,
    NAME
)
SELECT cr.CountryRegionCode,
       cr.NAME
FROM AdventureWorks.Person.CountryRegion AS cr;
GO

--Listing 19-6
SELECT A.AddressLine1,
       A.City,
       A.PostalCode,
       sp.NAME AS StateProvinceName,
       cr.NAME AS CountryName
FROM dbo.ADDRESS AS A
    JOIN dbo.StateProvince AS sp
        ON sp.StateProvinceID = A.StateProvinceID
    JOIN dbo.CountryRegion AS cr
        ON cr.CountryRegionCode = sp.CountryRegionCode
WHERE A.AddressID = 42;
GO 50

USE AdventureWorks;
go
--same query in AdventureWorks
SELECT A.AddressLine1,
       A.City,
       A.PostalCode,
       sp.NAME AS StateProvinceName,
       cr.NAME AS CountryName
FROM Person.ADDRESS AS A
    JOIN Person.StateProvince AS sp
        ON sp.StateProvinceID = A.StateProvinceID
    JOIN Person.CountryRegion AS cr
        ON cr.CountryRegionCode = sp.CountryRegionCode
WHERE A.AddressID = 42;
GO 50


USE InMemoryTest;
--Listing 19-7
SELECT i.NAME AS [index name],
       hs.total_bucket_count,
       hs.empty_bucket_count,
       hs.avg_chain_length,
       hs.max_chain_length
FROM sys.dm_db_xtp_hash_index_stats AS hs
    JOIN sys.indexes AS i
        ON hs.OBJECT_ID = i.OBJECT_ID
           AND hs.index_id = i.index_id
WHERE OBJECT_NAME(hs.OBJECT_ID) = 'Address';


--Listing 19-8
SELECT A.AddressLine1,
       A.City,
       A.PostalCode,
       sp.NAME AS StateProvinceName,
       cr.NAME AS CountryName
FROM dbo.ADDRESS AS A
    JOIN dbo.StateProvince AS sp
        ON sp.StateProvinceID = A.StateProvinceID
    JOIN dbo.CountryRegion AS cr
        ON cr.CountryRegionCode = sp.CountryRegionCode
WHERE A.City = 'Walla Walla';
GO 50

--Listing 19-9
ALTER TABLE dbo.ADDRESS ADD INDEX nci (City);


ALTER TABLE dbo.Address DROP INDEX nci;

--Listing 19-10
SELECT s.NAME,
       s.stats_id,
       ddsp.last_updated,
       ddsp.ROWS,
       ddsp.rows_sampled,
       ddsp.unfiltered_rows,
       ddsp.persisted_sample_percent,
       ddsp.steps
FROM sys.STATS AS s
    CROSS APPLY sys.dm_db_stats_properties(s.OBJECT_ID, s.stats_id) AS ddsp
WHERE s.OBJECT_ID = OBJECT_ID('Address');


--Listing 19-11
SELECT ddsh.step_number,
       ddsh.range_high_key,
       ddsh.range_rows,
       ddsh.equal_rows,
       ddsh.distinct_range_rows,
       ddsh.average_range_rows
FROM sys.dm_db_stats_histogram(OBJECT_ID('Address'), 2) AS ddsh;


--Listing 19-12
UPDATE STATISTICS dbo.ADDRESS
WITH FULLSCAN,
     NORECOMPUTE;


--Listing 19-13
DROP TABLE IF EXISTS dbo.CountryRegion;
go
CREATE TABLE dbo.CountryRegion
(
    CountryRegionCode NVARCHAR(3) NOT NULL,
    NAME VARCHAR(50) NOT NULL,
    ModifiedDate DATETIME NOT NULL
        CONSTRAINT DF_CountryRegion_ModifiedDate
            DEFAULT (GETDATE()),
    CONSTRAINT PK_CountryRegion_CountryRegionCode
        PRIMARY KEY NONCLUSTERED (CountryRegionCode ASC)
)
WITH (MEMORY_OPTIMIZED = ON);
GO
SELECT cr.CountryRegionCode,
       cr.NAME
INTO dbo.CountryRegionStaging
FROM AdventureWorks.Person.CountryRegion AS cr;
go
INSERT dbo.CountryRegion
(
    CountryRegionCode,
    NAME
)
SELECT cr.CountryRegionCode,
       cr.NAME
FROM dbo.CountryRegionStaging AS cr;
GO
DROP TABLE dbo.CountryRegionStaging;


--Listing 19-14
CREATE PROC dbo.AddressDetails @City NVARCHAR(30)
WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER AS
BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')
    SELECT A.AddressLine1,
           A.City,
           A.PostalCode,
           sp.NAME AS StateProvinceName,
           cr.NAME AS CountryName
    FROM dbo.ADDRESS AS A
        JOIN dbo.StateProvince AS sp
            ON sp.StateProvinceID = A.StateProvinceID
        JOIN dbo.CountryRegion
        AS
        cr
            ON cr.CountryRegionCode = sp.CountryRegionCode
    WHERE A.City = @City;
END;
GO

EXEC dbo.AddressDetails @City = N'Walla Walla';
GO 50

--Listing 19-15
USE InMemoryTest;
GO
CREATE TABLE dbo.AddressMigrate
(
    AddressID INT NOT NULL IDENTITY(1, 1) PRIMARY KEY,
    AddressLine1 NVARCHAR(60) NOT NULL,
    AddressLine2 NVARCHAR(60) NULL,
    City NVARCHAR(30) NOT NULL,
    StateProvinceID INT NOT NULL,
    PostalCode NVARCHAR(15) NOT NULL
);

--Listing 19-16
CREATE OR ALTER PROCEDURE dbo.FailWizard
(@City NVARCHAR(30))
AS
SELECT A.AddressLine1,
       A.City,
       A.PostalCode,
       sp.NAME AS StateProvinceName,
       cr.NAME AS CountryName
FROM dbo.ADDRESS AS A
    JOIN dbo.StateProvince AS sp
        ON sp.StateProvinceID = A.StateProvinceID
    JOIN dbo.CountryRegion AS cr WITH (NOLOCK)
        ON cr.CountryRegionCode = sp.CountryRegionCode
WHERE A.City = @City;
GO
CREATE OR ALTER PROCEDURE dbo.PassWizard
(@City NVARCHAR(30))
AS
SELECT A.AddressLine1,
       A.City,
       A.PostalCode,
       sp.NAME AS StateProvinceName,
       cr.NAME AS CountryName
FROM dbo.ADDRESS AS A
    JOIN dbo.StateProvince AS sp
        ON sp.StateProvinceID = A.StateProvinceID
    JOIN dbo.CountryRegion AS cr
        ON cr.CountryRegionCode = sp.CountryRegionCode
WHERE A.City = @City;
GO

--Listing 20-1
CREATE DATABASE RadioGraph;
GO
USE RadioGraph;
GO
--Create schema to hold different structures
CREATE SCHEMA grph;
CREATE SCHEMA rel;
GO


--Listing 20-2
CREATE TABLE grph.RadioOperator
(
    RadioOperatorID int IDENTITY(1, 1) NOT NULL,
    OperatorName varchar(50) NOT NULL,
    CallSign varchar(9) NOT NULL
) AS NODE;
CREATE TABLE grph.Frequency
(
    FrequencyID int IDENTITY(1, 1) NOT NULL,
    FrequencyValue decimal(6, 3) NOT NULL,
    Band varchar(12) NOT NULL,
    FrequencyUnit varchar(3) NOT NULL
) AS NODE;
CREATE TABLE grph.Radio
(
    RadioID int IDENTITY(1, 1),
    RadioName varchar(50) NOT NULL
) AS NODE;


--Listing 20-3
CREATE TABLE grph.Calls AS EDGE;

CREATE TABLE grph.Uses AS EDGE;

--Listing 20-4
INSERT INTO grph.RadioOperator
(
    OperatorName,
    CallSign
)
VALUES
('Grant Fritchey', 'KC1KCE'),
('Bob McCall', 'QQ5QQQ'),
('Abigail Serrano', 'VQ5ZZZ'),
('Josephine Wykovic', 'YQ9LLL');

INSERT INTO grph.Frequency
(
    FrequencyValue,
    Band,
    FrequencyUnit
)
VALUES
(14.250, '20 Meters', 'MHz'),
(145.520, '2 Meters', 'MHz'),
(478, '630 Meters', 'kHz'),
(14.225, '20 Meters', 'MHz'),
(14.3, '20 Meters', 'MHz'),
(7.18, '40 Meters', 'MHz');

INSERT INTO grph.Radio
(
    RadioName
)
VALUES
('Yaesu FT-3'),
('Baofeng UV5'),
('Icom 7300'),
('Raddiodity GD-88'),
('Xiegu G90');

--Listing 20-5
INSERT INTO grph.Uses
(
    $from_id,
    $to_id
)
VALUES
(
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 1
    ),
    (
        SELECT $node_id FROM grph.Radio AS r WHERE r.RadioID = 1
    ));


--Listing 20-6
--edges for operator uses radio
INSERT INTO grph.Uses
(
    $from_id,
    $to_id
)
VALUES
(
    (
        SELECT ro.$node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 1
    ),
    (
        SELECT r.$node_id FROM grph.Radio AS r WHERE r.RadioID = 2
    )),
(
    (
        SELECT ro.$node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 1
    ),
    (
        SELECT r.$node_id FROM grph.Radio AS r WHERE r.RadioID = 3
    )),
(
    (
        SELECT ro.$node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 2
    ),
    (
        SELECT r.$node_id FROM grph.Radio AS r WHERE r.RadioID = 2
    )),
(
    (
        SELECT ro.$node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 3
    ),
    (
        SELECT r.$node_id FROM grph.Radio AS r WHERE r.RadioID = 4
    )),
(
    (
        SELECT ro.$node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 1
    ),
    (
        SELECT r.$node_id FROM grph.Radio AS r WHERE r.RadioID = 5
    )),
(
    (
        SELECT ro.$node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 3
    ),
    (
        SELECT r.$node_id FROM grph.Radio AS r WHERE r.RadioID = 1
    )),
(
    (
        SELECT ro.$node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 4
    ),
    (
        SELECT r.$node_id FROM grph.Radio AS r WHERE r.RadioID = 1
    ));

--edges for radio uses frequency
INSERT INTO grph.Uses
(
    $from_id,
    $to_id
)
VALUES
(
    (
        SELECT $node_id FROM grph.Radio AS r WHERE r.RadioID = 1
    ),
    (
        SELECT $node_id FROM grph.Frequency AS F WHERE F.FrequencyID = 2
    )),
(
    (
        SELECT $node_id FROM grph.Radio AS r WHERE r.RadioID = 2
    ),
    (
        SELECT $node_id FROM grph.Frequency AS F WHERE F.FrequencyID = 2
    )),
(
    (
        SELECT $node_id FROM grph.Radio AS r WHERE r.RadioID = 1
    ),
    (
        SELECT $node_id FROM grph.Radio AS r WHERE r.RadioID = 2
    ));

--edges for calls
INSERT INTO grph.Calls
(
    $from_id,
    $to_id
)
VALUES
(
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 1
    ),
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 2
    )),
(
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 1
    ),
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 3
    )),
(
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 2
    ),
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 3
    )),
(
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 3
    ),
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 4
    )),
(
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 3
    ),
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 1
    )),
(
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 3
    ),
    (
        SELECT $node_id FROM grph.RadioOperator AS ro WHERE ro.RadioOperatorID = 2
    ));


--Listing 20-7
SELECT Calling.OperatorName,
       Calling.CallSign,
       Called.OperatorName,
       Called.CallSign
FROM grph.RadioOperator AS Calling,
     grph.Calls AS C,
     grph.RadioOperator AS Called
WHERE MATCH(Calling-(C)->Called);
GO 50

--Listing 20-8
--Relational comparison
CREATE TABLE rel.RadioOperator
(
    RadioOperatorID int IDENTITY(1, 1) NOT NULL,
    OperatorName varchar(50) NOT NULL,
    CallSign varchar(9) NOT NULL
);

CREATE TABLE rel.RadioOperatorCall
(
    CallingOperatorID INT NOT NULL,
    CalledOperatorID INT NOT NULL
);

INSERT INTO rel.RadioOperator
(
    OperatorName,
    CallSign
)
VALUES
('Grant Fritchey', 'KC1KCE'),
('Bob McCall', 'QQ5QQQ'),
('Abigail Serrano', 'VQ5ZZZ'),
('Josephine Wykovic', 'YQ9LLL');

INSERT INTO rel.RadioOperatorCall
(
    CallingOperatorID,
    CalledOperatorID
)
VALUES
(1, 2),
(1, 3),
(2, 3),
(3, 1),
(3, 2);


--Listing 20-9
SELECT Calling.OperatorName,
       calling.CallSign,
       CALLED.OperatorName,
       CALLED.CallSign
FROM rel.RadioOperator AS Calling
    JOIN rel.RadioOperatorCall AS roc
        ON Calling.RadioOperatorID = roc.CallingOperatorID
    JOIN rel.RadioOperator AS CALLED
        ON roc.CalledOperatorID = CALLED.RadioOperatorID;
GO 50


--Listing 20-10
SELECT Calling.OperatorName,
       Calling.CallSign,
       CALLED.OperatorName,
       CALLED.CallSign,
       TheyCalled.OperatorName,
       TheyCalled.CallSign
FROM grph.RadioOperator AS Calling,
     grph.Calls AS C,
     grph.RadioOperator AS CALLED,
     grph.Calls AS C2,
     grph.RadioOperator AS TheyCalled
WHERE MATCH(Calling-(C)->CALLED-(C2)->TheyCalled)
            AND Calling.RadioOperatorID = 1;


--Listing 20-11
SELECT AllCalled.CallSign AS WasCalledBy,
       WeCalled.CallSign AS CALLED,
       TheyCalled.CallSign AS AlsoCalled
FROM grph.RadioOperator AS AllCalled,
     grph.RadioOperator AS WeCalled,
     grph.RadioOperator AS TheyCalled,
     grph.Calls AS C,
     grph.Calls AS C2
WHERE MATCH(Wecalled-(C)->Allcalled<-(C2)-TheyCalled)
ORDER BY WasCalledBy ASC;

--Listing 20-12
SELECT op1.OperatorName,
       STRING_AGG(op2.OperatorName, '->')WITHIN GROUP(GRAPH PATH) AS Friends,
       LAST_VALUE(op2.OperatorName)WITHIN GROUP(GRAPH PATH) AS LastNode,
       COUNT(op2.OperatorName)WITHIN GROUP(GRAPH PATH) AS levels
FROM grph.RadioOperator AS op1,
     grph.Calls FOR PATH AS C,
     grph.RadioOperator FOR PATH AS op2
WHERE MATCH(SHORTEST_PATH(op1(-(C)->op2)+));


--Listing 20-13
SELECT op1.OperatorName,
       STRING_AGG(op2.OperatorName, '->')WITHIN GROUP(GRAPH PATH) AS Friends,
       LAST_VALUE(op2.OperatorName)WITHIN GROUP(GRAPH PATH) AS LastNode,
       COUNT(op2.OperatorName)WITHIN GROUP(GRAPH PATH) AS levels
FROM grph.RadioOperator AS op1,
     grph.Calls FOR PATH AS C,
     grph.RadioOperator FOR PATH AS op2,
	 grph.Uses AS u,
	 grph.Radio AS r
WHERE MATCH(SHORTEST_PATH(op1(-(C)->op2)+) AND LAST_NODE(op2)-(u)->r)
AND r.RadioName = 'Xiegu G90';


--Listing 20-14
SELECT Calling.OperatorName,
       Calling.CallSign,
       Called.OperatorName,
       Called.CallSign
FROM grph.RadioOperator AS Calling,
     grph.Calls AS C,
     grph.RadioOperator AS Called
WHERE MATCH(Calling-(C)->Called);
GO 50

CREATE UNIQUE CLUSTERED INDEX CallsToFrom ON grph.Calls ($from_id, $to_id);

USE AdventureWorks;

--Listing 21-1
CREATE OR ALTER FUNCTION dbo.SalesInfo
()
RETURNS @return_variable TABLE
(
    SalesOrderID INT,
    OrderDate DATETIME,
    SalesPersonID INT,
    PurchaseOrderNumber dbo.OrderNumber,
    AccountNumber dbo.AccountNumber,
    ShippingCity NVARCHAR(30)
)
AS
BEGIN;
    INSERT INTO @return_variable
    (
        SalesOrderID,
        OrderDate,
        SalesPersonID,
        PurchaseOrderNumber,
        AccountNumber,
        ShippingCity
    )
    SELECT soh.SalesOrderID,
           soh.OrderDate,
           soh.SalesPersonID,
           soh.PurchaseOrderNumber,
           soh.AccountNumber,
           A.City
    FROM Sales.SalesOrderHeader AS soh
        JOIN Person.ADDRESS AS A
            ON soh.ShipToAddressID = A.AddressID;
    RETURN;
END;
GO
CREATE OR ALTER FUNCTION dbo.SalesDetails
()
RETURNS @return_variable TABLE
(
    SalesOrderID INT,
    SalesOrderDetailID INT,
    OrderQty SMALLINT,
    UnitPrice MONEY
)
AS
BEGIN;
    INSERT INTO @return_variable
    (
        SalesOrderID,
        SalesOrderDetailID,
        OrderQty,
        UnitPrice
    )
    SELECT sod.SalesOrderID,
           sod.SalesOrderDetailID,
           sod.OrderQty,
           sod.UnitPrice
    FROM Sales.SalesOrderDetail AS sod;
    RETURN;
END;
GO
CREATE OR ALTER FUNCTION dbo.CombinedSalesInfo
()
RETURNS @return_variable TABLE
(
    SalesPersonID INT,
    ShippingCity NVARCHAR(30),
    OrderDate DATETIME,
    PurchaseOrderNumber dbo.OrderNumber,
    AccountNumber dbo.AccountNumber,
    OrderQty SMALLINT,
    UnitPrice MONEY
)
AS
BEGIN;
    INSERT INTO @return_variable
    (
        SalesPersonID,
        ShippingCity,
        OrderDate,
        PurchaseOrderNumber,
        AccountNumber,
        OrderQty,
        UnitPrice
    )
    SELECT si.SalesPersonID,
           si.ShippingCity,
           si.OrderDate,
           si.PurchaseOrderNumber,
           si.AccountNumber,
           sd.OrderQty,
           sd.UnitPrice
    FROM dbo.SalesInfo() AS si
        JOIN dbo.SalesDetails() AS sd
            ON si.SalesOrderID = sd.SalesOrderID;
    RETURN;
END;
GO


--Listing 21-2
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;
ALTER DATABASE SCOPED CONFIGURATION SET INTERLEAVED_EXECUTION_TVF = OFF;
GO
--noninterleaved
SELECT csi.OrderDate,
       csi.PurchaseOrderNumber,
       csi.AccountNumber,
       csi.OrderQty,
       csi.UnitPrice,
       sp.SalesQuota
FROM dbo.CombinedSalesInfo() AS csi
    JOIN Sales.SalesPerson AS sp
        ON csi.SalesPersonID = sp.BusinessEntityID
WHERE csi.SalesPersonID = 277
      AND csi.ShippingCity = 'Odessa';
GO --50
ALTER DATABASE SCOPED CONFIGURATION SET INTERLEAVED_EXECUTION_TVF = ON;
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;
GO
--Interleaved
SELECT csi.OrderDate,
       csi.PurchaseOrderNumber,
       csi.AccountNumber,
       csi.OrderQty,
       csi.UnitPrice,
       sp.SalesQuota
FROM dbo.CombinedSalesInfo() AS csi
    JOIN Sales.SalesPerson AS sp
        ON csi.SalesPersonID = sp.BusinessEntityID
WHERE csi.SalesPersonID = 277
      AND csi.ShippingCity = 'Odessa';
GO --50


--Listing 21-3
CREATE OR ALTER FUNCTION dbo.AllSalesInfo
(
    @SalesPersonID INT,
    @ShippingCity VARCHAR(50)
)
RETURNS @return_variable TABLE
(
    SalesPersonID INT,
    ShippingCity NVARCHAR(30),
    OrderDate DATETIME,
    PurchaseOrderNumber dbo.OrderNumber,
    AccountNumber dbo.AccountNumber,
    OrderQty SMALLINT,
    UnitPrice MONEY
)
AS
BEGIN;
    INSERT INTO @return_variable
    (
        SalesPersonID,
        ShippingCity,
        OrderDate,
        PurchaseOrderNumber,
        AccountNumber,
        OrderQty,
        UnitPrice
    )
    SELECT soh.SalesPersonID,
           A.City,
           soh.OrderDate,
           soh.PurchaseOrderNumber,
           soh.AccountNumber,
           sod.OrderQty,
           sod.UnitPrice
    FROM Sales.SalesOrderHeader AS soh
        JOIN Person.ADDRESS AS A
            ON A.AddressID = soh.ShipToAddressID
        JOIN Sales.SalesOrderDetail AS sod
            ON sod.SalesOrderID = soh.SalesOrderID
    WHERE soh.SalesPersonID = @SalesPersonID
          AND A.City = @ShippingCity;
    RETURN;
END;
GO


--Listing 21-4
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;
ALTER DATABASE SCOPED CONFIGURATION SET INTERLEAVED_EXECUTION_TVF = OFF;
GO
--noninterleaved
SELECT asi.OrderDate,
       asi.PurchaseOrderNumber,
       asi.AccountNumber,
       asi.OrderQty,
       asi.UnitPrice,
       sp.SalesQuota
FROM dbo.AllSalesInfo(277, 'Odessa') AS asi
    JOIN Sales.SalesPerson AS sp
        ON asi.SalesPersonID = sp.BusinessEntityID;
GO 50
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;
ALTER DATABASE SCOPED CONFIGURATION SET INTERLEAVED_EXECUTION_TVF = ON;
GO
--interleaved
SELECT asi.OrderDate,
       asi.PurchaseOrderNumber,
       asi.AccountNumber,
       asi.OrderQty,
       asi.UnitPrice,
       sp.SalesQuota
FROM dbo.AllSalesInfo(277, 'Odessa') AS asi
    JOIN Sales.SalesPerson AS sp
        ON asi.SalesPersonID = sp.BusinessEntityID;
GO 50

--Listing 21-5
CREATE EVENT SESSION MemoryGrant
ON SERVER
    ADD EVENT sqlserver.memory_grant_feedback_loop_disabled
    (WHERE (sqlserver.database_name = N'AdventureWorks')),
    ADD EVENT sqlserver.memory_grant_updated_by_feedback
    (WHERE (sqlserver.database_name = N'AdventureWorks')),
    ADD EVENT sqlserver.sql_batch_completed
    (WHERE (sqlserver.database_name = N'AdventureWorks'))
WITH
(
    TRACK_CAUSALITY = ON
);


--Listing 21-6
CREATE OR ALTER PROCEDURE dbo.CostCheck
(@Cost MONEY)
AS
SELECT p.NAME,
       AVG(th.Quantity),
       AVG(th.ActualCost)
FROM dbo.bigTransactionHistory AS th
    JOIN dbo.bigProduct AS p
        ON p.ProductID = th.ProductID
WHERE th.ActualCost = @Cost
GROUP BY p.NAME;
GO

ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;


EXEC dbo.CostCheck @cost = 0;


EXEC dbo.CostCheck @cost = 117.702;


--Listing 21-7
ALTER DATABASE SCOPED CONFIGURATION SET ROW_MODE_MEMORY_GRANT_FEEDBACK = OFF; --or ON


--Listing 21-8
CREATE EVENT SESSION [CardinalityFeedback]
ON SERVER
    ADD EVENT sqlserver.query_ce_feedback_telemetry(),
    ADD EVENT sqlserver.query_feedback_analysis(),
    ADD EVENT sqlserver.query_feedback_validation(),
    ADD EVENT sqlserver.sql_batch_completed();




--Listing 21-9
--DBCC TRACEON (2451, -1);
SELECT AddressID,
       AddressLine1,
       AddressLine2
FROM Person.ADDRESS
WHERE StateProvinceID = 79
      AND City = N'Redmond';
GO 50


--Listing 21-10
SELECT qsqt.query_sql_text,
       qspf.feature_id,
       qspf.feature_desc,
       qspf.feedback_data,
       qspf.STATE,
       qspf.state_desc
FROM sys.query_store_plan_feedback AS qspf
    JOIN sys.query_store_plan AS qsp
        ON qsp.plan_id = qspf.plan_id
    JOIN sys.query_store_query AS qsq
        ON qsq.query_id = qsp.query_id
    JOIN sys.query_store_query_text AS qsqt
        ON qsqt.query_text_id = qsq.query_text_id;


--Listing 21-11
SELECT TOP (1500)
       bp.Name,
       bp.ProductNumber,
       bth.Quantity,
       bth.ActualCost
FROM dbo.bigProduct AS bp
    JOIN dbo.bigTransactionHistory AS bth
        ON bth.ProductID = bp.ProductID
WHERE bth.Quantity = 10
      AND bth.ActualCost > 357
ORDER BY bp.Name;
GO 17

--Listing 21-12
--intentionally using SELECT *
SELECT *
FROM dbo.bigTransactionHistory AS bth
    JOIN dbo.bigProduct AS bp
        ON bp.ProductID = bth.ProductID
WHERE bth.Quantity = 10
      AND bth.ActualCost > 357;

--Listing 21-13
ALTER DATABASE SCOPED CONFIGURATION SET DOP_FEEDBACK = ON;

--Listing 21-14
CREATE EVENT SESSION [DOPFeedback]
ON SERVER
    ADD EVENT sqlserver.dop_feedback_eligible_query(),
    ADD EVENT sqlserver.dop_feedback_provided(),
    ADD EVENT sqlserver.dop_feedback_reverted(),
    ADD EVENT sqlserver.dop_feedback_validation(),
    ADD EVENT sqlserver.sql_batch_completed();
GO


--Listing 21-15
SELECT COUNT(DISTINCT bth.TransactionID)
FROM dbo.bigTransactionHistory AS bth
GROUP BY bth.TransactionDate,
         bth.ActualCost;
GO
SELECT APPROX_COUNT_DISTINCT(bth.TransactionID)
FROM dbo.bigTransactionHistory AS bth
GROUP BY bth.TransactionDate,
         bth.ActualCost;
GO


--Listing 21-16
SELECT DISTINCT
       bp.NAME,
       PERCENTILE_CONT(0.5)WITHIN GROUP(ORDER BY bth.ActualCost) OVER (PARTITION BY bp.NAME) AS MedianCont,
       PERCENTILE_DISC(0.5)WITHIN GROUP(ORDER BY bth.ActualCost) OVER (PARTITION BY bp.NAME) AS MedianDisc
FROM dbo.bigTransactionHistory AS bth
    JOIN dbo.bigProduct AS bp
        ON bp.ProductID = bth.ProductID
WHERE bth.Quantity > 75
ORDER BY bp.Name;
GO
SELECT bp.NAME,
       APPROX_PERCENTILE_CONT(0.5)WITHIN GROUP(ORDER BY bth.ActualCost) AS MedianCont,
       APPROX_PERCENTILE_DISC(0.5)WITHIN GROUP(ORDER BY bth.ActualCost) AS MedianDisc
FROM dbo.bigTransactionHistory AS bth
    JOIN dbo.bigProduct AS bp
        ON bp.ProductID = bth.ProductID
WHERE bth.Quantity > 75
GROUP BY bp.NAME
ORDER BY bp.Name;
GO


Listing 21-17
--Disable deferred compilation to see the old behavior
ALTER DATABASE SCOPED CONFIGURATION SET DEFERRED_COMPILATION_TV = OFF;
GO
DECLARE @HeaderInfo TABLE
(
    SalesOrderID INT,
    SalesOrderNumber NVARCHAR(25)
);

INSERT @HeaderInfo
(
    SalesOrderID,
    SalesOrderNumber
)
SELECT soh.SalesOrderID,
       soh.SalesOrderNumber
FROM Sales.SalesOrderHeader AS soh
WHERE soh.DueDate > '6/1/2014';

SELECT hi.SalesOrderNumber,
       sod.LineTotal
FROM @HeaderInfo AS hi
    JOIN Sales.SalesOrderDetail AS sod
        ON sod.SalesOrderID = hi.SalesOrderID;
GO
ALTER DATABASE SCOPED CONFIGURATION SET DEFERRED_COMPILATION_TV = ON;
GO
DECLARE @HeaderInfo TABLE
(
    SalesOrderID INT,
    SalesOrderNumber NVARCHAR(25)
);

INSERT @HeaderInfo
(
    SalesOrderID,
    SalesOrderNumber
)
SELECT soh.SalesOrderID,
       soh.SalesOrderNumber
FROM Sales.SalesOrderHeader AS soh
WHERE soh.DueDate > '6/1/2014';

SELECT hi.SalesOrderNumber,
       sod.LineTotal
FROM @HeaderInfo AS hi
    JOIN Sales.SalesOrderDetail AS sod
        ON sod.SalesOrderID = hi.SalesOrderID;


--Listing 21-18
CREATE OR ALTER FUNCTION dbo.ufnGetProductStandardCost
(
    @ProductID int,
    @OrderDate datetime
)
RETURNS money
AS
-- Returns the standard cost for the product on a specific date.
BEGIN
    DECLARE @StandardCost money;

    SELECT @StandardCost = pch.StandardCost
    FROM Production.Product p
        INNER JOIN Production.ProductCostHistory pch
            ON p.ProductID = pch.ProductID
               AND p.ProductID = @ProductID
               AND @OrderDate
               BETWEEN pch.StartDate AND COALESCE(pch.EndDate, CONVERT(datetime, '99991231', 112)); -- Make sure we get all the prices!

    RETURN @StandardCost;
END;


--Listing 21-19
SELECT sm.is_inlineable
FROM sys.sql_modules AS sm
    JOIN sys.objects AS o
        ON o.OBJECT_ID = sm.OBJECT_ID
WHERE o.NAME = 'ufnGetProductStandardCost';


--Listing 21-20
--Disable scalar inline
ALTER DATABASE SCOPED CONFIGURATION SET TSQL_SCALAR_UDF_INLINING = OFF;
GO
--not inline
SELECT sod.LineTotal,
       dbo.ufnGetProductStandardCost(sod.ProductID, soh.OrderDate)
FROM Sales.SalesOrderDetail AS sod
    JOIN Production.Product AS p
        ON p.ProductID = sod.ProductID
    JOIN Sales.SalesOrderHeader AS soh
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE sod.LineTotal > 1000;
GO
--Enable scalar inline
ALTER DATABASE SCOPED CONFIGURATION SET TSQL_SCALAR_UDF_INLINING = ON;
GO
--inline
SELECT sod.LineTotal,
       dbo.ufnGetProductStandardCost(sod.ProductID, soh.OrderDate)
FROM Sales.SalesOrderDetail AS sod
    JOIN Production.Product AS p
        ON p.ProductID = sod.ProductID
    JOIN Sales.SalesOrderHeader AS soh
        ON soh.SalesOrderID = sod.SalesOrderID
WHERE sod.LineTotal > 1000;
GO


--Listing 22-1
CREATE INDEX ix_ActualCost ON dbo.bigTransactionHistory (ActualCost);
GO
--a simple query for the experiment
CREATE OR ALTER PROCEDURE dbo.ProductByCost
(@ActualCost MONEY)
AS
SELECT bth.ActualCost
FROM dbo.bigTransactionHistory AS bth
    JOIN dbo.bigProduct AS p
        ON p.ProductID = bth.ProductID
WHERE bth.ActualCost = @ActualCost;
GO
--ensuring that Query Store is on and has a clean data set
ALTER DATABASE AdventureWorks SET QUERY_STORE = ON;
ALTER DATABASE AdventureWorks SET QUERY_STORE CLEAR;
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SENSITIVE_PLAN_OPTIMIZATION = OFF;

GO

ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE

--Listing 22-2
--establish a history of query performance
EXEC dbo.ProductByCost @ActualCost = 54.838;
GO 30
--remove the plan from cache
DECLARE @PlanHandle VARBINARY(64);
SELECT  @PlanHandle = deps.plan_handle
FROM    sys.dm_exec_procedure_stats AS deps
WHERE   deps.object_id = OBJECT_ID('dbo.ProductByCost');
IF @PlanHandle IS NOT NULL
    BEGIN
        DBCC FREEPROCCACHE(@PlanHandle);
    END
GO
--execute a query that will result in a different plan
EXEC dbo.ProductByCost @ActualCost = 0.0;
GO
--establish a new history of poor performance
EXEC dbo.ProductByCost @ActualCost = 54.838;
GO 15


--Listing 22-3
SELECT ddtr.TYPE,
       ddtr.reason,
       ddtr.STATE,
       ddtr.score,
       ddtr.details
FROM sys.dm_db_tuning_recommendations AS ddtr;


--Listing 22-4
WITH DbTuneRec
AS (SELECT ddtr.reason,
           ddtr.score,
           pfd.query_id,
           pfd.regressedPlanId,
           pfd.recommendedPlanId,
           JSON_VALUE(ddtr.STATE, '$.currentValue') AS CurrentState,
           JSON_VALUE(ddtr.STATE, '$.reason') AS CurrentStateReason,
           JSON_VALUE(ddtr.details, '$.implementationDetails.script') AS ImplementationScript
    FROM sys.dm_db_tuning_recommendations AS ddtr
        CROSS APPLY
        OPENJSON(ddtr.details, '$.planForceDetails')
        WITH
        (
            query_id INT '$.queryId',
            regressedPlanId INT '$.regressedPlanId',
            recommendedPlanId INT '$.recommendedPlanId'
        ) AS pfd)
SELECT qsq.query_id,
       dtr.reason,
       dtr.score,
       dtr.CurrentState,
       dtr.CurrentStateReason,
       qsqt.query_sql_text,
       CAST(rp.query_plan AS XML) AS RegressedPlan,
       CAST(sp.query_plan AS XML) AS SuggestedPlan,
       dtr.ImplementationScript
FROM DbTuneRec AS dtr
    JOIN sys.query_store_plan AS rp
        ON rp.query_id = dtr.query_id
           AND rp.plan_id = dtr.regressedPlanId
    JOIN sys.query_store_plan AS sp
        ON sp.query_id = dtr.query_id
           AND sp.plan_id = dtr.recommendedPlanId
    JOIN sys.query_store_query AS qsq
        ON qsq.query_id = rp.query_id
    JOIN sys.query_store_query_text AS qsqt
        ON qsqt.query_text_id = qsq.query_text_id;


--Listing 22-5
ALTER DATABASE CURRENT SET AUTOMATIC_TUNING(FORCE_LAST_GOOD_PLAN = ON);


--Listing 22-6
SELECT NAME,
       desired_state,
       desired_state_desc,
       actual_state,
       actual_state_desc,
       reason,
       reason_desc
FROM sys.database_automatic_tuning_options;


--Listing 22-7
CREATE OR ALTER PROCEDURE dbo.CustomerInfo
(@Firstname NVARCHAR(50))
AS
SELECT C.FirstName,
       C.LastName,
       C.Title,
       A.City
FROM SalesLT.Customer AS C
    JOIN SalesLT.CustomerAddress AS ca
        ON ca.CustomerID = C.CustomerID
    JOIN SalesLT.ADDRESS AS A
        ON A.AddressID = ca.AddressID
WHERE C.FirstName = @Firstname;
GO
CREATE OR ALTER PROCEDURE dbo.EmailInfo
(@EmailAddress nvarchar(50))
AS
SELECT C.EmailAddress,
       C.Title,
       soh.OrderDate
FROM SalesLT.Customer AS C
    JOIN SalesLT.SalesOrderHeader AS soh
        ON soh.CustomerID = C.CustomerID
WHERE C.EmailAddress = @EmailAddress;
GO
CREATE OR ALTER PROCEDURE dbo.SalesInfo
(@firstName NVARCHAR(50))
AS
SELECT C.FirstName,
       C.LastName,
       C.Title,
       soh.OrderDate
FROM SalesLT.Customer AS C
    JOIN SalesLT.SalesOrderHeader AS soh
        ON soh.CustomerID = C.CustomerID
WHERE C.FirstName = @firstName;
GO
CREATE OR ALTER PROCEDURE dbo.OddName
(@FirstName NVARCHAR(50))
AS
SELECT C.FirstName
FROM SalesLT.Customer AS C
WHERE C.FirstName
BETWEEN 'Brian' AND @FirstName;
GO


--Listing 23-1
CREATE NONCLUSTERED INDEX AK_Product_Name
ON Production.Product (NAME ASC)
WITH (DROP_EXISTING = ON);

--Listing 23-2
SELECT DISTINCT
       (p.NAME)
FROM Production.Product AS p;


--Listing 23-3
CREATE UNIQUE NONCLUSTERED INDEX AK_Product_Name
ON Production.Product (NAME ASC)
WITH (DROP_EXISTING = ON);


--Listing 23-4
DROP TABLE IF EXISTS dbo.Test1;
GO
CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 INT CHECK (C2
                  BETWEEN 10 AND 20
                 )
);
INSERT INTO dbo.Test1
VALUES
(11, 12);
GO
DROP TABLE IF EXISTS dbo.Test2;
GO
CREATE TABLE dbo.Test2
(
    C1 INT,
    C2 INT
);
INSERT INTO dbo.Test2
VALUES
(101, 102);


--Listing 23-5
SELECT T1.C1,
       T1.C2,
       T2.C2
FROM dbo.Test1 AS T1
    JOIN dbo.Test2 AS T2
        ON T1.C1 = T2.C2
           AND T1.C2 = 20;
GO
SELECT T1.C1,
       T1.C2,
       T2.C2
FROM dbo.Test1 AS T1
    JOIN dbo.Test2 AS T2
        ON T1.C1 = T2.C2
           AND T1.C2 = 30;


--Listing 23-6
WITH XMLNAMESPACES
(
    DEFAULT N'http://schemas.microsoft.com/sqlserver/2004/07/showplan'
)
, QueryStore
AS (SELECT CAST(qsp.query_plan AS XML) AS QueryPlan
    FROM sys.query_store_plan AS qsp),
  QueryPlans
AS (SELECT RelOp.pln.value(N'@EstimatedTotalSubtreeCost', N'float') AS EstimatedCost,
           RelOp.pln.value(N'@NodeId', N'integer') AS NodeId,
           qs.QueryPlan
    FROM QueryStore AS qs
        CROSS APPLY qs.queryplan.nodes(N'//RelOp') RelOp(pln) )
SELECT qp.EstimatedCost
FROM QueryPlans AS qp
WHERE qp.NodeId = 0;


--Listing 23-7
SELECT p.ProductID,
       p.Name,
       p.ProductNumber,
       p.SafetyStockLevel,
       p.ReorderPoint,
       p.StandardCost,
       p.ListPrice,
       p.Size,
       p.DaysToManufacture,
       p.ProductLine
FROM Production.Product AS p
WHERE p.NAME LIKE '%Caps';



--Listing 23-8
SELECT soh.SalesOrderNumber
FROM Sales.SalesOrderHeader AS soh
WHERE 'SO5' = LEFT(SalesOrderNumber, 3);
SELECT soh.SalesOrderNumber
FROM Sales.SalesOrderHeader AS soh
WHERE SalesOrderNumber LIKE 'SO5%';











