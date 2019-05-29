

SELECT
    dt1.[Schema]
    ,dt1.TableName
    ,dt1.IndexType
    ,dt2.PartitionCount
    ,CASE
        WHEN dt1.Rows=0 THEN 'None'
        WHEN dt1.Rows BETWEEN 1 AND 999 THEN 'Under one thousand'
        WHEN dt1.Rows BETWEEN 1000 AND 999999 THEN 'Thousands'
        WHEN dt1.Rows BETWEEN 1000000 AND 9999999 THEN 'Millions'
        WHEN dt1.Rows BETWEEN 10000000 AND 99999999 THEN 'Tens of millions'
        WHEN dt1.Rows BETWEEN 100000000 AND 999999999 THEN 'Hundreds of millions'
        WHEN dt1.Rows>=1000000000 THEN 'Billions'
    END AS 'RowCountMagnitude'
    ,dt1.Rows
    ,dt3.Columns
FROM
    (
    SELECT  --get row counts
        t.object_id
        ,s.name AS 'Schema'
        ,t.name AS 'TableName'
        ,i.type_desc AS 'IndexType'
        ,SUM(p.rows) AS 'Rows'
    FROM sys.tables t
    JOIN sys.schemas s ON (t.schema_id=s.schema_id)
    JOIN sys.indexes i ON (t.object_id=i.object_id)
    JOIN sys.partitions p ON ((i.object_id=p.object_id)AND(i.index_id=p.index_id))
    WHERE i.type IN (0,1)   --0=heap, 1=clustered index
    GROUP BY t.object_id, s.name, t.schema_id, t.name, i.type_desc
    )dt1
JOIN
    (
    SELECT  --get partition count
        t.object_id
        ,COUNT(p.partition_id) AS 'PartitionCount'
    FROM sys.tables t
    JOIN sys.indexes i ON (t.object_id=i.object_id)
    JOIN sys.partitions p ON ((i.object_id=p.object_id)AND(i.index_id=p.index_id))
    WHERE i.type IN (0,1)   --0=heap, 1=clustered index
    GROUP BY t.object_id
    )dt2 ON (dt1.object_id=dt2.object_id)
JOIN
    (
    SELECT --get column count
        c.object_id
        ,COUNT(c.name) AS 'Columns'
    FROM sys.columns c
    GROUP BY c.object_id
    )dt3 ON (dt1.object_id=dt3.object_id)
ORDER BY dt1.Rows DESC;
