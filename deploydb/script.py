DATABASES = """
    SELECT name AS DB_NAME
    FROM sys.databases
    WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
    ORDER BY 1
"""

CREATE_TABLE = """
    DECLARE
        @schema_name    NVARCHAR(200) = ?
    ,	@table_name     NVARCHAR(300) = ?

    DECLARE
        @object_name    SYSNAME
    ,   @object_id      INT

    SELECT
        @object_name    = '[' + s.name + '].[' + o.name + ']'
    ,   @object_id      = o.[object_id]
    FROM sys.objects o WITH (NOWAIT)
        JOIN sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id]
    WHERE s.name = @schema_name
    AND o.name = @table_name
    AND o.[type] = 'U'
    AND o.is_ms_shipped = 0

    DECLARE @SQL NVARCHAR(MAX) = ''

    ;WITH index_column AS
    (
        SELECT
            ic.[object_id]
            , ic.index_id
            , ic.is_descending_key
            , ic.is_included_column
            , c.name
        FROM sys.index_columns ic WITH (NOWAIT)
        JOIN sys.columns c WITH (NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
        WHERE ic.[object_id] = @object_id
    ),
    fk_columns AS
    (
        SELECT
            k.constraint_object_id
            , cname = c.name
            , rcname = rc.name
        FROM sys.foreign_key_columns k WITH (NOWAIT)
        JOIN sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id
        JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
        WHERE k.parent_object_id = @object_id
    )
    SELECT @SQL = 'CREATE TABLE ' + @object_name + CHAR(13) + '(' + CHAR(13) + STUFF((
        SELECT CHAR(9) + ', [' + c.name + '] ' +
            CASE WHEN c.is_computed = 1
                THEN 'AS ' + cc.[definition]
                ELSE (tp.name) +
                    CASE WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary', 'text')
                        THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
                        --WHEN tp.name IN ('nvarchar', 'nchar', 'ntext')
                        WHEN tp.name IN ('nvarchar', 'nchar')
                        THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
                        WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')
                        THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
                        WHEN tp.name = 'decimal'
                        THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'
                        ELSE ''
                    END +
                    --CASE WHEN c.collation_name IS NOT NULL THEN ' COLLATE ' + c.collation_name ELSE '' END +
                    CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END +
                    CASE WHEN dc.[definition] IS NOT NULL THEN ' DEFAULT' + dc.[definition] ELSE '' END +
                    CASE WHEN ic.is_identity = 1 THEN ' IDENTITY(' + CAST(ISNULL(ic.seed_value, '0') AS CHAR(1)) + ',' + CAST(ISNULL(ic.increment_value, '1') AS CHAR(1)) + ')' ELSE '' END
            END + CHAR(13)
        FROM sys.columns c WITH (NOWAIT)
        JOIN sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id
        LEFT JOIN sys.computed_columns cc WITH (NOWAIT) ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
        LEFT JOIN sys.default_constraints dc WITH (NOWAIT) ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
        LEFT JOIN sys.identity_columns ic WITH (NOWAIT) ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
        WHERE c.[object_id] = @object_id
        ORDER BY c.column_id
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, CHAR(9) + ' ')
        + ISNULL((SELECT CHAR(9) + ', CONSTRAINT [' + k.name + '] PRIMARY KEY (' +
                        (SELECT STUFF((
                            SELECT ', [' + c.name + '] ' + CASE WHEN ic.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END
                            FROM sys.index_columns ic WITH (NOWAIT)
                            JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
                            WHERE ic.is_included_column = 0
                                AND ic.[object_id] = k.parent_object_id
                                AND ic.index_id = k.unique_index_id
                            FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''))
                + ')' + CHAR(13)
                FROM sys.key_constraints k WITH (NOWAIT)
                WHERE k.parent_object_id = @object_id
                    AND k.[type] = 'PK'), '') + ')'  + CHAR(13)
        + ISNULL((SELECT (
            SELECT CHAR(13) +
                'ALTER TABLE ' + @object_name + ' WITH'
                + CASE WHEN fk.is_not_trusted = 1
                    THEN ' NOCHECK'
                    ELSE ' CHECK'
                END +
                ' ADD CONSTRAINT [' + fk.name  + '] FOREIGN KEY('
                + STUFF((
                    SELECT ', [' + k.cname + ']'
                    FROM fk_columns k
                    WHERE k.constraint_object_id = fk.[object_id]
                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
                + ')' +
                ' REFERENCES [' + SCHEMA_NAME(ro.[schema_id]) + '].[' + ro.name + '] ('
                + STUFF((
                    SELECT ', [' + k.rcname + ']'
                    FROM fk_columns k
                    WHERE k.constraint_object_id = fk.[object_id]
                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
                + ')'
                + CASE
                    WHEN fk.delete_referential_action = 1 THEN ' ON DELETE CASCADE'
                    WHEN fk.delete_referential_action = 2 THEN ' ON DELETE SET NULL'
                    WHEN fk.delete_referential_action = 3 THEN ' ON DELETE SET DEFAULT'
                    ELSE ''
                END
                + CASE
                    WHEN fk.update_referential_action = 1 THEN ' ON UPDATE CASCADE'
                    WHEN fk.update_referential_action = 2 THEN ' ON UPDATE SET NULL'
                    WHEN fk.update_referential_action = 3 THEN ' ON UPDATE SET DEFAULT'
                    ELSE ''
                END
                + CHAR(13) + 'ALTER TABLE ' + @object_name + ' CHECK CONSTRAINT [' + fk.name  + ']' + CHAR(13)
            FROM sys.foreign_keys fk WITH (NOWAIT)
            JOIN sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id
            WHERE fk.parent_object_id = @object_id
            FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')), '')
        + ISNULL(((SELECT
            CHAR(13) + 'CREATE' + CASE WHEN i.is_unique = 1 THEN ' UNIQUE' ELSE '' END
                    + ' NONCLUSTERED INDEX [' + i.name + '] ON ' + @object_name + ' (' +
                    STUFF((
                    SELECT ', [' + c.name + ']' + CASE WHEN c.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                    FROM index_column c
                    WHERE c.is_included_column = 0
                        AND c.index_id = i.index_id
                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')'
                    + ISNULL(CHAR(13) + 'INCLUDE (' +
                        STUFF((
                        SELECT ', [' + c.name + ']'
                        FROM index_column c
                        WHERE c.is_included_column = 1
                            AND c.index_id = i.index_id
                        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')', '')  + CHAR(13)
            FROM sys.indexes i WITH (NOWAIT)
            WHERE i.[object_id] = @object_id
                AND i.is_primary_key = 0
                AND i.[type] = 2
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
        ), '')

    SELECT @SQL AS SQL
"""  # noqa

OBJECTS = """
	SELECT
        SUB_FOLDER		= CASE all_objects.type
                            WHEN 'U' THEN 'Tables'			-- SQL_SCALAR_FUNCTION
                            WHEN 'FN' THEN 'Functions'			-- SQL_SCALAR_FUNCTION
                            WHEN 'V ' THEN 'Views'				-- VIEW
                            WHEN 'IF' THEN 'Functions'			-- SQL_INLINE_TABLE_VALUED_FUNCTION
                            WHEN 'TF' THEN 'Functions'			-- SQL_TABLE_VALUED_FUNCTION
                            WHEN 'P ' THEN 'Stored-Procedures'	-- SQL_STORED_PROCEDURE
                            WHEN 'TR' THEN 'Triggers'			-- SQL_TRIGGER
                        END
    ,	OBJECT_ID	= all_objects.object_id
    ,	SCHEMA_NAME	= schemas.name
    ,	OBJECT_NAME	= all_objects.name
    ,	SQL		    = all_sql_modules.definition

    FROM sys.all_objects
		JOIN sys.schemas
			ON schemas.schema_id = all_objects.schema_id 
		LEFT JOIN sys.all_sql_modules
			ON all_sql_modules.object_id = all_objects.object_id
    WHERE all_objects.object_id > 0
	AND all_objects.type IN ('U', 'FN', 'V', 'IF', 'TF', 'P', 'TR')
    ORDER BY
        CASE all_objects.type
            WHEN 'U' THEN 0	-- SQL_SCALAR_FUNCTION
            WHEN 'FN' THEN 1	-- SQL_SCALAR_FUNCTION
            WHEN 'V ' THEN 2	-- VIEW
            WHEN 'IF' THEN 3	-- SQL_INLINE_TABLE_VALUED_FUNCTION
            WHEN 'TF' THEN 4	-- SQL_TABLE_VALUED_FUNCTION
            WHEN 'P ' THEN 5	-- SQL_STORED_PROCEDURE
            WHEN 'TR' THEN 6	-- SQL_TRIGGER
        END
    ,   schemas.name
    --,	all_objects.object_id
"""  # noqa

GET_OBJECT = """
    SELECT *
    FROM sys.all_objects
    WHERE
        CASE ?
            WHEN 'Tables'			    THEN 'U'
            WHEN 'Functions'			THEN 'FN'
            WHEN 'Views'				THEN 'V '
            WHEN 'Functions'			THEN 'IF'
            WHEN 'Functions'			THEN 'TF'
            WHEN 'Stored-Procedures'	THEN 'P '
            WHEN 'Triggers'			    THEN 'TR'
        END = all_objects.type
    AND all_objects.object_id = OBJECT_ID(?)
"""

INIT_DEPLOYDB = """
    IF NOT EXISTS (SELECT NULL FROM sys.schemas WHERE name = 'Deploydb')
        EXEC('CREATE SCHEMA Deploydb');

    IF OBJECT_ID('Deploydb.ExecutionLog', 'U') IS NULL
        CREATE TABLE Deploydb.ExecutionLog (
            RowId INT IDENTITY,
            CreatedAt DATETIME CONSTRAINT DF_Deploydb_ExecutionLog_CreatedAt DEFAULT(GETDATE()),
            CommitHexSHA VARCHAR(64),
            Folder NVARCHAR(1000),
            IsFailed BIT CONSTRAINT DF_Deploydb_ExecutionLog_IsFailed DEFAULT(0),
            Error NVARCHAR(2000),
            INDEX IX_Deploydb_ExecutionLog_CommitHexSHA_Folder (CommitHexSHA, Folder)
        );
"""

EXECUTION_LOG_INSERT = """
    INSERT INTO Deploydb.ExecutionLog (CommitHexSHA, Folder, IsFailed, Error)
    VALUES (?,?,?,?);
"""

DUPLICATE_CONTROL = """
    SELECT 1 FROM Deploydb.ExecutionLog WHERE CommitHexSHA = ? AND Folder = ?
"""
