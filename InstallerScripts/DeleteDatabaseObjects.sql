/*
   Copyright 2016 Aaron Morelli

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

	------------------------------------------------------------------------

	PROJECT NAME: ChiRho https://github.com/AaronMorelli/ChiRho

	PROJECT DESCRIPTION: A T-SQL toolkit for troubleshooting performance and stability problems on SQL Server instances

	FILE NAME: DeleteDatabaseObjects.sql

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Deletes all ChiRho objects from the database specified by
	the uninstaller Powershell script.
*/
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#CleanUpChiRhoObjects') IS NOT NULL
BEGIN
	DROP TABLE #CleanUpChiRhoObjects
END
GO
CREATE TABLE #CleanUpChiRhoObjects (
	ObjectName NVARCHAR(256)
);

IF OBJECT_ID('tempdb..#FailedChiRhoObjects') IS NOT NULL
BEGIN
	DROP TABLE #FailedChiRhoObjects
END
GO
CREATE TABLE #FailedChiRhoObjects (
	ObjectName NVARCHAR(256),
	NumFailures INT,
	LastFailureMessage NVARCHAR(MAX)
);

DECLARE @curObjectName NVARCHAR(256),
		@DynSQL NVARCHAR(512),
		@FailureMessage NVARCHAR(MAX);

--procedures
WHILE EXISTS (SELECT * FROM sys.procedures p
				WHERE p.schema_id IN ( 
						SCHEMA_ID('CoreXR'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('XR'), SCHEMA_ID('HEM')
						) 
			)
	AND NOT EXISTS (SELECT * FROM #FailedChiRhoObjects WHERE NumFailures > 5)
BEGIN
	TRUNCATE TABLE #CleanUpChiRhoObjects;

	INSERT INTO #CleanUpChiRhoObjects (
		ObjectName
	)
	SELECT 
		QUOTENAME(SCHEMA_NAME(p.schema_id)) + N'.' + QUOTENAME(p.name)
	FROM sys.procedures p
	WHERE p.schema_id IN ( 
						SCHEMA_ID('CoreXR'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('XR'), SCHEMA_ID('HEM')
						) 
	;

	DECLARE IterateChiRhoObjects CURSOR FOR 
	SELECT o.ObjectName
	FROM #CleanUpChiRhoObjects o
	ORDER BY o.ObjectName;

	OPEN IterateChiRhoObjects;
	FETCH IterateChiRhoObjects INTO @curObjectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			SET @DynSQL = N'DROP PROCEDURE ' + @curObjectName + N';'
			PRINT @DynSQL;
			EXEC (@DynSQL);
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @FailureMessage = N'Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
				N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
				N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
				N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

			IF EXISTS (SELECT * FROM #FailedChiRhoObjects o
						WHERE o.ObjectName = @curObjectName)
			BEGIN
				UPDATE #FailedChiRhoObjects
				SET NumFailures = NumFailures + 1,
					LastFailureMessage = @FailureMessage
				WHERE ObjectName = @curObjectName;
			END
			ELSE
			BEGIN
				INSERT INTO #FailedChiRhoObjects (ObjectName, NumFailures, LastFailureMessage)
				SELECT @curObjectName, 0, @FailureMessage;
			END
		END CATCH

		FETCH IterateChiRhoObjects INTO @curObjectName;
	END

	CLOSE IterateChiRhoObjects;
	DEALLOCATE IterateChiRhoObjects;
END

IF EXISTS (SELECT * FROM #FailedChiRhoObjects WHERE NumFailures > 5)
BEGIN
	SELECT 'Procedures' as ObjType,* 
	FROM #FailedChiRhoObjects
	ORDER BY ObjectName;

	RAISERROR('> 5 failures (for one object) encountered when dropping procedures',16,1);
	GOTO ScriptFailure
END

TRUNCATE TABLE #FailedChiRhoObjects;


--functions
WHILE EXISTS (SELECT * FROM sys.objects f
				WHERE f.type in (N'FN', N'IF', N'TF') 
				AND f.schema_id IN ( 
						SCHEMA_ID('CoreXR'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('XR'), SCHEMA_ID('HEM')
						) 
			)
	AND NOT EXISTS (SELECT * FROM #FailedChiRhoObjects WHERE NumFailures > 5)
BEGIN
	TRUNCATE TABLE #CleanUpChiRhoObjects;

	INSERT INTO #CleanUpChiRhoObjects (
		ObjectName
	)
	SELECT 
		QUOTENAME(SCHEMA_NAME(p.schema_id)) + N'.' + QUOTENAME(p.name)
	FROM sys.procedures p
	WHERE p.schema_id IN ( 
						SCHEMA_ID('CoreXR'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('XR'), SCHEMA_ID('HEM')
						) 
	;

	DECLARE IterateChiRhoObjects CURSOR FOR 
	SELECT o.ObjectName
	FROM #CleanUpChiRhoObjects o
	ORDER BY o.ObjectName;

	OPEN IterateChiRhoObjects;
	FETCH IterateChiRhoObjects INTO @curObjectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			SET @DynSQL = N'DROP FUNCTION ' + @curObjectName + N';'
			PRINT @DynSQL;
			EXEC (@DynSQL);
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @FailureMessage = N'Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
				N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
				N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
				N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

			IF EXISTS (SELECT * FROM #FailedChiRhoObjects o
						WHERE o.ObjectName = @curObjectName)
			BEGIN
				UPDATE #FailedChiRhoObjects
				SET NumFailures = NumFailures + 1,
					LastFailureMessage = @FailureMessage
				WHERE ObjectName = @curObjectName;
			END
			ELSE
			BEGIN
				INSERT INTO #FailedChiRhoObjects (ObjectName, NumFailures, LastFailureMessage)
				SELECT @curObjectName, 0, @FailureMessage;
			END
		END CATCH

		FETCH IterateChiRhoObjects INTO @curObjectName;
	END

	CLOSE IterateChiRhoObjects;
	DEALLOCATE IterateChiRhoObjects;
END

IF EXISTS (SELECT * FROM #FailedChiRhoObjects WHERE NumFailures > 5)
BEGIN
	SELECT 'Functions' as ObjType,* 
	FROM #FailedChiRhoObjects
	ORDER BY ObjectName;

	RAISERROR('> 5 failures (for one object) encountered when dropping functions',16,1);
	GOTO ScriptFailure
END

TRUNCATE TABLE #FailedChiRhoObjects;

--views
WHILE EXISTS (SELECT * FROM sys.views v
				WHERE v.schema_id IN ( 
						SCHEMA_ID('CoreXR'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('XR'), SCHEMA_ID('HEM')
						) 
				)
	AND NOT EXISTS (SELECT * FROM #FailedChiRhoObjects WHERE NumFailures > 5)
BEGIN
	TRUNCATE TABLE #CleanUpChiRhoObjects;

	INSERT INTO #CleanUpChiRhoObjects (
		ObjectName
	)
	SELECT 
		QUOTENAME(SCHEMA_NAME(v.schema_id)) + N'.' + QUOTENAME(v.name)
	FROM sys.views v
	WHERE v.schema_id IN ( 
						SCHEMA_ID('CoreXR'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('XR'), SCHEMA_ID('HEM')
						)
	;

	DECLARE IterateChiRhoObjects CURSOR FOR 
	SELECT o.ObjectName
	FROM #CleanUpChiRhoObjects o
	ORDER BY o.ObjectName;

	OPEN IterateChiRhoObjects;
	FETCH IterateChiRhoObjects INTO @curObjectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			SET @DynSQL = N'DROP VIEW ' + @curObjectName + N';'
			PRINT @DynSQL;
			EXEC (@DynSQL);
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @FailureMessage = N'Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
				N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
				N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
				N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

			IF EXISTS (SELECT * FROM #FailedChiRhoObjects o
						WHERE o.ObjectName = @curObjectName)
			BEGIN
				UPDATE #FailedChiRhoObjects
				SET NumFailures = NumFailures + 1,
					LastFailureMessage = @FailureMessage
				WHERE ObjectName = @curObjectName;
			END
			ELSE
			BEGIN
				INSERT INTO #FailedChiRhoObjects (ObjectName, NumFailures, LastFailureMessage)
				SELECT @curObjectName, 0, @FailureMessage;
			END
		END CATCH

		FETCH IterateChiRhoObjects INTO @curObjectName;
	END

	CLOSE IterateChiRhoObjects;
	DEALLOCATE IterateChiRhoObjects;
END

IF EXISTS (SELECT * FROM #FailedChiRhoObjects WHERE NumFailures > 5)
BEGIN
	SELECT 'Views' as ObjType,* 
	FROM #FailedChiRhoObjects
	ORDER BY ObjectName;

	RAISERROR('> 5 failures (for one object) encountered when dropping views',16,1);
	GOTO ScriptFailure
END

TRUNCATE TABLE #FailedChiRhoObjects;

--tables
WHILE EXISTS (SELECT * FROM sys.tables t
				WHERE t.schema_id IN ( 
						SCHEMA_ID('CoreXR'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('XR'), SCHEMA_ID('HEM')
						)
				)
	AND NOT EXISTS (SELECT * FROM #FailedChiRhoObjects WHERE NumFailures > 5)
BEGIN
	TRUNCATE TABLE #CleanUpChiRhoObjects;

	INSERT INTO #CleanUpChiRhoObjects (
		ObjectName
	)
	SELECT 
		QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name)
	FROM sys.tables t
	WHERE t.schema_id IN ( 
						SCHEMA_ID('CoreXR'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('XR'), SCHEMA_ID('HEM')
						)
	;

	DECLARE IterateChiRhoObjects CURSOR FOR 
	SELECT o.ObjectName
	FROM #CleanUpChiRhoObjects o
	ORDER BY o.ObjectName;

	OPEN IterateChiRhoObjects;
	FETCH IterateChiRhoObjects INTO @curObjectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			SET @DynSQL = N'DROP TABLE ' + @curObjectName + N';'
			PRINT @DynSQL;
			EXEC (@DynSQL);
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @FailureMessage = N'Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
				N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
				N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
				N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

			IF EXISTS (SELECT * FROM #FailedChiRhoObjects o
						WHERE o.ObjectName = @curObjectName)
			BEGIN
				UPDATE #FailedChiRhoObjects
				SET NumFailures = NumFailures + 1,
					LastFailureMessage = @FailureMessage
				WHERE ObjectName = @curObjectName;
			END
			ELSE
			BEGIN
				INSERT INTO #FailedChiRhoObjects (ObjectName, NumFailures, LastFailureMessage)
				SELECT @curObjectName, 0, @FailureMessage;
			END
		END CATCH

		FETCH IterateChiRhoObjects INTO @curObjectName;
	END

	CLOSE IterateChiRhoObjects;
	DEALLOCATE IterateChiRhoObjects;
END

IF EXISTS (SELECT * FROM #FailedChiRhoObjects WHERE NumFailures > 5)
BEGIN
	SELECT 'Tables' as ObjType,* 
	FROM #FailedChiRhoObjects
	ORDER BY ObjectName;

	RAISERROR('> 5 failures (for one object) encountered when dropping tables',16,1);
	GOTO ScriptFailure
END

TRUNCATE TABLE #FailedChiRhoObjects;

BEGIN TRY
	IF EXISTS (SELECT * FROM sys.types t WHERE t.name = N'CoreXRFiltersType')
	BEGIN
		PRINT 'DROP TYPE dbo.CoreXRFiltersType;'
		DROP TYPE dbo.CoreXRFiltersType;
	END
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK;

	SET @FailureMessage = N'Drop Types: Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
		N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
		N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
		N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

	RAISERROR(@FailureMessage, 16, 1);
	GOTO ScriptFailure
END CATCH 

BEGIN TRY
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = N'AutoWho')
	BEGIN
		PRINT 'DROP SCHEMA [AutoWho];'
		DROP SCHEMA [AutoWho];
	END
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = N'ServerEye')
	BEGIN
		PRINT 'DROP SCHEMA [ServerEye];'
		DROP SCHEMA [ServerEye];
	END
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = N'HEM')
	BEGIN
		PRINT 'DROP SCHEMA [HEM];'
		DROP SCHEMA [HEM];
	END
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = N'XR')
	BEGIN
		PRINT 'DROP SCHEMA [XR];'
		DROP SCHEMA [XR];
	END
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = N'CoreXR')
	BEGIN
		PRINT 'DROP SCHEMA [CoreXR];'
		DROP SCHEMA [CoreXR];
	END
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK;

	SET @FailureMessage = N'Drop Schemas: Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
		N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
		N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
		N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

	RAISERROR(@FailureMessage, 16, 1);
	GOTO ScriptFailure
END CATCH

ScriptFailure:

