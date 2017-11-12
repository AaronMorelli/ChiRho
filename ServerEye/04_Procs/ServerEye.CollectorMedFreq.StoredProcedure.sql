SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [ServerEye].[CollectorMedFreq]
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

	FILE NAME: ServerEye.CollectorMedFreq.StoredProcedure.sql

	PROCEDURE NAME: ServerEye.CollectorMedFreq

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Runs on a schedule (or initiated by a user via a viewer proc) and calls various sub-procs to gather miscellaneous 
		server-level DMV data. Collects data that we want captured somewhat frequently (by default every 5 minutes)

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------

*/
(
	@init				TINYINT,
	@LocalCaptureTime	DATETIME, 
	@UTCCaptureTime		DATETIME
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @DynSQL NVARCHAR(4000),
			@CurDBID INT,
			@CurDBName NVARCHAR(128),
			@ChiRhoDBName NVARCHAR(128);

	DECLARE @errorloc NVARCHAR(100),
		@err__ErrorSeverity INT, 
		@err__ErrorState INT, 
		@err__ErrorText NVARCHAR(4000);

BEGIN TRY
	SET @ChiRhoDBName = DB_NAME();

	--DB and file stats.
	--First, update the DBID to Name mapping just to make sure these stats are tied to the correct database id/name pair
	EXEC CoreXR.UpdateDBMapping;

	IF OBJECT_ID('tempdb..#DBLogUsageStats') IS NOT NULL DROP TABLE #DBLogUsageStats;
	CREATE TABLE #DBLogUsageStats (
		[Database Name] NVARCHAR(128) NULL,
		[Log Size (MB)] DECIMAL(21,8) NULL,
		[Log Space Used Pct] DECIMAL(11,8) NULL,
		[Status] INT NULL
	);

	INSERT INTO #DBLogUsageStats
		EXEC ('DBCC SQLPERF(LOGSPACE)');

	INSERT INTO [ServerEye].[DatabaseStats] (
		[UTCCaptureTime],
		[LocalCaptureTime],
		[database_id],
		[user_access_desc],
		[state_desc],
		[LogSizeMB],
		[LogPctUsed]
	)
	SELECT 
		@UTCCaptureTime,
		@LocalCaptureTime,
		d.database_id,
		d.user_access_desc,
		d.state_desc,
		[LogSizeMB] = t.[Log Size (MB)],
		[LogPctUsed] = t.[Log Space Used Pct]
	FROM sys.databases d
		LEFT OUTER JOIN #DBLogUsageStats t
			ON d.name = t.[Database Name];

	DECLARE iterateDBsCollectStats CURSOR FOR 
	SELECT dstat.database_id
	FROM ServerEye.DatabaseStats dstat
	WHERE dstat.UTCCaptureTime = @UTCCaptureTime
	AND dstat.user_access_desc = 'MULTI_USER'
	AND dstat.state_desc = 'ONLINE'
	ORDER BY dstat.database_id ASC;

	OPEN iterateDBsCollectStats;
	FETCH iterateDBsCollectStats INTO @CurDBID;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @CurDBName = DB_NAME(@CurDBID);

		SET @DynSQL = N'USE ' + QUOTENAME(@CurDBName) + N';
		INSERT INTO ' + QUOTENAME(@ChiRhoDBName) + '.[ServerEye].[DBFileStats](
			[UTCCaptureTime],
			[LocalCaptureTime],
			[database_id],
			[file_id],
			[name],
			[type_desc],
			[state_desc],
			[is_media_read_only],
			[is_read_only],
			[mf_size_pages],
			[df_size_pages],
			[pages_used],
			[DataSpaceName],
			[DataSpaceType],
			[DataSpaceIsDefault],
			[FGIsReadOnly]
		)
		SELECT 
			@UTCCaptureTime,
			@LocalCaptureTime,
			mf.database_id,
			mf.file_id,
			mf.name,
			mf.type_desc,
			mf.state_desc,
			mf.is_media_read_only,
			mf.is_read_only,
			[mf_size_pages] = mf.size,
			[df_size_pages] = df.size,
			[pages_used] = FILEPROPERTY(df.name, ''SpaceUsed''),
			[DataSpaceName] = dsp.name,
			[DataSpaceType] = dsp.type_desc,
			[DataSpaceIsDefault] = dsp.is_default,
			[FGIsReadOnly] = fg.is_read_only
		FROM sys.master_files mf
			left outer join sys.database_files df
				on mf.file_id = df.file_id
			left outer join sys.data_spaces dsp
				on mf.data_space_id = dsp.data_space_id
			left outer join sys.filegroups fg
				on dsp.data_space_id = fg.data_space_id
		WHERE mf.database_id = ' + CONVERT(NVARCHAR(20),@CurDBID) + '
		';

		EXEC sp_executesql @DynSQL, N'@UTCCaptureTime DATETIME, @LocalCaptureTime DATETIME', @UTCCaptureTime, @LocalCaptureTime;

		FETCH iterateDBsCollectStats INTO @CurDBID;
	END

	CLOSE iterateDBsCollectStats;
	DEALLOCATE iterateDBsCollectStats;

	RETURN 0;
END TRY
BEGIN CATCH
PRINT 'EXC'
	IF @@TRANCOUNT > 0 ROLLBACK;

	SET @err__ErrorSeverity = ERROR_SEVERITY();
	SET @err__ErrorState = ERROR_STATE();
	SET @err__ErrorText = N'Unexpected exception occurred at location "' + ISNULL(@errorloc,N'<null>') + '". Error #: ' + 
		CONVERT(NVARCHAR(20),ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20),ERROR_STATE()) + N'; Severity: ' + 
		CONVERT(NVARCHAR(20),ERROR_SEVERITY()) + N'; message: ' + ERROR_MESSAGE();

	RAISERROR(@err__ErrorText, @err__ErrorSeverity, @err__ErrorState);
	RETURN -1;
END CATCH

	RETURN 0;
END
GO