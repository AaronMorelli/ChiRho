SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[Branches_ReturnCandidateStatements] 
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

	FILE NAME: AutoWho.Branches_ReturnCandidateStatements.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.Branches_ReturnCandidateStatements

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: 

	OUTSTANDING ISSUES: 

To Execute
------------------------
*/
(
	@init		TINYINT,
	@startUTC	DATETIME,
	@endUTC		DATETIME,
	@spid		INT,
	@rqst		INT,
	@rqststart	DATETIME
)
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	DECLARE 
		@lv__nullstring				NVARCHAR(8),
		@lv__nullint				INT,
		@lv__nullsmallint			SMALLINT,
		@PKSQLStmtStoreID			BIGINT, 
		@sql_handle					VARBINARY(64),
		@dbid						INT,
		@objectid					INT,
		@stmt_text					NVARCHAR(MAX),
		@stmt_xml					XML,
		@dbname						NVARCHAR(128),
		@schname					NVARCHAR(128),
		@objectname					NVARCHAR(128);

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value

	--First, validate that the @spid/@rqst/@rqststart is a valid statement in AutoWho.StatementCaptureTimes
	IF NOT EXISTS (
		SELECT *
		FROM AutoWho.StatementCaptureTimes sct
		WHERE sct.session_id = @spid
		AND sct.request_id = @rqst
		AND sct.TimeIdentifier = @rqststart
		)
	BEGIN
		--Is the statement in AutoWho.SessionsAndRequests?
		IF NOT EXISTS (
			SELECT * 
			FROM AutoWho.SessionsAndRequests sar
			WHERE sar.CollectionInitiatorID = @init
			AND sar.UTCCaptureTime BETWEEN @startUTC AND @endUTC
			AND sar.session_id = @spid
			AND sar.request_id = @rqst
			AND sar.TimeIdentifier = @rqststart
			)
		BEGIN
			RAISERROR('Batch request specified in @spid/@rqst/@rqststart parameters could not be found in the time range specified. Please check your parameter values.', 16, 1);
			RETURN -1;
		END
		ELSE
		BEGIN
			--In SAR but not SCT, probably just need to re-run the Master job to catch SCT up
			RAISERROR('Batch request specified in @spid/@rqst/@rqststart parameters was found but lacks some key data. Please rerun the ChiRho Master job to catch up the Statement Stats table.', 16, 1);
			RETURN -1;
		END
	END --IF request identifiers don't exist in SCT


	--Ok, the batch request exists. Let's find the statements for it.
	CREATE TABLE #CandidateStatements (
		PKSQLStmtStoreID	BIGINT NOT NULL,
		FirstSeenUTC		DATETIME NOT NULL,
		LastSeenUTC			DATETIME NOT NULL,
		TimesSeen			INT NOT NULL,

		FirstSeen			DATETIME NULL,
		LastSeen			DATETIME NULL
	);

	CREATE TABLE #SQLStmtStore (
		PKSQLStmtStoreID			BIGINT NOT NULL PRIMARY KEY CLUSTERED,
		[sql_handle]				VARBINARY(64) NOT NULL,
		statement_start_offset		INT NOT NULL,
		statement_end_offset		INT NOT NULL, 
		[dbid]						SMALLINT NOT NULL,
		[objectid]					INT NOT NULL,
		datalen_batch				INT NOT NULL,
		stmt_text					NVARCHAR(MAX) NOT NULL,
		stmt_xml					XML,
		dbname						NVARCHAR(128),
		schname						NVARCHAR(128),
		objname						NVARCHAR(128)
	);


	INSERT INTO #CandidateStatements (
		PKSQLStmtStoreID,
		FirstSeenUTC,
		LastSeenUTC,
		TimesSeen
	)
	SELECT 
		sct.PKSQLStmtStoreID,
		sct.StatementFirstCaptureUTC,
		[StatementLastCaptureUTC] = MAX(CASE WHEN (sct.IsStmtLastCapture = 1 OR sct.IsCurrentLastRowOfBatch = 1)
										THEN sct.UTCCaptureTime
										ELSE NULL
										END),
		[TimesSeen] = COUNT(*)
	FROM AutoWho.StatementCaptureTimes sct
	WHERE sct.session_id = @spid
	AND sct.request_id = @rqst
	AND sct.TimeIdentifier = @rqststart
	GROUP BY sct.PKSQLStmtStoreID,
		sct.StatementFirstCaptureUTC;


	UPDATE targ 
	SET FirstSeen = xapp1.SPIDCaptureTime,
		LastSeen = xapp2.SPIDCaptureTime
	FROM #CandidateStatements targ
		OUTER APPLY (
			SELECT sar.SPIDCaptureTime
			FROM AutoWho.SessionsAndRequests sar
			WHERE sar.CollectionInitiatorID = @init
			AND sar.session_id = @spid
			AND sar.request_id = @rqst
			AND sar.rqst__start_time = @rqst
			AND sar.UTCCaptureTime = targ.FirstSeenUTC
		) xapp1
		OUTER APPLY (
			SELECT sar.SPIDCaptureTime
			FROM AutoWho.SessionsAndRequests sar
			WHERE sar.CollectionInitiatorID = @init
			AND sar.session_id = @spid
			AND sar.request_id = @rqst
			AND sar.rqst__start_time = @rqst
			AND sar.UTCCaptureTime = targ.LastSeenUTC
		) xapp2;


	INSERT INTO #SQLStmtStore (
		PKSQLStmtStoreID,
		[sql_handle],
		statement_start_offset,
		statement_end_offset,
		[dbid],
		[objectid],
		datalen_batch,
		stmt_text
		--stmt_xml
		--dbname						NVARCHAR(128),
		--objname						NVARCHAR(128)
	)
	SELECT sss.PKSQLStmtStoreID, 
		sss.sql_handle,
		sss.statement_start_offset,
		sss.statement_end_offset,
		sss.dbid,
		sss.objectid,
		sss.datalen_batch,
		sss.stmt_text
	FROM CoreXR.SQLStmtStore sss
	WHERE sss.PKSQLStmtStoreID IN (
		SELECT DISTINCT cs.PKSQLStmtStoreID
		FROM #CandidateStatements cs
		WHERE cs.PKSQLStmtStoreID IS NOT NULL 
		);

	DECLARE resolveSQLStmtStore CURSOR LOCAL FAST_FORWARD FOR
	SELECT 
		PKSQLStmtStoreID,
		[sql_handle],
		[dbid],
		[objectid],
		stmt_text
	FROM #SQLStmtStore sss
	;

	OPEN resolveSQLStmtStore;
	FETCH resolveSQLStmtStore INTO @PKSQLStmtStoreID,
		@sql_handle,
		@dbid,
		@objectid,
		@stmt_text
	;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		--Note that one major assumption of this procedure is that the DBID hasn't changed since the time the spid was 
		-- collected. For performance reasons, we don't resolve DBID in AutoWho.Collector; thus, if a DB is detached/re-attached,
		-- or deleted and the DBID is re-used by a completely different database, confusion can ensue.
		IF @dbid > 0
		BEGIN
			SET @dbname = DB_NAME(@dbid);
		END
		ELSE
		BEGIN
			SET @dbname = N'';
		END

		--Above note about DBID is relevant for this as well. 
		IF @objectid > 0
		BEGIN
			SET @objectname = OBJECT_NAME(@objectid,@dbid);
		END
		ELSE
		BEGIN
			SET @objectname = N'';
		END

		IF @objectid > 0
		BEGIN
			--if we do have a dbid/objectid pair, get the schema for the object
			IF @dbid > 0
			BEGIN
				SET @schname = OBJECT_SCHEMA_NAME(@objectid, @dbid);
			END
			ELSE
			BEGIN
				--if we don't have a valid dbid, we still do a "best effort" attempt to get schema
				SET @schname = OBJECT_SCHEMA_NAME(@objectid);
			END
			
			IF @schname IS NULL
			BEGIN
				SET @schname = N'';
			END
		END
		ELSE
		BEGIN
			SET @schname = N'';
		END

		IF @sql_handle = 0x0
		BEGIN
			SET @stmt_xml = CONVERT(XML, N'<?cmd --' + NCHAR(10)+NCHAR(13) + N'sql_handle is 0x0. The current SQL statement cannot be displayed.' + NCHAR(10) + NCHAR(13) + 
			N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 
			NCHAR(10) + NCHAR(13) + N'-- ?>');
		END
		ELSE
		BEGIN
			IF @stmt_text IS NULL
			BEGIN
				SET @stmt_xml = CONVERT(XML, N'<?cmd --' + NCHAR(10)+NCHAR(13) + N'The statement text is NULL. No T-SQL command to display.' + NCHAR(10) + NCHAR(13) + 
					N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 
					NCHAR(10) + NCHAR(13) + N'-- ?>');
			END
			ELSE
			BEGIN
				BEGIN TRY
					SET @stmt_xml = CONVERT(XML, N'<?cmd --' + NCHAR(10)+NCHAR(13) + @stmt_text + + NCHAR(10) + NCHAR(13) + 
					N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 
					NCHAR(10) + NCHAR(13) + N'-- ?>');
				END TRY
				BEGIN CATCH
					SET @stmt_xml = CONVERT(XML, N'<?cmd --' + NCHAR(10)+NCHAR(13) + N'Error converting text to XML: ' + ERROR_MESSAGE() + NCHAR(10) + NCHAR(13) + 
					N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 

					NCHAR(10) + NCHAR(13) + N'-- ?>');
				END CATCH
			END
		END

		UPDATE #SQLStmtStore
		SET dbname = @dbname,
			objname = @objectname,
			schname = @schname,
			stmt_xml = @stmt_xml
		WHERE PKSQLStmtStoreID = @PKSQLStmtStoreID;

		FETCH resolveSQLStmtStore INTO @PKSQLStmtStoreID,
			@sql_handle,
			@dbid,
			@objectid,
			@stmt_text
		;
	END	--WHILE loop for SQL Stmt Store cursor
		
	CLOSE resolveSQLStmtStore;
	DEALLOCATE resolveSQLStmtStore;

	SELECT 
		cs.PKSQLStmtStoreID,
		cs.FirstSeen,
		cs.LastSeen,
		cs.TimesSeen,
		sss.dbname,
		sss.schname,
		sss.objname,
		sss.statement_start_offset,
		sss.statement_end_offset,
		sss.stmt_xml
	FROM #CandidateStatements cs
		LEFT OUTER JOIN #SQLStmtStore sss
			ON cs.PKSQLStmtStoreID = sss.PKSQLStmtStoreID
	ORDER BY FirstSeenUTC;

		RAISERROR('On the Results pane are a list of statement IDs (from the AutoWho statement store) that were executed and observed by the batch request specified
in the @spid/@rqst/@rqststart parameters. Please select a statement ID and add it as a parameter for this procedure.', 16, 1);

	RETURN -1;
END 
GO
