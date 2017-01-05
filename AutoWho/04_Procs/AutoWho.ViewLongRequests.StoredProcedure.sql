SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ViewLongRequests] 
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

	FILE NAME: AutoWho.ViewLongRequests.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.ViewLongRequests

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Called by the sp_XR_LongRequests user-facing procedure. 
		The logic below pulls data from the various AutoWho tables, based on parameter values, and combines
		and formats the data as appropriate. 


	METRIC NOTES: A big part of this proc's output is the collection of observed metric values for a given request.
		At any individual AutoWho capture, the following are observed (other less-relevant data points have been omitted):
			# of tasks allocated
			the DOP of the query (from dm_exec_query_memory_grants)
			Query Memory requested, granted, used
			TempDB usage at both the task & session level
			CPU usage at the request level
			logical and physical reads at the request level
			writes at the request level
			transaction log usage

		In this proc, these metrics are aggregated across the statements in a request. Since a given statement could
		be revisited (and there is no way for us to know whether a statement was revisited), we can't take it for granted
		that a metric will always increase (e.g. in the case of logical reads) or decrease (e.g. # of allocated tasks) 
		as we continue to observe a query. Thus, in addition to min & max, we also capture "first seen" and "last seen".
		In some cases, this will lead to redundant data, such as when a query was only observed once or twice, or if the
		query was not revisited and is just a standard "query runs and accumulates CPU/reads/writes/log" pattern.

		Here are the rules for displaying (so that we don't have the same #'s repeatedly displayed, distracting the eyes)
			The order of the columns in the result set for each metric:
				First/Last Delta
				First
				Last
				Min/Max Delta
				Min/Max
				Avg

			If [#Seen] = 1 OR Min=Max		there has been no variation of this metric for the statement
				print "First" but skip the rest, including both deltas

			Else If (First <> Last) OR (Min <> Max)		--there has been some variation in this metric
				--for metrics that should never decrease for a request (CPU, reads, writes, CPU, tran log bytes)
				If First = Min and Last = Max		--a predictable increase as the request/statement goes on
					print "first/last delta", "first", "last", and "avg". --By leaving Min/Max Delta and Min and Max blank, we show they are the same as F/L
				Else
					print all the columns
			
				--for other metrics, i.e. that don't follow the ever increasing pattern
				-- (# tasks, DOP, both tempdb metrics, all qmem metrics)
				Always print all the columns

	FUTURE ENHANCEMENTS: 

To Execute
------------------------

*/
(
	@init		TINYINT,
	@start		DATETIME, 
	@end		DATETIME,
	@mindur		INT,
	@spids		NVARCHAR(128)=N'',
	@xspids		NVARCHAR(128)=N'',
	@dbs		NVARCHAR(512)=N'',
	@xdbs		NVARCHAR(512)=N'',
	@attr		NCHAR(1),
	@plan		NCHAR(1),
	@units		NVARCHAR(20)
)
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SET XACT_ABORT ON;
	SET ANSI_WARNINGS ON;

	/****************************************************************************************************
	**********							Variables and Temp Tables							   **********
	*****************************************************************************************************/

	DECLARE 
		--stmt store
		@PKSQLStmtStoreID			BIGINT, 
		@sql_handle					VARBINARY(64),
		@dbid						INT,
		@objectid					INT,
		@stmt_text					NVARCHAR(MAX),
		@stmt_xml					XML,
		@dbname						NVARCHAR(128),
		@schname					NVARCHAR(128),
		@objectname					NVARCHAR(128),

		--QueryPlan Stmt/Batch store
		@PKQueryPlanStmtStoreID		BIGINT,
		@PKQueryPlanBatchStoreID	BIGINT,
		@plan_handle				VARBINARY(64),
		@query_plan_text			NVARCHAR(MAX),
		@query_plan_xml				XML,

		--input buffer store
		@PKInputBufferStore			BIGINT,
		@ibuf_text					NVARCHAR(4000),
		@ibuf_xml					XML,

		--General variables
		@DBInclusionsExist			INT,
		@DBExclusionsExist			INT,
		@SPIDInclusionsExist		INT,
		@SPIDExclusionsExist		INT,
		@cxpacketwaitid				SMALLINT,

		--Enums
		@enum__waitorder__none				TINYINT,
		@enum__waitorder__lck				TINYINT,
		@enum__waitorder__latchblock		TINYINT,
		@enum__waitorder_pglatch			TINYINT,
		@enum__waitorder__cxp				TINYINT,
		@enum__waitorder__other				TINYINT
		;

	DECLARE 
		--misc control-flow helpers
		@lv__scratchint				INT,
		@lv__msg					NVARCHAR(MAX),
		@lv__errsev					INT,
		@lv__errstate				INT,
		@lv__errorloc				NVARCHAR(100),
		@lv__nullstring				NVARCHAR(8),
		@lv__nullint				INT,
		@lv__nullsmallint			SMALLINT,
		@lv__DynSQL					NVARCHAR(MAX);

BEGIN TRY

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value

	SET @enum__waitorder__none =			CONVERT(TINYINT, 250);		--a. we typically want a "not waiting" task to sort near the end 
	SET @enum__waitorder__lck =				CONVERT(TINYINT, 5);		--b. lock waits should be at the top (so that blocking data is correct)
	SET @enum__waitorder__latchblock =		CONVERT(TINYINT, 10);		--c. sometimes latch waits can have a blocking spid, so those sort next, after lock waits.
																		--	these can be any type of latch (pg, pgio, a memory object, etc); 
	SET @enum__waitorder_pglatch =			CONVERT(TINYINT, 15);		-- Page and PageIO latches are fairly common, and in parallel plans we want them
																		-- to sort higher than other latches, e.g. the fairly common ACCESS_METHODS_DATASET_PARENT
	SET @enum__waitorder__cxp =				CONVERT(TINYINT, 200);		--d. parallel sorts near the end, since a parallel wait doesn't mean the spid is completely halted
	SET @enum__waitorder__other =			CONVERT(TINYINT, 20);		--e. catch-all bucket

	SELECT @cxpacketwaitid = dwt.DimWaitTypeID
	FROM AutoWho.DimWaitType dwt
	WHERE dwt.wait_type = N'CXPACKET';

	SET @lv__errorloc = N'Declare #TT';
	CREATE TABLE #FilterTab
	(
		FilterType	TINYINT NOT NULL, 
			--0 DB inclusion
			--1 DB exclusion
			--2 SPID inclusion
			--3 SPID exclusion
		FilterID	INT NOT NULL, 
		FilterName	NVARCHAR(255)
	);

	CREATE TABLE #LongBatches (
		session_id			INT NOT NULL,
		request_id			INT NOT NULL,
		rqst__start_time	DATETIME NOT NULL,
		BatchIdentifier		INT NOT NULL,
		FirstSeen			DATETIME NOT NULL,
		LastSeen			DATETIME NOT NULL, 
		StartingDBID		INT NULL,
		FKInputBufferStore	BIGINT,
		SessAttr			NVARCHAR(MAX),
		FilteredOut			INT NOT NULL
	);

	CREATE TABLE #sarcache (
		SPIDCaptureTime		DATETIME NOT NULL,
		session_id			INT NOT NULL,
		request_id			INT NOT NULL,
		rqst__start_time	DATETIME NOT NULL,
		BatchIdentifier		INT NOT NULL,		--sess/rqst/starttime uniquely identifies a batch, but in our XML grouping code, I don't want to mess
												-- with datetime values, so we create an int that is unique per batch.
		rqst__status_code	TINYINT NULL, 
		rqst__cpu_time		BIGINT NULL, 
		rqst__reads			BIGINT NULL, 
		rqst__writes		BIGINT NULL, 
		rqst__logical_reads BIGINT NULL, 
		rqst__FKDimCommand			SMALLINT NULL, 
		rqst__FKDimWaitType			SMALLINT NULL, 
		--tempdb__CurrentlyAllocatedPages		BIGINT NULL, 
		tempdb__TaskPages					BIGINT NULL,
		tempdb__SessPages					BIGINT NULL,
		tempdb__CalculatedNumberOfTasks		SMALLINT NULL,
		mgrant__requested_memory_kb	BIGINT NULL,
		mgrant__granted_memory_kb	BIGINT NULL,
		mgrant__used_memory_kb		BIGINT NULL, 
		mgrant__dop					SMALLINT NULL, 
		tran_log_bytes				BIGINT NULL,
		calc__tmr_wait				TINYINT NULL, 
		FKSQLStmtStoreID			BIGINT NOT NULL, 
		FKInputBufferStoreID		BIGINT NULL, 
		FKQueryPlanStmtStoreID		BIGINT NOT NULL
	);

	CREATE TABLE #tawcache (
		SPIDCaptureTime		DATETIME NOT NULL,
		session_id			INT NOT NULL,
		request_id			INT NOT NULL,
		task_address		VARBINARY(8) NOT NULL, 
		BatchIdentifier		INT NOT NULL,
		TaskIdentifier		INT NOT NULL,
		tstate				NVARCHAR(20) NOT NULL,
		FKDimWaitType		SMALLINT NOT NULL, 
		wait_duration_ms	BIGINT NOT NULL, 
		wait_order_category TINYINT NOT NULL, 
		wait_special_tag	NVARCHAR(100) NOT NULL,
		wait_special_number INT NOT NULL
	);

	CREATE TABLE #stmtstats (
		BatchIdentifier		INT NOT NULL,
		FKSQLStmtStoreID	BIGINT NOT NULL, 
		FKQueryPlanStmtStoreID BIGINT NOT NULL,
		[#Seen]				INT NOT NULL,
		FirstSeen			DATETIME NOT NULL, 
		LastSeen			DATETIME NOT NULL,
		StatusCodeAgg		NVARCHAR(100) NULL,
		Waits				NVARCHAR(4000) NULL,
		CXWaits				NVARCHAR(4000) NULL,

		tempdb_task__FirstSeenPages	BIGINT NULL,
		tempdb_task__LastSeenPages	BIGINT NULL,
		tempdb_task__MinPages	BIGINT NULL,
		tempdb_task__MaxPages	BIGINT NULL,
		tempdb_task__AvgPages	DECIMAL(21,2) NULL,
		--calculated fields:
		--tempdb_task__FirstLastDeltaPages
		--tempdb_task__MinMaxDeltaPages

		tempdb_sess__FirstSeenPages	BIGINT NULL,
		tempdb_sess__LastSeenPages	BIGINT NULL,
		tempdb_sess__MinPages	BIGINT NULL,
		tempdb_sess__MaxPages	BIGINT NULL,
		tempdb_sess__AvgPages	DECIMAL(21,2) NULL,
		--calculated fields:
		--tempdb_sess__FirstLastDeltaPages
		--tempdb_sess__MinMaxDeltaPages

		qmem_requested__FirstSeenKB	BIGINT NULL,
		qmem_requested__LastSeenKB	BIGINT NULL,
		qmem_requested__MinKB			BIGINT,
		qmem_requested__MaxKB			BIGINT,
		qmem_requested__AvgKB			DECIMAL(21,2) NULL,
		--calculated fields:
		--qmem_requested__FirstLastDeltaKB
		--qmem_requested__MinMaxDeltaKB

		qmem_granted__FirstSeenKB	BIGINT NULL,
		qmem_granted__LastSeenKB	BIGINT NULL,
		qmem_granted__MinKB			BIGINT,
		qmem_granted__MaxKB			BIGINT,
		qmem_granted__AvgKB			DECIMAL(21,2) NULL,
		--calculated fields:
		--qmem_granted__FirstLastDeltaKB
		--qmem_granted__MinMaxDeltaKB

		qmem_used__FirstSeenKB	BIGINT NULL,
		qmem_used__LastSeenKB	BIGINT NULL,
		qmem_used__MinKB		BIGINT,
		qmem_used__MaxKB		BIGINT,
		qmem_used__AvgKB		DECIMAL(21,2) NULL,
		--calculated fields:
		--qmem_used__FirstLastDeltaKB
		--qmem_used__MinMaxDeltaKB

		tasks_FirstSeen		SMALLINT NULL,
		tasks_LastSeen		SMALLINT NULL,
		tasks_MinSeen		SMALLINT NULL,
		tasks_MaxSeen		SMALLINT NULL,
		tasks_AvgSeen		DECIMAL(7,2) NULL,
		--no need for calculated fields here. The numbers are small enough
		-- for people to do it quickly w/their eye

		DOP_FirstSeen		SMALLINT NULL,
		DOP_LastSeen		SMALLINT NULL,
		DOP_MinSeen			SMALLINT NULL,
		DOP_MaxSeen			SMALLINT NULL,
		DOP_AvgSeen			DECIMAL(7,2) NULL,
		--no need for calculated fields here. The numbers are small enough
		-- for people to do it quickly w/their eye

		TlogUsed__FirstSeenBytes	BIGINT NULL,
		TlogUsed__LastSeenBytes		BIGINT NULL,
		TlogUsed__minBytes			BIGINT,
		TlogUsed__maxBytes			BIGINT,
		--calculated fields:
		--TlogUsed__FirstLastDeltaKB
		--TlogUsed__MinMaxDeltaKB

		CPUused__FirstSeenMs	BIGINT NULL,
		CPUused__LastSeenMs	BIGINT NULL,
		CPUused__minMs		BIGINT,
		CPUused__maxMs		BIGINT,
		--calculated fields:
		--CPUused__FirstLastDeltaMs
		--CPUused__MinMaxDeltaMs

		LReads__FirstSeenPages	BIGINT NULL,
		LReads__LastSeenPages	BIGINT NULL,
		LReads__MinPages	BIGINT NULL,
		LReads__MaxPages	BIGINT NULL,
		--calculated fields:
		--LReads__FirstLastDeltaPages
		--LReads__MinMaxDeltaPages

		PReads__FirstSeenPages	BIGINT NULL,
		PReads__LastSeenPages	BIGINT NULL,
		PReads__MinPages	BIGINT NULL,
		PReads__MaxPages	BIGINT NULL,
		--calculated fields:
		--PReads__FirstLastDeltaPages
		--PReads__MinMaxDeltaPages

		Writes__FirstSeenPages	BIGINT NULL,
		Writes__LastSeenPages	BIGINT NULL,
		Writes__MinPages	BIGINT NULL,
		Writes__MaxPages	BIGINT NULL
		--calculated fields:
		--Writes__FirstLastDeltaPages
		--Writes__MinMaxDeltaPages
	);

	--Note that this holds data in a partially-aggregated state, b/c of both the "tstate" field and the FKDimWaitType field are present in the grouping,
	-- but our final display will present the full aggregation over the data w/only one of them at a time. (tstate data in one field, waits in another) 
	CREATE TABLE #stmtwaitstats (
		BatchIdentifier		INT NOT NULL,
		FKSQLStmtStoreID	BIGINT NOT NULL, 
		FKQueryPlanStmtStoreID BIGINT NOT NULL,
		FKDimWaitType		SMALLINT NOT NULL,
		tstate				NVARCHAR(20) NOT NULL,
		wait_order_category TINYINT NOT NULL,
		wait_special_tag	NVARCHAR(100) NOT NULL,
		NumTasks			INT, 
		TotalWaitTime		BIGINT
	);

	-- There is also the possibility that conversion to XML will fail, so we don't want to wait until the final join.
	-- This temp table is our workspace for that resolution/conversion work.
	CREATE TABLE #SQLStmtStore (
		PKSQLStmtStoreID		BIGINT NOT NULL,
		[sql_handle]			VARBINARY(64) NOT NULL,
		statement_start_offset	INT NOT NULL,
		statement_end_offset	INT NOT NULL, 
		[dbid]					SMALLINT NOT NULL,
		[objectid]				INT NOT NULL,
		datalen_batch			INT NOT NULL,
		stmt_text				NVARCHAR(MAX) NOT NULL,
		stmt_xml				XML,
		dbname					NVARCHAR(128),
		schname					NVARCHAR(128),
		objname					NVARCHAR(128)
	);

	--Ditto, input buffer conversions to XML can fail.
	CREATE TABLE #InputBufferStore (
		PKInputBufferStoreID	BIGINT NOT NULL,
		inputbuffer				NVARCHAR(4000) NOT NULL,
		inputbuffer_xml			XML
	);

	--Ditto, QP conversions to XML can fail.
	CREATE TABLE #QueryPlanStmtStore (
		PKQueryPlanStmtStoreID		BIGINT NOT NULL,
		[plan_handle]				VARBINARY(64) NOT NULL,
		--statement_start_offset		INT NOT NULL,
		--statement_end_offset		INT NOT NULL,
		--[dbid]						SMALLINT NOT NULL,
		--[objectid]					INT NOT NULL,
		query_plan_text				NVARCHAR(MAX) NOT NULL,
		query_plan_xml				XML
	);

	/****************************************************************************************************
	**********						Request identification and scope filtering				   **********
	*****************************************************************************************************/

	IF ISNULL(@dbs,N'') = N''
	BEGIN
		SET @DBInclusionsExist = 0;		--this flag (only for inclusions) is a perf optimization
	END 
	ELSE
	BEGIN
		BEGIN TRY 
			INSERT INTO #FilterTab (FilterType, FilterID, FilterName)
				SELECT 0, d.database_id, d.name
				FROM (SELECT [dbnames] = LTRIM(RTRIM(Split.a.value(N'.', 'NVARCHAR(512)')))
					FROM (SELECT CAST(N'<M>' + REPLACE( @dbs,  N',' , N'</M><M>') + N'</M>' AS XML) AS dblist) xmlparse
					CROSS APPLY dblist.nodes(N'/M') Split(a)
					) SS
					INNER JOIN sys.databases d			--we need the join to sys.databases so that we can get the correct case for the DB name.
						ON LOWER(SS.dbnames) = LOWER(d.name)	--the user may have passed in a db name that doesn't match the case for the DB name in the catalog,
				WHERE SS.dbnames <> N'';						-- and on a server with case-sensitive collation, we need to make sure we get the DB name exactly right

			SET @lv__ScratchInt = @@ROWCOUNT;

			IF @lv__ScratchInt = 0
			BEGIN
				SET @DBInclusionsExist = 0;
			END
			ELSE
			BEGIN
				SET @DBInclusionsExist = 1;
			END
		END TRY
		BEGIN CATCH
			SET @lv__msg = N'Error occurred when attempting to CONVERT the @dbs parameter (comma-separated list of database names) to a table of valid DB names. Error #: ' +  
				CONVERT(NVARCHAR(20), ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20), ERROR_STATE()) + N'; Severity: ' + CONVERT(NVARCHAR(20), ERROR_SEVERITY()) + '; Message: ' + 
				ERROR_MESSAGE();

			RAISERROR(@lv__msg, 16, 1);
			RETURN -1;
		END CATCH
	END		--db inclusion string parsing


	IF ISNULL(@xdbs, N'') = N''
	BEGIN
		SET @DBExclusionsExist = 0;
	END
	ELSE
	BEGIN
		BEGIN TRY 
			INSERT INTO #FilterTab (FilterType, FilterID, FilterName)
				SELECT 1, d.database_id, d.name
				FROM (SELECT [dbnames] = LTRIM(RTRIM(Split.a.value(N'.', 'NVARCHAR(512)')))
					FROM (SELECT CAST(N'<M>' + REPLACE( @xdbs,  N',' , N'</M><M>') + N'</M>' AS XML) AS dblist) xmlparse
					CROSS APPLY dblist.nodes(N'/M') Split(a)
					) SS
					INNER JOIN sys.databases d			--we need the join to sys.databases so that we can get the correct case for the DB name.
						ON LOWER(SS.dbnames) = LOWER(d.name)	--the user may have passed in a db name that doesn't match the case for the DB name in the catalog,
				WHERE SS.dbnames <> N'';						-- and on a server with case-sensitive collation, we need to make sure we get the DB name exactly right

			SET @lv__ScratchInt = @@ROWCOUNT;

			IF @lv__ScratchInt = 0
			BEGIN
				SET @DBExclusionsExist = 0;
			END
			ELSE
			BEGIN
				SET @DBExclusionsExist = 1;
			END
		END TRY
		BEGIN CATCH
			SET @lv__msg = N'Error occurred when attempting to CONVERT the @xdbs parameter (comma-separated list of database names) to a table of valid DB names. Error #: ' +  
				CONVERT(NVARCHAR(20), ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20), ERROR_STATE()) + N'; Severity: ' + CONVERT(NVARCHAR(20), ERROR_SEVERITY()) + '; Message: ' + 
				ERROR_MESSAGE();

			RAISERROR(@lv__msg, 16, 1);
			RETURN -1;
		END CATCH
	END		--db exclusion string parsing

	IF ISNULL(@spids,N'') = N''
	BEGIN
		SET @SPIDInclusionsExist = 0;		--this flag (only for inclusions) is a perf optimization
	END 
	ELSE
	BEGIN
		BEGIN TRY 
			INSERT INTO #FilterTab (FilterType, FilterID, FilterName)
				SELECT 2, SS.spids, NULL
				FROM (SELECT [spids] = LTRIM(RTRIM(Split.a.value(N'.', 'NVARCHAR(512)')))
					FROM (SELECT CAST(N'<M>' + REPLACE( @spids,  N',' , N'</M><M>') + N'</M>' AS XML) AS spidlist) xmlparse
					CROSS APPLY spidlist.nodes(N'/M') Split(a)
					) SS
				WHERE SS.spids <> N'';

			SET @lv__ScratchInt = @@ROWCOUNT;

			IF @lv__ScratchInt = 0
			BEGIN
				SET @SPIDInclusionsExist = 0;
			END
			ELSE
			BEGIN
				SET @SPIDInclusionsExist = 1;
			END
		END TRY
		BEGIN CATCH
			SET @lv__msg = N'Error occurred when attempting to CONVERT the @spids parameter (comma-separated list of session IDs) to a table of valid integer values. Error #: ' +  
				CONVERT(NVARCHAR(20), ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20), ERROR_STATE()) + N'; Severity: ' + CONVERT(NVARCHAR(20), ERROR_SEVERITY()) + '; Message: ' + 
				ERROR_MESSAGE();

			RAISERROR(@lv__msg, 16, 1);
			RETURN -1;
		END CATCH
	END		--spid inclusion string parsing

	IF ISNULL(@xspids,N'') = N''
	BEGIN
		SET @SPIDExclusionsExist = 0;
	END 
	ELSE
	BEGIN
		BEGIN TRY 
			INSERT INTO #FilterTab (FilterType, FilterID, FilterName)
				SELECT 3, SS.spids, NULL
				FROM (SELECT [spids] = LTRIM(RTRIM(Split.a.value(N'.', 'NVARCHAR(512)')))
					FROM (SELECT CAST(N'<M>' + REPLACE( @xspids,  N',' , N'</M><M>') + N'</M>' AS XML) AS spidlist) xmlparse
					CROSS APPLY spidlist.nodes(N'/M') Split(a)
					) SS
				WHERE SS.spids <> N'';

			SET @lv__ScratchInt = @@ROWCOUNT;

			IF @lv__ScratchInt = 0
			BEGIN
				SET @SPIDExclusionsExist = 0;
			END
			ELSE
			BEGIN
				SET @SPIDExclusionsExist = 1;
			END
		END TRY
		BEGIN CATCH
			SET @lv__msg = N'Error occurred when attempting to CONVERT the @xspids parameter (comma-separated list of session IDs) to a table of valid integer values. Error #: ' +  
				CONVERT(NVARCHAR(20), ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20), ERROR_STATE()) + N'; Severity: ' + CONVERT(NVARCHAR(20), ERROR_SEVERITY()) + '; Message: ' + 
				ERROR_MESSAGE();

			RAISERROR(@lv__msg, 16, 1);
			RETURN -1;
		END CATCH
	END		--spid exclusion string parsing


	IF EXISTS (SELECT * FROM #FilterTab dbs 
					INNER JOIN #FilterTab xdbs
						ON dbs.FilterID = xdbs.FilterID
						AND dbs.FilterType = 0
						AND xdbs.FilterType = 1
			)
	BEGIN
		RAISERROR('A database cannot be specified in both the @dbs and @xdbs filter parameters.', 16, 1);
		RETURN -1;
	END

	IF EXISTS (SELECT * FROM #FilterTab dbs 
					INNER JOIN #FilterTab xdbs
						ON dbs.FilterID = xdbs.FilterID
						AND dbs.FilterType = 2
						AND xdbs.FilterType = 3
			)
	BEGIN
		RAISERROR('A session ID cannot be specified in both the @spids and @xspids filter parameters.', 16, 1);
		RETURN -1;
	END
	
	SET @lv__errorloc = N'Identify long requests';
	INSERT INTO #LongBatches (
		session_id,
		request_id,
		rqst__start_time,
		BatchIdentifier,
		FirstSeen,
		LastSeen,
		FilteredOut
	)
	SELECT 
		session_id, request_id, rqst__start_time, 
		BatchIdentifier = RANK() OVER (ORDER BY rqst__start_time, session_id, request_id),
		FirstSeen,
		LastSeen,
		0
	FROM (
		SELECT 
			sar.session_id, 
			sar.request_id,
			sar.rqst__start_time,
			FirstSeen = MIN(SpidCaptureTime),		--"Min" only within the @start/@end range
			LastSeen = MAX(SpidCaptureTime)			--"Max" only within the @start/@end range
		FROM AutoWho.SessionsAndRequests sar
		WHERE sar.CollectionInitiatorID = @init
		AND sar.SPIDCaptureTime BETWEEN @start AND @end 
		AND sar.request_id >= 0
		AND sar.rqst__start_time IS NOT NULL 
		AND sar.sess__is_user_process = 1
		AND sar.calc__threshold_ignore = 0
		AND sar.calc__duration_ms > @mindur*1000
		GROUP BY session_id,
			request_id,
			rqst__start_time
	) ss
	OPTION(RECOMPILE)
	;


	IF @SPIDInclusionsExist = 1		--we only want spids that match what's in the #FilterTab table
	BEGIN							--with type=2
		DELETE FROM #LongBatches
		WHERE NOT EXISTS (
			SELECT *
			FROM #FilterTab f
			WHERE f.FilterType=2
			AND f.FilterID = session_id
		);
	END
	ELSE
	BEGIN	--no inclusions exist. Are we excluding anything?
		IF @SPIDExclusionsExist = 1
		BEGIN
			DELETE FROM #LongBatches
			WHERE EXISTS (
				SELECT * 
				FROM #FilterTab f
				WHERE f.FilterType = 3
				AND f.FilterID = session_id
			);
		END
	END

	CREATE STATISTICS custstat1 ON #LongBatches (session_id, request_id, rqst__start_time, FirstSeen);

	--Can't filter by database until we have the DB info obtained (see previous statement)
	IF @attr = N'n'
	BEGIN
		UPDATE lb 
		SET lb.StartingDBID = sar.sess__database_id
		FROM #LongBatches lb
			INNER JOIN AutoWho.SessionsAndRequests sar 
				ON sar.SPIDCaptureTime = lb.FirstSeen
				AND sar.session_id = lb.session_id
				AND sar.request_id = lb.request_id
				AND sar.rqst__start_time = lb.rqst__start_time
		WHERE sar.CollectionInitiatorID = @init
		AND sar.SPIDCaptureTime BETWEEN @start and @end
		;
	END
	ELSE
	BEGIN
		UPDATE lb 
		SET lb.StartingDBID = sar.sess__database_id,
			SessAttr = N'<?spid' + CONVERT(NVARCHAR(20),sar.session_id) + N' -- ' + NCHAR(10) + NCHAR(13) + 

				N'Connect Time:				' + ISNULL(CONVERT(NVARCHAR(30),sar.conn__connect_time,113),N'<null>') + NCHAR(10) + 
				N'Login Time:					' + ISNULL(CONVERT(NVARCHAR(30),sar.sess__login_time,113),N'<null>') + NCHAR(10) + 
				N'Last Request Start Time:	' + ISNULL(CONVERT(NVARCHAR(30),sar.sess__last_request_start_time,113),N'<null>') + NCHAR(10) + 
				N'Last Request End Time:		' + ISNULL(CONVERT(NVARCHAR(30),sar.sess__last_request_end_time,113),N'<null>') + NCHAR(10) + NCHAR(13) + 

				N'Client PID:					' + ISNULL(CONVERT(NVARCHAR(20),sar.sess__host_process_id),N'<null>') + NCHAR(10) +
				N'Client Interface/Version:	' + ISNULL(dsa.client_interface_name,N'<null>') + N' / ' + ISNULL(CONVERT(NVARCHAR(20),dsa.client_version),N'<null>') + NCHAR(10) +
				N'Net Transport:				' + ISNULL(dca.net_transport,N'<null>') + NCHAR(10) +
				N'Client Address/Port:		' + ISNULL(dna.client_net_address,N'<null>') + + N' / ' + ISNULL(CONVERT(NVARCHAR(20),NULLIF(sar.conn__client_tcp_port,@lv__nullint)),N'<null>') + NCHAR(10) + 
				N'Local Address/Port:			' + ISNULL(NULLIF(dna.local_net_address,@lv__nullstring),N'<null>') + N' / ' + ISNULL(CONVERT(NVARCHAR(20),NULLIF(dna.local_tcp_port,@lv__nullint)),N'<null>') + NCHAR(10) + 
				N'Endpoint (Sess/Conn):		' + ISNULL(CONVERT(NVARCHAR(20),dsa.endpoint_id),N'<null>') + N' / ' + ISNULL(CONVERT(NVARCHAR(20),dca.endpoint_id),N'<null>') + NCHAR(10) + 
				N'Protocol Type/Version:		' + ISNULL(dca.protocol_type,N'<null>') + N' / ' + ISNULL(CONVERT(NVARCHAR(20),dca.protocol_version),N'<null>') + NCHAR(10) +
				N'Net Transport:				' + ISNULL(dca.net_transport,N'<null>') + NCHAR(10) + 
				N'Net Packet Size:			' + ISNULL(CONVERT(NVARCHAR(20),dca.net_packet_size),N'<null>') + NCHAR(10) + 
				N'Encrypt Option:				' + ISNULL(dca.encrypt_option,N'<null>') + NCHAR(10) + 
				N'Auth Scheme:				' + ISNULL(dca.auth_scheme,N'<null>') + NCHAR(10) + NCHAR(13) + 

				N'Node Affinity:				' + ISNULL(CONVERT(NVARCHAR(20),dca.node_affinity),N'<null>') + NCHAR(10) +
				N'Group ID (Sess/Rqst):		' + ISNULL(CONVERT(NVARCHAR(20),dsa.group_id),N'<null>') + N' / ' + ISNULL(CONVERT(NVARCHAR(20),ISNULL(sar.rqst__group_id,-1)),N'<null>') + NCHAR(10) + 
				N'Scheduler ID:				' + ISNULL(CONVERT(NVARCHAR(20),sar.rqst__scheduler_id),N'<null>') + NCHAR(10) + 
				N'Managed Code:				' + ISNULL(CONVERT(NVARCHAR(20),sar.rqst__executing_managed_code),N'<null>') + NCHAR(10) + NCHAR(13) + 

				N'Open Tran Count (Sess/Rqst):		' + ISNULL(CONVERT(NVARCHAR(20),sar.sess__open_transaction_count),N'<null>') + N' / ' + ISNULL(CONVERT(NVARCHAR(20),sar.rqst__open_transaction_count),N'<null>') + NCHAR(10) + 
				N'Tran Iso Level (Sess/Rqst):			' + ISNULL(CONVERT(NVARCHAR(20),dsa.transaction_isolation_level),N'<null>') + N' / ' + ISNULL(CONVERT(NVARCHAR(20),sar.rqst__transaction_isolation_level),N'<null>') + NCHAR(10) + 
				N'Lock Timeout (Sess/Rqst):			' + ISNULL(CONVERT(NVARCHAR(20),sar.sess__lock_timeout),N'<null>') + N' / ' + ISNULL(CONVERT(NVARCHAR(20),sar.rqst__lock_timeout),N'<null>') + NCHAR(10) + 
				N'Deadlock Priority (Sess/Rqst):		' + ISNULL(CONVERT(NVARCHAR(20),dsa.deadlock_priority),N'<null>') + N' / ' + ISNULL(CONVERT(NVARCHAR(20),sar.rqst__deadlock_priority),N'<null>') + NCHAR(10) + 
					NCHAR(13) + N' -- ?>'
		FROM #LongBatches lb
			INNER JOIN AutoWho.SessionsAndRequests sar 
				ON sar.SPIDCaptureTime = lb.FirstSeen
				AND sar.session_id = lb.session_id
				AND sar.request_id = lb.request_id
				AND sar.rqst__start_time = lb.rqst__start_time
			LEFT OUTER JOIN AutoWho.DimSessionAttribute dsa
				ON sar.sess__FKDimSessionAttribute = dsa.DimSessionAttributeID
			LEFT OUTER JOIN AutoWho.DimNetAddress dna
				ON sar.conn__FKDimNetAddress = dna.DimNetAddressID
			LEFT OUTER JOIN AutoWho.DimConnectionAttribute dca
				ON sar.conn__FKDimConnectionAttribute = dca.DimConnectionAttributeID
		WHERE sar.CollectionInitiatorID = @init
		AND sar.SPIDCaptureTime BETWEEN @start and @end
		;
	END

	IF @DBInclusionsExist = 1
	BEGIN
		DELETE FROM #LongBatches
		WHERE NOT EXISTS (
			SELECT *
			FROM #FilterTab f
			WHERE f.FilterType=0
			AND f.FilterID = StartingDBID
		);
	END
	ELSE
	BEGIN
		IF @DBExclusionsExist = 1
		BEGIN
			DELETE FROM #LongBatches 
			WHERE EXISTS (
				SELECT *
				FROM #FilterTab f
				WHERE f.FilterType=1
				AND f.FilterID = StartingDBID
			);
		END
	END


	/****************************************************************************************************
	**********									Get the data!								   **********
	*****************************************************************************************************/

	--For efficiency, let's grab the data from SAR and TAW that we will need for these batches, to save repeated
	-- trips to the much larger tables.
	SET @lv__errorloc = N'Populate SAR cache';
	INSERT INTO #sarcache (
		SPIDCaptureTime,
		session_id,
		request_id,
		rqst__start_time,
		BatchIdentifier,
		rqst__status_code,
		rqst__cpu_time,
		rqst__reads,
		rqst__writes,
		rqst__logical_reads,
		rqst__FKDimCommand,
		rqst__FKDimWaitType,
		--tempdb__CurrentlyAllocatedPages,
		tempdb__TaskPages,
		tempdb__SessPages,
		tempdb__CalculatedNumberOfTasks,
		mgrant__requested_memory_kb,
		mgrant__granted_memory_kb,
		mgrant__used_memory_kb,
		mgrant__dop,
		tran_log_bytes,
		calc__tmr_wait,
		FKSQLStmtStoreID,
		FKInputBufferStoreID,
		FKQueryPlanStmtStoreID
	)
	SELECT 
		sar.SPIDCaptureTime,				--1
		sar.session_id,
		sar.request_id,
		sar.rqst__start_time,
		lb.BatchIdentifier,
		sar.rqst__status_code,				--5
		sar.rqst__cpu_time,			
		sar.rqst__reads,			
		sar.rqst__writes,			
		sar.rqst__logical_reads,	
		sar.rqst__FKDimCommand,				--10
		sar.rqst__FKDimWaitType,			--11
		[tempdb__task] = (CASE WHEN (ISNULL(sar.tempdb__task_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_user_objects_dealloc_page_count,0)) < 0 THEN 0
								ELSE (ISNULL(sar.tempdb__task_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_user_objects_dealloc_page_count,0)) END + 
						CASE WHEN (ISNULL(sar.tempdb__task_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_internal_objects_dealloc_page_count,0)) < 0 THEN 0
								ELSE (ISNULL(sar.tempdb__task_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_internal_objects_dealloc_page_count,0)) END
						),
		[tempdb__sess] = (
						CASE WHEN (ISNULL(sar.tempdb__sess_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_user_objects_dealloc_page_count,0)) < 0 THEN 0
								ELSE (ISNULL(sar.tempdb__sess_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_user_objects_dealloc_page_count,0)) END + 
						CASE WHEN (ISNULL(sar.tempdb__sess_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_internal_objects_dealloc_page_count,0)) < 0 THEN 0
								ELSE (ISNULL(sar.tempdb__sess_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_internal_objects_dealloc_page_count,0)) END
						),
		sar.tempdb__CalculatedNumberOfTasks,	--13	
		sar.mgrant__requested_memory_kb,
		sar.mgrant__granted_memory_kb,		--14
		sar.mgrant__used_memory_kb,
		sar.mgrant__dop,
		trx.tran_log_bytes,
		sar.calc__tmr_wait,
		
		ISNULL(sar.FKSQLStmtStoreID,-1),				--20
		sar.FKInputBufferStoreID,
		CASE WHEN FKQueryPlanStmtStoreID IS NULL OR @plan=N'n' THEN -1 ELSE sar.FKQueryPlanStmtStoreID END

	FROM AutoWho.SessionsAndRequests sar
		INNER JOIN #LongBatches lb
			ON lb.session_id = sar.session_id
			AND lb.request_id = sar.request_id
			AND lb.rqst__start_time = sar.rqst__start_time
		LEFT OUTER JOIN (
			SELECT 
				SPIDCaptureTime,
				session_id,
				tran_log_bytes = SUM(tran_log_bytes)
			FROM (
				SELECT td.SPIDCaptureTime,
					td.session_id, 
					[tran_log_bytes] = CASE WHEN (ISNULL(dtdt_database_transaction_log_bytes_used,0) + ISNULL(dtdt_database_transaction_log_bytes_used_system,0)) >= 
													(ISNULL(dtdt_database_transaction_log_bytes_reserved,0) + ISNULL(dtdt_database_transaction_log_bytes_reserved_system,0)) 
											THEN ISNULL(dtdt_database_transaction_log_bytes_used,0) + ISNULL(dtdt_database_transaction_log_bytes_used_system,0)
											ELSE ISNULL(dtdt_database_transaction_log_bytes_reserved,0) + ISNULL(dtdt_database_transaction_log_bytes_reserved_system,0) END
				FROM AutoWho.TransactionDetails td
				WHERE td.CollectionInitiatorID = @init
				AND td.SPIDCaptureTime BETWEEN @start AND @end 
			) ss
			GROUP BY SPIDCaptureTime, session_id
		) trx
			ON sar.SPIDCaptureTime = trx.SPIDCaptureTime
			AND sar.session_id = trx.session_id
	WHERE sar.CollectionInitiatorID = @init
	AND sar.SPIDCaptureTime BETWEEN @start AND @end 
	OPTION(RECOMPILE);

	CREATE UNIQUE CLUSTERED INDEX CL1 ON #sarcache (BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID, SPIDCaptureTime);


	SET @lv__errorloc = N'Populate TAW cache';
	INSERT INTO #tawcache (
		SPIDCaptureTime,
		session_id,
		request_id,
		task_address,
		BatchIdentifier,
		TaskIdentifier,
		tstate,
		FKDimWaitType,
		wait_duration_ms,
		wait_order_category,
		wait_special_tag,
		wait_special_number
	)
	SELECT 
		SPIDCaptureTime,
		session_id,
		request_id,
		task_address,
		BatchIdentifier,
		TaskIdentifier = ROW_NUMBER() OVER (PARTITION BY BatchIdentifier, task_address
											ORDER BY SPIDCaptureTime ASC),
		tstate, 
		FKDimWaitType,
		wait_duration_ms,
		wait_order_category,
		wait_special_tag,
		wait_special_number
	FROM (
		SELECT 
			taw.SPIDCaptureTime,
			taw.session_id, 
			taw.request_id,
			taw.task_address,
			sar.BatchIdentifier,
			tstate, 
			taw.FKDimWaitType,
			taw.wait_duration_ms,
			taw.wait_order_category,
			taw.wait_special_tag,
			taw.wait_special_number
		FROM (
				SELECT 
					SPIDCaptureTime, 
					session_id,
					request_id,
					task_address,
					--We want to display the states that tasks were observed in over the many AutoWho collections
					-- for a given request, and we want to separate out waits related to parallelism (CXPACKET) from
					-- other waits.
					tstate = CASE WHEN taw.FKDimWaitType = @cxpacketwaitid AND taw.tstate = N'S'
									THEN CONVERT(NVARCHAR(20),N'Suspended(CX)') 
									WHEN taw.tstate = N'S' THEN N'Suspended'
									WHEN taw.tstate = N'A' THEN N'Runnable'
									WHEN taw.tstate = N'R' THEN N'Running'
									ELSE CONVERT(NVARCHAR(20),tstate) END,
					FKDimWaitType,
					wait_duration_ms,
					wait_order_category,
					wait_special_tag,
					wait_special_number,
					--A task_address can be waiting on multiple blockers. We just choose the row
					-- that has the largest wait_duration_ms
					rn = ROW_NUMBER() OVER (PARTITION BY SPIDCaptureTime, session_id, request_id, task_address
											ORDER BY wait_duration_ms DESC)
				FROM AutoWho.TasksAndWaits taw
				WHERE taw.CollectionInitiatorID = @init
				AND taw.SPIDCaptureTime BETWEEN @start AND @end
			) taw
			INNER JOIN #sarcache sar
				ON sar.SPIDCaptureTime = taw.SPIDCaptureTime
				AND sar.session_id = taw.session_id
				AND sar.request_id = taw.request_id
		WHERE taw.rn = 1
	) ss
	OPTION(RECOMPILE);

	--Now that we have isolated the requests that will be returned along
	-- with their SAR and TAW records, we start processing the "header"
	-- data (a request) and its first-level detail (the statements in that request).
	SET @lv__errorloc = N'Populate #stmtstats';
	INSERT INTO #stmtstats (
		BatchIdentifier,				--1
		[FKSQLStmtStoreID],
		[FKQueryPlanStmtStoreID],
		[#Seen],
		[FirstSeen],					--5
		[LastSeen],

		tempdb_task__MinPages,
		tempdb_task__MaxPages,
		tempdb_task__AvgPages,

		tempdb_sess__MinPages,			--10
		tempdb_sess__MaxPages,
		tempdb_sess__AvgPages,

		qmem_requested__MinKB,
		qmem_requested__MaxKB,
		qmem_requested__AvgKB,			--15

		qmem_granted__MinKB,
		qmem_granted__MaxKB,
		qmem_granted__AvgKB,

		qmem_used__MinKB,
		qmem_used__MaxKB,				--20
		qmem_used__AvgKB,
		
		tasks_MinSeen,
		tasks_MaxSeen,
		tasks_AvgSeen,

		DOP_MinSeen,					--25
		DOP_MaxSeen,
		DOP_AvgSeen,

		TlogUsed__minBytes,
		TlogUsed__maxBytes,

		CPUused__minMs,					--30
		CPUused__maxMs,

		LReads__MinPages,
		LReads__MaxPages,

		PReads__MinPages,
		PReads__MaxPages,				--35

		Writes__MinPages,
		Writes__MaxPages				--37
	)
	SELECT 
		s.BatchIdentifier,					--1
		s.FKSQLStmtStoreID,
		s.FKQueryPlanStmtStoreID,
		SUM(1) AS [#Seen], 
		MIN(SPIDCaptureTime) as FirstSeen,		--5
		MAX(SPIDCaptureTime) as LastSeen,

		MIN(s.tempdb__TaskPages),
		MAX(s.tempdb__TaskPages),
		CONVERT(DECIMAL(21,2),AVG(s.tempdb__TaskPages*1.)),
		
		MIN(s.tempdb__SessPages),				--10
		MAX(s.tempdb__SessPages),
		CONVERT(DECIMAL(21,2),AVG(s.tempdb__SessPages*1.)),

		MIN(s.mgrant__requested_memory_kb),
		MAX(s.mgrant__requested_memory_kb),
		CONVERT(DECIMAL(21,2),AVG(s.mgrant__requested_memory_kb*1.)),		--15

		MIN(s.mgrant__granted_memory_kb),
		MAX(s.mgrant__granted_memory_kb),
		CONVERT(DECIMAL(21,2),AVG(s.mgrant__granted_memory_kb*1.)),

		MIN(s.mgrant__used_memory_kb),
		MAX(s.mgrant__used_memory_kb),			--20
		CONVERT(DECIMAL(21,2),AVG(s.mgrant__used_memory_kb*1.)),

		MIN(s.tempdb__CalculatedNumberOfTasks),
		MAX(s.tempdb__CalculatedNumberOfTasks),
		CONVERT(DECIMAL(7,2),AVG(s.tempdb__CalculatedNumberOfTasks*1.)),

		MIN(s.mgrant__dop),					--25
		MAX(s.mgrant__dop), 
		CONVERT(DECIMAL(7,2),AVG(s.mgrant__dop*1.)), 

		MIN(s.tran_log_bytes),
		MAX(s.tran_log_bytes),

		MIN(s.rqst__cpu_time),				--30
		MAX(s.rqst__cpu_time),

		MIN(s.rqst__logical_reads),
		MAX(s.rqst__logical_reads),

		MIN(s.rqst__reads),
		MAX(s.rqst__reads),				--35

		MIN(s.rqst__writes),
		MAX(s.rqst__writes)				--37
	FROM #sarcache s
	GROUP BY s.BatchIdentifier,
		s.FKSQLStmtStoreID,
		s.FKQueryPlanStmtStoreID
	;

	UPDATE targ 
	SET tempdb_task__FirstSeenPages = f.tempdb__TaskPages,
		tempdb_task__LastSeenPages = l.tempdb__TaskPages,
		tempdb_sess__FirstSeenPages = f.tempdb__SessPages,
		tempdb_sess__LastSeenPages = l.tempdb__SessPages,
		qmem_requested__FirstSeenKB = f.mgrant__requested_memory_kb,
		qmem_requested__LastSeenKB = l.mgrant__requested_memory_kb,
		qmem_granted__FirstSeenKB = f.mgrant__granted_memory_kb,
		qmem_granted__LastSeenKB = l.mgrant__granted_memory_kb,
		qmem_used__FirstSeenKB = f.mgrant__used_memory_kb,
		qmem_used__LastSeenKB = l.mgrant__used_memory_kb,
		tasks_FirstSeen = f.tempdb__CalculatedNumberOfTasks,
		tasks_LastSeen = l.tempdb__CalculatedNumberOfTasks,
		DOP_FirstSeen = f.mgrant__dop,
		DOP_LastSeen = l.mgrant__dop,
		TlogUsed__FirstSeenBytes = f.tran_log_bytes,
		TlogUsed__LastSeenBytes = l.tran_log_bytes,
		CPUused__FirstSeenMs = f.rqst__cpu_time,
		CPUused__LastSeenMs = l.rqst__cpu_time,
		LReads__FirstSeenPages = f.rqst__logical_reads,
		LReads__LastSeenPages = l.rqst__logical_reads,
		PReads__FirstSeenPages = f.rqst__reads,
		PReads__LastSeenPages = l.rqst__reads,
		Writes__FirstSeenPages = f.rqst__writes,
		Writes__LastSeenPages = l.rqst__writes
	FROM #stmtstats targ
		CROSS APPLY (
			SELECT 
				cf.tempdb__TaskPages,
				cf.tempdb__SessPages,
				cf.mgrant__requested_memory_kb,
				cf.mgrant__granted_memory_kb,
				cf.mgrant__used_memory_kb,
				cf.tempdb__CalculatedNumberOfTasks,
				cf.mgrant__dop,
				cf.tran_log_bytes,
				cf.rqst__cpu_time,
				cf.rqst__logical_reads,
				cf.rqst__reads,
				cf.rqst__writes
			FROM #sarcache cf
			WHERE cf.BatchIdentifier = targ.BatchIdentifier
			AND cf.FKSQLStmtStoreID = targ.FKSQLStmtStoreID
			AND cf.FKQueryPlanStmtStoreID = targ.FKQueryPlanStmtStoreID
			AND cf.SPIDCaptureTime = targ.FirstSeen
		) f
		CROSS APPLY (
			SELECT
				cf.tempdb__TaskPages,
				cf.tempdb__SessPages,
				cf.mgrant__requested_memory_kb,
				cf.mgrant__granted_memory_kb,
				cf.mgrant__used_memory_kb,
				cf.tempdb__CalculatedNumberOfTasks,
				cf.mgrant__dop,
				cf.tran_log_bytes,
				cf.rqst__cpu_time,
				cf.rqst__logical_reads,
				cf.rqst__reads,
				cf.rqst__writes
			FROM #sarcache cf
			WHERE cf.BatchIdentifier = targ.BatchIdentifier
			AND cf.FKSQLStmtStoreID = targ.FKSQLStmtStoreID
			AND cf.FKQueryPlanStmtStoreID = targ.FKQueryPlanStmtStoreID
			AND cf.SPIDCaptureTime = targ.LastSeen
		) l

	--We now have the first and last observed time for each Batch/Stmt/Plan combo. Go and get 
	-- the first/last metrics 

	--We then process the second-level detail data, task-level info.
	-- We aggregate task-level info in a couple of different ways, including
	-- wait stats.
	SET @lv__errorloc = N'Populate Stmt Wait Stats';
	INSERT INTO #stmtwaitstats (
		BatchIdentifier,
		[FKSQLStmtStoreID],
		[FKQueryPlanStmtStoreID],
		FKDimWaitType,
		tstate,
		wait_order_category,
		wait_special_tag,
		NumTasks,
		TotalWaitTime
	)
	SELECT
		BatchIdentifier,
		FKSQLStmtStoreID,
		FKQueryPlanStmtStoreID,
		FKDimWaitType,
		tstate, 
		wait_order_category,
		wait_special_tag, 
		NumTasks = SUM(1),
		TotalWaitTime = SUM(wait_duration_ms)
	FROM (
		SELECT 
			sar.BatchIdentifier,
			sar.FKSQLStmtStoreID,
			sar.FKQueryPlanStmtStoreID,
			taw.FKDimWaitType, 
			tstate,
			taw.wait_order_category,				--need this to prevent CXPACKET waits from being the top wait every time

			taw.wait_duration_ms, 
			--For CXPacket waits, we're going to display the wait subtype
			wait_special_tag = CASE WHEN taw.wait_order_category = @enum__waitorder__cxp 
									THEN taw.wait_special_tag + N':' + ISNULL(CONVERT(NVARCHAR(20),taw.wait_special_number),N'')
									ELSE N'' END
		FROM #sarcache sar
			INNER JOIN (
				--We need to prevent *really* long waits from being double-counted. Join #tawcache to itself
				-- and find where the "current" wait time is actually > the gap between cur & prev SPIDCaptureTimes.
				SELECT 
					cur.BatchIdentifier,
					cur.SPIDCaptureTime,
					cur.session_id,
					cur.request_id,
					cur.tstate,
					cur.FKDimWaitType,
					cur.wait_order_category,
					cur.wait_special_tag,
					cur.wait_special_number,
					wait_duration_ms = CASE WHEN prev.wait_duration_ms IS NULL THEN cur.wait_duration_ms
										ELSE (--we have a match, and we already know the wait type is the same
											CASE WHEN cur.wait_duration_ms > DATEDIFF(millisecond, prev.SPIDCaptureTime, cur.SPIDCaptureTime)
												THEN cur.wait_duration_ms - prev.wait_duration_ms
												ELSE cur.wait_duration_ms
												END
											)
										END
				FROM #tawcache cur
					LEFT OUTER JOIN #tawcache prev
						ON cur.BatchIdentifier = prev.BatchIdentifier
						AND cur.task_address = prev.task_address
						AND cur.FKDimWaitType = prev.FKDimWaitType
						AND cur.TaskIdentifier = prev.TaskIdentifier+1
					) taw
				ON sar.SPIDCaptureTime = taw.SPIDCaptureTime
				AND sar.session_id = taw.session_id
				AND sar.request_id = taw.request_id
		) tbase
	GROUP BY BatchIdentifier,
		FKSQLStmtStoreID,
		FKQueryPlanStmtStoreID,
		FKDimWaitType,
		tstate,
		wait_order_category,
		wait_special_tag
	;


	--Resolve the statement IDs to the actual statement text
	SET @lv__errorloc = N'Obtain Stmt Store raw';
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
		SELECT DISTINCT fk.FKSQLStmtStoreID
		FROM #sarcache fk
		WHERE fk.FKSQLStmtStoreID > 0
		)
	;

	SET @lv__errorloc = N'Declare Stmt Store Cursor';
	DECLARE resolveSQLStmtStore CURSOR LOCAL FAST_FORWARD FOR
	SELECT 
		PKSQLStmtStoreID,
		[sql_handle],
		[dbid],
		[objectid],
		stmt_text
	FROM #SQLStmtStore sss
	;

	SET @lv__errorloc = N'Open Stmt Store Cursor';
	OPEN resolveSQLStmtStore;
	FETCH resolveSQLStmtStore INTO @PKSQLStmtStoreID,
		@sql_handle,
		@dbid,
		@objectid,
		@stmt_text
	;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @lv__errorloc = N'In Stmt Store loop';
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
			SET @stmt_xml = CONVERT(XML, N'<?Stmt --' + NCHAR(10)+NCHAR(13) + N'sql_handle is 0x0. The current SQL statement cannot be displayed.' + NCHAR(10) + NCHAR(13) + 
			N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 
			NCHAR(10) + NCHAR(13) + N'-- ?>');
		END
		ELSE
		BEGIN
			IF @stmt_text IS NULL
			BEGIN
				SET @stmt_xml = CONVERT(XML, N'<?Stmt --' + NCHAR(10)+NCHAR(13) + N'The statement text is NULL. No T-SQL command to display.' + NCHAR(10) + NCHAR(13) + 
					N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 
					NCHAR(10) + NCHAR(13) + N'-- ?>');
			END
			ELSE
			BEGIN
				BEGIN TRY
					SET @stmt_xml = CONVERT(XML, N'<?Stmt --' + NCHAR(10)+NCHAR(13) + @stmt_text + + NCHAR(10) + NCHAR(13) + 
					N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 
					NCHAR(10) + NCHAR(13) + N'-- ?>');
				END TRY
				BEGIN CATCH
					SET @stmt_xml = CONVERT(XML, N'<?Stmt --' + NCHAR(10)+NCHAR(13) + N'Error CONVERTing text to XML: ' + ERROR_MESSAGE() + NCHAR(10) + NCHAR(13) + 
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

	--Resolve the Input Buffer IDs to the corresponding text.
	SET @lv__errorloc = N'Obtain IB raw';
	INSERT INTO #InputBufferStore (
		PKInputBufferStoreID,
		inputbuffer
		--inputbuffer_xml
	)
	SELECT ibs.PKInputBufferStoreID,
		ibs.InputBuffer
	FROM CoreXR.InputBufferStore ibs
	WHERE ibs.PKInputBufferStoreID IN (
		SELECT DISTINCT fk.FKInputBufferStoreID 
		FROM #sarcache fk
		WHERE fk.FKInputBufferStoreID IS NOT NULL 
	)
	;

	SET @lv__errorloc = N'Declare IB cursor';
	DECLARE resolveInputBufferStore  CURSOR LOCAL FAST_FORWARD FOR 
	SELECT 
		PKInputBufferStoreID,
		inputbuffer
	FROM #InputBufferStore
	;

	SET @lv__errorloc = N'Open IB cursor';
	OPEN resolveInputBufferStore;
	FETCH resolveInputBufferStore INTO @PKInputBufferStore,
		@ibuf_text;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @lv__errorloc = N'In IB loop';
		IF @ibuf_text IS NULL
		BEGIN
			SET @ibuf_xml = CONVERT(XML, N'<?InputBuffer --' + NCHAR(10)+NCHAR(13) + N'The Input Buffer is NULL.' + NCHAR(10) + NCHAR(13) + 
			N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@PKInputBufferStore,-1)) + 
			NCHAR(10) + NCHAR(13) + N'-- ?>');
		END
		ELSE
		BEGIN
			BEGIN TRY
				SET @ibuf_xml = CONVERT(XML, N'<?InputBuffer --' + NCHAR(10)+NCHAR(13) + @ibuf_text + + NCHAR(10) + NCHAR(13) + 
				N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@PKInputBufferStore,-1)) + 
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END TRY
			BEGIN CATCH
				SET @ibuf_xml = CONVERT(XML, N'<?InputBuffer --' + NCHAR(10)+NCHAR(13) + N'Error CONVERTing Input Buffer to XML: ' + ERROR_MESSAGE() + NCHAR(10) + NCHAR(13) + 
				N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@PKInputBufferStore,-1)) + 
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END CATCH
		END

		UPDATE #InputBufferStore
		SET inputbuffer_xml = @ibuf_xml
		WHERE PKInputBufferStoreID = @PKInputBufferStore;

		FETCH resolveInputBufferStore INTO @PKInputBufferStore,
			@ibuf_text;
	END

	CLOSE resolveInputBufferStore;
	DEALLOCATE resolveInputBufferStore;

	--A given request/batch should only ever have 1 unique input buffer. (The ROW_NUMBER() calc below
	-- handles any weird cases). Assign that IB to the parent-level record for the request
	SET @lv__errorloc = N'Assign IB to batch';
	UPDATE targ 
	SET targ.FKInputBufferStore = ss2.FKInputBufferStoreID
	FROM #LongBatches targ
		INNER JOIN (
				SELECT 
					session_id, 
					request_id, 
					rqst__start_time, 
					FKInputBufferStoreID,
					rn = ROW_NUMBER() OVER (PARTITION BY session_id, request_id, rqst__start_time, FKInputBufferStoreID 
												ORDER BY NumOccurrences DESC)
				FROM (
					SELECT 
						sar.session_id,
						sar.request_id,
						sar.rqst__start_time,
						sar.FKInputBufferStoreID,
						NumOccurrences = COUNT(*)
					FROM #sarcache sar
					GROUP BY sar.session_id,
						sar.request_id,
						sar.rqst__start_time,
						sar.FKInputBufferStoreID
				) ss
			) ss2
				ON targ.session_id = ss2.session_id
				AND targ.request_id = ss2.request_id
				AND targ.rqst__start_time = ss2.rqst__start_time
	WHERE ss2.rn = 1
	;

	--Resolve query plan identifiers to their actual XML
	IF @plan = N'y'
	BEGIN
		SET @lv__errorloc = N'Obtain query plan store raw';
		INSERT INTO #QueryPlanStmtStore (
			PKQueryPlanStmtStoreID,
			[plan_handle],
			--statement_start_offset,
			--statement_end_offset,
			--[dbid],
			--[objectid],
			[query_plan_text]
			--[query_plan_xml]
		)
		SELECT 
			qpss.PKQueryPlanStmtStoreID,
			qpss.plan_handle,
			qpss.query_plan
		FROM CoreXR.QueryPlanStmtStore qpss
		WHERE qpss.PKQueryPlanStmtStoreID IN (
			SELECT DISTINCT fk.FKQueryPlanStmtStoreID
			FROM #sarcache fk
			WHERE fk.FKQueryPlanStmtStoreID > 0
		)
		;

		SET @lv__errorloc = N'Declare query plan cursor';
		DECLARE resolveQueryPlanStmtStore CURSOR LOCAL FAST_FORWARD FOR 
		SELECT qpss.PKQueryPlanStmtStoreID,
			qpss.plan_handle,
			qpss.query_plan_text
		FROM #QueryPlanStmtStore qpss;

		SET @lv__errorloc = N'Open query plan cursor';
		OPEN resolveQueryPlanStmtStore;
		FETCH resolveQueryPlanStmtStore INTO @PKQueryPlanStmtStoreID,
			@plan_handle,
			@query_plan_text;

		WHILE @@FETCH_STATUS = 0
		BEGIN
			SET @lv__errorloc = N'In query plan loop';
			IF @plan_handle = 0x0
			BEGIN
				SET @query_plan_xml = CONVERT(XML, N'<?StmtPlan --' + NCHAR(10)+NCHAR(13) + N'plan_handle is 0x0. The Statement Query Plan cannot be displayed.' + NCHAR(10) + NCHAR(13) + 
				N'PKQueryPlanStmtStoreID: ' + CONVERT(NVARCHAR(20), ISNULL(@PKQueryPlanStmtStoreID,-1)) +
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END
			ELSE
			BEGIN
				IF @query_plan_text IS NULL
				BEGIN
					SET @query_plan_xml = CONVERT(XML, N'<?StmtPlan --' + NCHAR(10)+NCHAR(13) + N'The Statement Query Plan is NULL.' + NCHAR(10) + NCHAR(13) + 
					N'PKQueryPlanStmtStoreID: ' + CONVERT(NVARCHAR(20), ISNULL(@PKQueryPlanStmtStoreID,-1)) +
					NCHAR(10) + NCHAR(13) + N'-- ?>');
				END
				ELSE
				BEGIN
					BEGIN TRY
						SET @query_plan_xml = CONVERT(XML, @query_plan_text);
					END TRY
					BEGIN CATCH
						--Most common reason for this is the 128-node limit
						SET @query_plan_xml = CONVERT(XML, N'<?StmtPlan --' + NCHAR(10)+NCHAR(13) + N'Error CONVERTing Statement Query Plan to XML: ' + ERROR_MESSAGE() + NCHAR(10) + NCHAR(13) + 
						N'PKQueryPlanStmtStoreID: ' + CONVERT(NVARCHAR(20), ISNULL(@PKQueryPlanStmtStoreID,-1)) +

						CASE WHEN ERROR_NUMBER() = 6335 AND @PKSQLStmtStoreID IS NOT NULL THEN 
							N'-- You can extract this query plan to a file with the below script
							--DROP TABLE dbo.largeQPbcpout
							SELECT query_plan
							INTO dbo.largeQPbcpout
							FROM CoreXR.QueryPlanStmtStore q
							WHERE q.PKQueryPlanStmtStoreID = ' + CONVERT(NVARCHAR(20),@PKQueryPlanStmtStoreID) + N'
							--then from a command line:
							bcp dbo.largeQPbcpout out c:\largeqpxmlout.sqlplan -c -S. -T
							'
						ELSE N'' END + 

						NCHAR(10) + NCHAR(13) + N'-- ?>');
					END CATCH
				END
			END

			UPDATE #QueryPlanStmtStore
			SET query_plan_xml = @query_plan_xml
			WHERE PKQueryPlanStmtStoreID = @PKQueryPlanStmtStoreID;

			FETCH resolveQueryPlanStmtStore INTO @PKQueryPlanStmtStoreID,
				@plan_handle,
				@query_plan_text;
		END

		CLOSE resolveQueryPlanStmtStore;
		DEALLOCATE resolveQueryPlanStmtStore;
	END

	--Aggregate the task states.
	SET @lv__errorloc = N'Assign Status Code Agg';
	UPDATE targ 
	SET StatusCodeAgg = t0.status_info
	FROM #stmtstats targ
		INNER JOIN (
		SELECT 
			status_nodes.status_node.value('(batchidentifier/text())[1]', 'INT') AS BatchIdentifier,
			status_nodes.status_node.value('(fksqlstmtstoreid/text())[1]', 'BIGINT') AS FKSQLStmtStoreID,
			status_nodes.status_node.value('(fkqueryplanstmtstoreid/text())[1]', 'BIGINT') AS FKQueryPlanStmtStoreID,
			status_nodes.status_node.value('(rqststatformatted/text())[1]', 'NVARCHAR(4000)') AS status_info
		FROM (
			SELECT 
				CONVERT(XML,
					REPLACE
					(
						CONVERT(NVARCHAR(MAX), status_raw.status_xml_raw) COLLATE Latin1_General_Bin2,
						N'</rqststatformatted></status><status><rqststatformatted>',
						N', '
						+ 
					--LEFT(CRYPT_GEN_RANDOM(1), 0)
					LEFT(CONVERT(NVARCHAR(40),NEWID()),0)

					--This statement sometimes runs very slow, so we are using a "side effecting function" to avoid the default Expression Service caching behavior,
					-- per the Paul White blog article here: http://sqlblog.com/blogs/paul_white/archive/2012/09/05/compute-scalars-expressions-and-execution-plan-performance.aspx 
					)
				) AS status_xml
			FROM (
				SELECT 
					batchidentifier = CASE WHEN ordered.OccurrenceOrder = 1 THEN ordered.BatchIdentifier ELSE NULL END, 
					fksqlstmtstoreid = CASE WHEN ordered.OccurrenceOrder = 1 THEN ordered.FKSQLStmtStoreID ELSE NULL END, 
					fkqueryplanstmtstoreid = CASE WHEN ordered.OccurrenceOrder = 1 THEN ordered.FKQueryPlanStmtStoreID ELSE NULL END,
					rqststatformatted = StatusCode + N',' + CONVERT(NVARCHAR(20),Pct) + N'% (' + CONVERT(NVARCHAR(20),NumOccurrences) + N')' 
				FROM (
					SELECT BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID, StatusCode, NumOccurrences, 
						TotalPerStmt,
						Pct = CONVERT(DECIMAL(4,1),100.*(1.*NumOccurrences) / (1.*TotalPerStmt)), 
						OccurrenceOrder = ROW_NUMBER() OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID
															ORDER BY NumOccurrences DESC)
					FROM (
						SELECT 
							BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID, StatusCode, NumOccurrences, 
							[TotalPerStmt] = SUM(NumOccurrences) OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID)
						FROM (
							SELECT 
								BatchIdentifier,
								FKSQLStmtStoreID,
								FKQueryPlanStmtStoreID,
								StatusCode = tstate,
								NumOccurrences = SUM(1)
							FROM #stmtwaitstats sar
							GROUP BY BatchIdentifier,
								FKSQLStmtStoreID,
								FKQueryPlanStmtStoreID,
								tstate
						) grp
					) grpwithtotal
				) ordered 
				ORDER BY ordered.BatchIdentifier, ordered.FKSQLStmtStoreID, ordered.FKQueryPlanStmtStoreID 
				FOR XML PATH(N'status')
			) AS status_raw (status_xml_raw)
		) as status_final
		CROSS APPLY status_final.status_xml.nodes(N'/status') AS status_nodes (status_node)
		WHERE status_nodes.status_node.exist(N'batchidentifier') = 1
		--order by 1, 2, 3
	) t0
		ON targ.BatchIdentifier = t0.BatchIdentifier
		AND targ.FKSQLStmtStoreID = t0.FKSQLStmtStoreID
		AND targ.FKQueryPlanStmtStoreID = t0.FKQueryPlanStmtStoreID
	;
	

	--Now do non-CX waits
	SET @lv__errorloc = N'Construct nonCX waits';
	UPDATE targ 
	SET Waits = t0.waity_info
	FROM #stmtstats targ
		INNER JOIN (
		SELECT 
			waity_nodes.waity_node.value('(batchidentifier/text())[1]', 'INT') AS BatchIdentifier,
			waity_nodes.waity_node.value('(fksqlstmtstoreid/text())[1]', 'BIGINT') AS FKSQLStmtStoreID,
			waity_nodes.waity_node.value('(fkqueryplanstmtstoreid/text())[1]', 'BIGINT') AS FKQueryPlanStmtStoreID,
			waity_nodes.waity_node.value('(waitformatted/text())[1]', 'NVARCHAR(4000)') AS waity_info
		FROM (
			SELECT
					CONVERT(XML,
						REPLACE
						(
							CONVERT(NVARCHAR(MAX), waity_raw.waity_xml_raw) COLLATE Latin1_General_Bin2,
							N'</waitformatted></waity><waity><waitformatted>',
							N', '
							+ 
						--LEFT(CRYPT_GEN_RANDOM(1), 0)
						LEFT(CONVERT(NVARCHAR(40),NEWID()),0)

						--This statement sometimes runs very slow, so we are using a "side effecting function" to avoid the default Expression Service caching behavior,
						-- per the Paul White blog article here: http://sqlblog.com/blogs/paul_white/archive/2012/09/05/compute-scalars-expressions-and-execution-plan-performance.aspx 
						)
					) AS waity_xml 
			FROM (
				SELECT 
					batchidentifier = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.BatchIdentifier ELSE NULL END, 
					fksqlstmtstoreid = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.FKSQLStmtStoreID ELSE NULL END, 
					fkqueryplanstmtstoreid = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.FKQueryPlanStmtStoreID ELSE NULL END,
					waitformatted = waittype + N'{' + CONVERT(NVARCHAR(20),NumTasks) + N'x' + 
									CONVERT(NVARCHAR(20),AvgWaitTime) + N'ms=' + 
										CASE WHEN TotalWaitTime = N'''' THEN N'''' 
											ELSE SUBSTRING(TotalWaitTime, 1, CHARINDEX('.',TotalWaitTime)-1) END + 
									N' (' + CONVERT(NVARCHAR(20),WaitPct) + N'%)' + N' }' 
				FROM (
						SELECT 
							BatchIdentifier,
							FKSQLStmtStoreID,
							FKQueryPlanStmtStoreID,
							waittype,
							NumTasks,
							TotalWaitTime=ISNULL(CONVERT(NVARCHAR(20),CONVERT(MONEY,TotalWaitTime),1),N''),
							AvgWaitTime,
							WaitPct = CASE WHEN AllWaitTime <= 0 THEN -1 ELSE
									CONVERT(DECIMAL(4,1),100*(1.*TotalWaitTime) / (1.*AllWaitTime)) END,
							PriorityOrder = ROW_NUMBER() OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID
																ORDER BY TotalWaitTime DESC)
						FROM (
							SELECT 
								BatchIdentifier,
								FKSQLStmtStoreID,
								FKQueryPlanStmtStoreID,
								waittype = CASE WHEN dwt.latch_subtype <> N'' THEN dwt.wait_type + N'(' + dwt.latch_subtype + N')'
													ELSE dwt.wait_type END,
								NumTasks, 
								TotalWaitTime, 
								AvgWaitTime = CONVERT(DECIMAL(21,1), (1.*TotalWaitTime) / (1.*NumTasks)),
								AllWaitTime = SUM(TotalWaitTime) OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID)
							FROM (
								SELECT 
									BatchIdentifier, 
									FKSQLStmtStoreID, 
									FKQueryPlanStmtStoreID, 
									FKDimWaitType, 
									--wait_order_category,		--note that we do NOT include this here, because unlike SessionViewer, we DO want to show
																-- the waits by how many/long they are, regardless of category.

									--We need to re-sum (final aggregation) b/c the data in the table has tstate as an additional grouping field,
									-- but we are not including tstate in this data.
									NumTasks = SUM(NumTasks), 
									TotalWaitTime = SUM(TotalWaitTime)
								FROM #stmtwaitstats w
								WHERE w.wait_order_category <> @enum__waitorder__cxp
								AND w.FKDimWaitType <> 1		--we ignore running tasks for this field
								GROUP BY BatchIdentifier, 
									FKSQLStmtStoreID, 
									FKQueryPlanStmtStoreID, 
									FKDimWaitType
							) grp
							INNER JOIN AutoWho.DimWaitType dwt
								ON grp.FKDimWaitType = dwt.DimWaitTypeID
						) grpwithtotal
							--debug
							--order by BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID
					) ordered
					ORDER BY ordered.BatchIdentifier, ordered.FKSQLStmtStoreID, ordered.FKQueryPlanStmtStoreID
					FOR XML PATH(N'waity')
				) AS waity_raw (waity_xml_raw)
			) as waity_final
			CROSS APPLY waity_final.waity_xml.nodes(N'/waity') AS waity_nodes (waity_node)
			WHERE waity_nodes.waity_node.exist(N'batchidentifier') = 1
	) t0
		ON targ.BatchIdentifier = t0.BatchIdentifier
		AND targ.FKSQLStmtStoreID = t0.FKSQLStmtStoreID
		AND targ.FKQueryPlanStmtStoreID = t0.FKQueryPlanStmtStoreID
		;


	--And now CXP waits
	SET @lv__errorloc = N'Construct CX waits';
	UPDATE targ 
	SET CXWaits = t0.mcwaiter_info
	FROM #stmtstats targ
		INNER JOIN (
		SELECT 
			mcwaiter_nodes.mcwaiter_node.value('(batchidentifier/text())[1]', 'INT') AS BatchIdentifier,
			mcwaiter_nodes.mcwaiter_node.value('(fksqlstmtstoreid/text())[1]', 'BIGINT') AS FKSQLStmtStoreID,
			mcwaiter_nodes.mcwaiter_node.value('(fkqueryplanstmtstoreid/text())[1]', 'BIGINT') AS FKQueryPlanStmtStoreID,
			mcwaiter_nodes.mcwaiter_node.value('(waitformatted/text())[1]', 'NVARCHAR(4000)') AS mcwaiter_info
		FROM (
			SELECT
					CONVERT(XML,
						REPLACE
						(
							CONVERT(NVARCHAR(MAX), mcwaiter_raw.mcwaiter_xml_raw) COLLATE Latin1_General_Bin2,
							N'</waitformatted></mcwaiter><mcwaiter><waitformatted>',
							N', '
							+ 
						--LEFT(CRYPT_GEN_RANDOM(1), 0)
						LEFT(CONVERT(NVARCHAR(40),NEWID()),0)

						--This statement sometimes runs very slow, so we are using a "side effecting function" to avoid the default Expression Service caching behavior,
						-- per the Paul White blog article here: http://sqlblog.com/blogs/paul_white/archive/2012/09/05/compute-scalars-expressions-and-execution-plan-performance.aspx 
						)
					) AS mcwaiter_xml 
			FROM (
				SELECT 
					batchidentifier = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.BatchIdentifier ELSE NULL END, 
					fksqlstmtstoreid = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.FKSQLStmtStoreID ELSE NULL END, 
					fkqueryplanstmtstoreid = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.FKQueryPlanStmtStoreID ELSE NULL END,
					waitformatted = waittype + 
									N'{' + CONVERT(NVARCHAR(20),NumTasks) + N'x' + 
									CONVERT(NVARCHAR(20),AvgWaitTime) + N'ms=' + 
										CASE WHEN TotalWaitTime = N'''' THEN N'''' 
											ELSE SUBSTRING(TotalWaitTime, 1, CHARINDEX('.',TotalWaitTime)-1) END + 
									N' (' + CONVERT(NVARCHAR(20),WaitPct) + N'%)' + N' }' 
				FROM (
						SELECT 
							BatchIdentifier,
							FKSQLStmtStoreID,
							FKQueryPlanStmtStoreID,
							waittype,
							NumTasks,
							TotalWaitTime=ISNULL(CONVERT(NVARCHAR(20),CONVERT(MONEY,TotalWaitTime),1),N''),
							AvgWaitTime,
							WaitPct = CASE WHEN AllWaitTime <= 0 THEN -1 ELSE 
										CONVERT(DECIMAL(4,1),100*(1.*TotalWaitTime) / (1.*AllWaitTime)) END,
							PriorityOrder = ROW_NUMBER() OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID
																ORDER BY TotalWaitTime DESC)
						FROM (
							SELECT 
								BatchIdentifier,
								FKSQLStmtStoreID,
								FKQueryPlanStmtStoreID,
								waittype = wait_special_tag,

								NumTasks, 
								TotalWaitTime, 
								AvgWaitTime = CONVERT(DECIMAL(21,1), (1.*TotalWaitTime) / (1.*NumTasks)),
								AllWaitTime = SUM(TotalWaitTime) OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID)
							FROM (
								SELECT 
									BatchIdentifier, 
									FKSQLStmtStoreID, 
									FKQueryPlanStmtStoreID, 
									wait_special_tag,

									--We need to re-sum (final aggregation) b/c the data in the table has tstate as an additional grouping field,
									-- but we are not including tstate in this data.
									NumTasks = SUM(NumTasks), 
									TotalWaitTime = SUM(TotalWaitTime)
								FROM #stmtwaitstats w
								WHERE w.wait_order_category = @enum__waitorder__cxp
								AND w.FKDimWaitType <> 1		--we ignore running tasks for this field
								AND w.wait_special_tag <> N'?:-929'		-- no node ID, unknown wait sub-type... the resource_description field is prob fragmented
								GROUP BY BatchIdentifier, 
									FKSQLStmtStoreID, 
									FKQueryPlanStmtStoreID, 
									wait_special_tag
							) grp
							--For CX waits, we already know the wait type (CXPACKET!) so we can avoid
							-- the join to DimWaitType completely
							--INNER JOIN AutoWho.DimWaitType dwt
							--	ON grp.FKDimWaitType = dwt.DimWaitTypeID
						) grpwithtotal
							--debug
							--order by BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID
					) ordered
					ORDER BY ordered.BatchIdentifier, ordered.FKSQLStmtStoreID, ordered.FKQueryPlanStmtStoreID
					FOR XML PATH(N'mcwaiter')
				) AS mcwaiter_raw (mcwaiter_xml_raw)
			) as mcwaiter_final
			CROSS APPLY mcwaiter_final.mcwaiter_xml.nodes(N'/mcwaiter') AS mcwaiter_nodes (mcwaiter_node)
			WHERE mcwaiter_nodes.mcwaiter_node.exist(N'batchidentifier') = 1
		) t0
		ON targ.BatchIdentifier = t0.BatchIdentifier
		AND targ.FKSQLStmtStoreID = t0.FKSQLStmtStoreID
		AND targ.FKQueryPlanStmtStoreID = t0.FKQueryPlanStmtStoreID
		;

	SET @lv__errorloc = N'Construct final dynSQL';
	DECLARE @lv__CalcSubQuery NVARCHAR(MAX),
			@lv__FormatSubQuery NVARCHAR(MAX),
			@lv__UnionSubQuery NVARCHAR(MAX),
			@lv__OuterSelect NVARCHAR(MAX);

	/* Remember the display rules:

			The order of the columns in the result set for each metric:
				First/Last Delta
				First
				Last
				Min/Max Delta
				Min/Max
				Avg

			If [#Seen] = 1 OR Min=Max		there has been no variation of this metric for the statement
				print "First" but skip the rest, including both deltas
				THIS IS CASE=0

			Else --there has been some variation in this metric
				--for metrics that should never decrease for a request (CPU, reads, writes, CPU, tran log bytes)
				If First = Min and Last = Max		--a predictable increase as the request/statement goes on
					print "first/last delta", "first", "last", and "avg". --By leaving Min/Max Delta and Min and Max blank, we show they are the same as F/L
					THIS IS CASE=1
				Else
					print all the columns
					THIS IS CASE=2
			
				--for other metrics, i.e. that don't follow the ever increasing pattern
				-- (# tasks, DOP, both tempdb metrics, all qmem metrics)
				Always print all the columns
				THIS IS CASE=3


		tempdb_task__FirstSeenPages	BIGINT NULL,
		tempdb_task__LastSeenPages	BIGINT NULL,
		tempdb_task__MinPages	BIGINT NULL,
		tempdb_task__MaxPages	BIGINT NULL,
		tempdb_task__AvgPages	DECIMAL(21,2) NULL,
		--calculated fields:
		--tempdb_task__FirstLastDeltaPages
		--tempdb_task__MinMaxDeltaPages

		tempdb_sess__FirstSeenPages	BIGINT NULL,
		tempdb_sess__LastSeenPages	BIGINT NULL,
		tempdb_sess__MinPages	BIGINT NULL,
		tempdb_sess__MaxPages	BIGINT NULL,
		tempdb_sess__AvgPages	DECIMAL(21,2) NULL,
		--calculated fields:
		--tempdb_sess__FirstLastDeltaPages
		--tempdb_sess__MinMaxDeltaPages

		qmem_requested__FirstSeenKB	BIGINT NULL,
		qmem_requested__LastSeenKB	BIGINT NULL,
		qmem_requested__MinKB			BIGINT,
		qmem_requested__MaxKB			BIGINT,
		qmem_requested__AvgKB			DECIMAL(21,2) NULL,
		--calculated fields:
		--qmem_requested__FirstLastDeltaKB
		--qmem_requested__MinMaxDeltaKB

		qmem_granted__FirstSeenKB	BIGINT NULL,
		qmem_granted__LastSeenKB	BIGINT NULL,
		qmem_granted__MinKB			BIGINT,
		qmem_granted__MaxKB			BIGINT,
		qmem_granted__AvgKB			DECIMAL(21,2) NULL,
		--calculated fields:
		--qmem_granted__FirstLastDeltaKB
		--qmem_granted__MinMaxDeltaKB

		qmem_used__FirstSeenKB	BIGINT NULL,
		qmem_used__LastSeenKB	BIGINT NULL,
		qmem_used__MinKB		BIGINT,
		qmem_used__MaxKB		BIGINT,
		qmem_used__AvgKB		DECIMAL(21,2) NULL,
		--calculated fields:
		--qmem_used__FirstLastDeltaKB
		--qmem_used__MinMaxDeltaKB

		tasks_FirstSeen		SMALLINT NULL,
		tasks_LastSeen		SMALLINT NULL,
		tasks_MinSeen		SMALLINT NULL,
		tasks_MaxSeen		SMALLINT NULL,
		tasks_AvgSeen		DECIMAL(7,2) NULL,
		--no need for calculated fields here. The numbers are small enough
		-- for people to do it quickly w/their eye

		DOP_FirstSeen		SMALLINT NULL,
		DOP_LastSeen		SMALLINT NULL,
		DOP_MinSeen			SMALLINT NULL,
		DOP_MaxSeen			SMALLINT NULL,
		DOP_AvgSeen			DECIMAL(7,2) NULL,
		--no need for calculated fields here. The numbers are small enough
		-- for people to do it quickly w/their eye

		TlogUsed__FirstSeenBytes	BIGINT NULL,
		TlogUsed__LastSeenBytes		BIGINT NULL,
		TlogUsed__minBytes			BIGINT,
		TlogUsed__maxBytes			BIGINT,
		--calculated fields:
		--TlogUsed__FirstLastDeltaKB
		--TlogUsed__MinMaxDeltaKB

		CPUused__FirstSeenMs	BIGINT NULL,
		CPUused__LastSeenMs	BIGINT NULL,
		CPUused__minMs		BIGINT,
		CPUused__maxMs		BIGINT,
		--calculated fields:
		--CPUused__FirstLastDeltaMs
		--CPUused__MinMaxDeltaMs

		LReads__FirstSeenPages	BIGINT NULL,
		LReads__LastSeenPages	BIGINT NULL,
		LReads__MinPages	BIGINT NULL,
		LReads__MaxPages	BIGINT NULL,
		--calculated fields:
		--LReads__FirstLastDeltaPages
		--LReads__MinMaxDeltaPages

		PReads__FirstSeenPages	BIGINT NULL,
		PReads__LastSeenPages	BIGINT NULL,
		PReads__MinPages	BIGINT NULL,
		PReads__MaxPages	BIGINT NULL,
		--calculated fields:
		--PReads__FirstLastDeltaPages
		--PReads__MinMaxDeltaPages

		Writes__FirstSeenPages	BIGINT NULL,
		Writes__LastSeenPages	BIGINT NULL,
		Writes__MinPages	BIGINT NULL,
		Writes__MaxPages	BIGINT NULL
		--calculated fields:
		--Writes__FirstLastDeltaPages
		--Writes__MinMaxDeltaPages
	*/
	SET @lv__CalcSubQuery = N'
		SELECT 
			--ordering fields
			[BatchOrderBy] =		s.BatchIdentifier, 
			[StmtStartOrderBy] =	FirstSeen, 
			[TieBreaker] =			2,

			--Visible fields
			[SPID] =				N'''', 
			[#Seen] =				CONVERT(NVARCHAR(20), [#Seen]),
			[FirstSeen] =			CONVERT(NVARCHAR(20),CONVERT(TIME(0),FirstSeen)), 
			[LastSeen] =			CONVERT(NVARCHAR(20),CONVERT(TIME(0),LastSeen)),
			[Extent(sec)] =			CONVERT(NVARCHAR(20),DATEDIFF(second, FirstSeen, LastSeen)),
			[DBObject] =			CASE 
										WHEN sss.dbid = 32767 OR sss.dbid = -929 
											THEN N''''
										ELSE ISNULL(sss.dbname,N''<null>'') + N''.'' 
										END + 

										ISNULL(sss.schname,N''<null>'') + N''.'' +
										CASE 
											WHEN sss.objectid = -929 
												THEN N''''
										ELSE ISNULL(sss.objname,N''<null>'') 
										END,
			[Cmd] =					sss.stmt_xml,
			[StatusCodes] =			ISNULL(s.StatusCodeAgg,N''<null>''), 
			[NonCXWaits] =			ISNULL(s.Waits,N''''),
			[CXWaits] =				ISNULL(s.CXWaits,N''''),' + 

			CASE WHEN @attr=N'n' AND @plan = N'n' THEN N'' 
				ELSE (CASE WHEN @plan=N'n' THEN N'PNI = N'''','
						ELSE N'PNI = qp.query_plan_xml,'
						END)
				END + N'
			
			--metrics that are not sensitive to @units param
			s.tasks_FirstSeen,
			s.tasks_LastSeen,
			s.tasks_MinSeen,
			s.tasks_MaxSeen,
			s.tasks_AvgSeen,
			[tasks_CASE] = CASE WHEN [#Seen] = 1 OR s.tasks_MinSeen = s.tasks_MaxSeen THEN 0 ELSE 3 END,

			s.DOP_FirstSeen,	
			s.DOP_LastSeen,
			s.DOP_MinSeen,
			s.DOP_MaxSeen,
			s.DOP_AvgSeen,
			[DOP_CASE] = CASE WHEN [#Seen] = 1 OR s.DOP_MinSeen = s.DOP_MaxSeen THEN 0 ELSE 3 END,

			[CPUused__FirstSeen] =	CONVERT(MONEY,   s.CPUused__FirstSeenMs   ),
			[CPUused__LastSeen] =	CONVERT(MONEY,   s.CPUused__LastSeenMs   ),
			[CPUused__FLDelta] =	CONVERT(MONEY,   (s.CPUused__LastSeenMs - s.CPUused__FirstSeenMs)   ),
			[CPUused__Min] =		CONVERT(MONEY,   s.CPUused__minMs   ),
			[CPUused__Max] =		CONVERT(MONEY,   s.CPUused__maxMs   ),
			[CPUused__MMDelta] =	CONVERT(MONEY,   (s.CPUused__maxMs - s.CPUused__minMs)   ),
			[CPUused_CASE] =		CASE WHEN [#Seen] = 1 OR s.CPUused__minMs = s.CPUused__maxMs 
											THEN 0 
										WHEN s.CPUused__FirstSeenMs = s.CPUused__minMs
											AND s.CPUused__LastSeenMs = s.CPUused__maxMs
											THEN 1
										ELSE 2 END,';


	IF @units = N'mb'
	BEGIN
		SET @lv__CalcSubQuery = @lv__CalcSubQuery + N'
			--these metrics can vary up or down as queries progress
			[tempdb_task__FirstSeen] =	CONVERT(MONEY,   s.tempdb_task__FirstSeenPages*8./1024.   ),
			[tempdb_task__LastSeen] =	CONVERT(MONEY,   s.tempdb_task__LastSeenPages*8./1024.   ),
			[tempdb_task__FLDelta] =	CONVERT(MONEY,   (s.tempdb_task__LastSeenPages - s.tempdb_task__FirstSeenPages)*8./1024.   ),
			[tempdb_task__Min] =		CONVERT(MONEY,   s.tempdb_task__MinPages*8./1024.   ),
			[tempdb_task__Max] =		CONVERT(MONEY,   s.tempdb_task__MaxPages*8./1024.   ),
			[tempdb_task__MMDelta] =	CONVERT(MONEY,   (s.tempdb_task__MaxPages - s.tempdb_task__MinPages)*8./1024.   ),
			[tempdb_task__Avg] =		CONVERT(MONEY,   s.tempdb_task__AvgPages*8./1024.   ),
			[tempdb_task_CASE] =		CASE WHEN [#Seen] = 1 OR s.tempdb_task__MinPages = s.tempdb_task__MaxPages THEN 0 ELSE 3 END,

			[tempdb_sess__FirstSeen] =	CONVERT(MONEY,   s.tempdb_sess__FirstSeenPages*8./1024.   ),
			[tempdb_sess__LastSeen] =	CONVERT(MONEY,   s.tempdb_sess__LastSeenPages*8./1024.   ),
			[tempdb_sess__FLDelta] =	CONVERT(MONEY,   (s.tempdb_sess__LastSeenPages - s.tempdb_sess__FirstSeenPages)*8./1024.   ),
			[tempdb_sess__Min] =		CONVERT(MONEY,   s.tempdb_sess__MinPages*8./1024.   ),
			[tempdb_sess__Max] =		CONVERT(MONEY,   s.tempdb_sess__MaxPages*8./1024.   ),
			[tempdb_sess__MMDelta] =	CONVERT(MONEY,   (s.tempdb_sess__MaxPages - s.tempdb_sess__MinPages)*8./1024.   ),
			[tempdb_sess__Avg] =		CONVERT(MONEY,   s.tempdb_sess__AvgPages*8./1024.   ),
			[tempdb_sess_CASE] =		CASE WHEN [#Seen] = 1 OR s.tempdb_sess__MinPages = s.tempdb_sess__MaxPages THEN 0 ELSE 3 END,

			[qmem_requested__FirstSeen] =	CONVERT(MONEY,   s.qmem_requested__FirstSeenKB/1024.   ),
			[qmem_requested__LastSeen] =	CONVERT(MONEY,   s.qmem_requested__LastSeenKB/1024.   ),
			[qmem_requested__Min] =		CONVERT(MONEY,   s.qmem_requested__MinKB/1024.   ),
			[qmem_requested__Max] =		CONVERT(MONEY,   s.qmem_requested__MaxKB/1024.   ),
			[qmem_requested__MMDelta] =	CONVERT(MONEY,   (s.qmem_requested__MaxKB - s.qmem_requested__MinKB)/1024.   ),
			[qmem_requested__Avg] =		CONVERT(MONEY,   s.qmem_requested__AvgKB/1024.   ),
			[qmem_requested_CASE] =		CASE WHEN [#Seen] = 1 OR s.qmem_requested__MinKB = s.qmem_requested__MaxKB THEN 0 ELSE 3 END,

			[qmem_granted__FirstSeen] =	CONVERT(MONEY,   s.qmem_granted__FirstSeenKB/1024.   ),
			[qmem_granted__LastSeen] =	CONVERT(MONEY,   s.qmem_granted__LastSeenKB/1024.   ),
			[qmem_granted__Min] =		CONVERT(MONEY,   s.qmem_granted__MinKB/1024.   ),
			[qmem_granted__Max] =		CONVERT(MONEY,   s.qmem_granted__MaxKB/1024.   ),
			[qmem_granted__MMDelta] =	CONVERT(MONEY,   (s.qmem_granted__MaxKB - s.qmem_granted__MinKB)/1024.   ),
			[qmem_granted__Avg] =		CONVERT(MONEY,   s.qmem_granted__AvgKB/1024.   ),
			[qmem_granted_CASE] =		CASE WHEN [#Seen] = 1 OR s.qmem_granted__MinKB = s.qmem_granted__MaxKB THEN 0 ELSE 3 END,

			[qmem_used__FirstSeen] =	CONVERT(MONEY,   s.qmem_used__FirstSeenKB/1024.   ),
			[qmem_used__LastSeen] =	CONVERT(MONEY,   s.qmem_used__LastSeenKB/1024.   ),
			[qmem_used__Min] =		CONVERT(MONEY,   s.qmem_used__MinKB/1024.   ),
			[qmem_used__Max] =		CONVERT(MONEY,   s.qmem_used__MaxKB/1024.   ),
			[qmem_used__MMDelta] =	CONVERT(MONEY,   (s.qmem_used__MaxKB - s.qmem_used__MinKB)/1024.   ),
			[qmem_used__Avg] =		CONVERT(MONEY,   s.qmem_used__AvgKB/1024.   ),
			[qmem_used_CASE] =		CASE WHEN [#Seen] = 1 OR s.qmem_used__MinKB = s.qmem_used__MaxKB THEN 0 ELSE 3 END,

			--these metrics should be ever-increasing for a given request
			[TlogUsed__FirstSeen] =	CONVERT(MONEY,   s.TlogUsed__FirstSeenBytes/1024./1024.   ),
			[TlogUsed__LastSeen] =	CONVERT(MONEY,   s.TlogUsed__LastSeenBytes/1024./1024.   ),
			[TlogUsed__FLDelta] =	CONVERT(MONEY,   (s.TlogUsed__LastSeenBytes - s.TlogUsed__FirstSeenBytes)/1024./1024.    ),
			[TlogUsed__Min] =		CONVERT(MONEY,   s.TlogUsed__minBytes/1024./1024.   ),
			[TlogUsed__Max] =		CONVERT(MONEY,   s.TlogUsed__maxBytes/1024./1024.   ),
			[TlogUsed__MMDelta] =	CONVERT(MONEY,   (s.TlogUsed__maxBytes - s.TlogUsed__minBytes)/1024./1024.    ),
			[TlogUsed_CASE] =		CASE WHEN [#Seen] = 1 OR s.TlogUsed__minBytes = s.TlogUsed__maxBytes 
											THEN 0 
										WHEN s.TlogUsed__FirstSeenBytes = s.TlogUsed__minBytes
											AND s.TlogUsed__LastSeenBytes = s.TlogUsed__maxBytes
											THEN 1
										ELSE 2 END,

			[LReads__FirstSeen] =	CONVERT(MONEY,   s.LReads__FirstSeenPages*8./1024.   ),
			[LReads__LastSeen] =	CONVERT(MONEY,   s.LReads__LastSeenPages*8./1024.   ),
			[LReads__FLDelta] =	CONVERT(MONEY,   (s.LReads__LastSeenPages - s.LReads__FirstSeenPages)/8./1024.    ),
			[LReads__Min] =		CONVERT(MONEY,   s.LReads__MinPages*8./1024.   ),
			[LReads__Max] =		CONVERT(MONEY,   s.LReads__MaxPages*8./1024.   ),
			[LReads__MMDelta] =	CONVERT(MONEY,   (s.LReads__MaxPages - s.LReads__MinPages)/8./1024.    ),
			[LReads_CASE] =		CASE WHEN [#Seen] = 1 OR s.LReads__MinPages = s.LReads__MaxPages
											THEN 0 
										WHEN s.LReads__FirstSeenPages = s.LReads__MinPages
											AND s.LReads__LastSeenPages = s.LReads__MaxPages
											THEN 1
										ELSE 2 END,

			[PReads__FirstSeen] =	CONVERT(MONEY,   s.PReads__FirstSeenPages*8./1024.   ),
			[PReads__LastSeen] =	CONVERT(MONEY,   s.PReads__LastSeenPages*8./1024.   ),
			[PReads__FLDelta] =	CONVERT(MONEY,   (s.PReads__LastSeenPages - s.PReads__FirstSeenPages)*8./1024.    ),
			[PReads__Min] =		CONVERT(MONEY,   s.PReads__MinPages*8./1024.   ),
			[PReads__Max] =		CONVERT(MONEY,   s.PReads__MaxPages*8./1024.   ),
			[PReads__MMDelta] =	CONVERT(MONEY,   (s.PReads__MaxPages - s.PReads__MinPages)*8./1024.    ),
			[PReads_CASE] =		CASE WHEN [#Seen] = 1 OR s.PReads__MinPages = s.PReads__MaxPages
											THEN 0 
										WHEN s.PReads__FirstSeenPages = s.PReads__MinPages
											AND s.PReads__LastSeenPages = s.PReads__MaxPages
											THEN 1
										ELSE 2 END,

			[Writes__FirstSeen] =	CONVERT(MONEY,   s.Writes__FirstSeenPages*8./1024.   ),
			[Writes__LastSeen] =	CONVERT(MONEY,   s.Writes__LastSeenPages*8./1024.   ),
			[Writes__FLDelta] =	CONVERT(MONEY,   (s.Writes__LastSeenPages - s.Writes__FirstSeenPages)*8./1024.    ),
			[Writes__Min] =		CONVERT(MONEY,   s.Writes__MinPages*8./1024.   ),
			[Writes__Max] =		CONVERT(MONEY,   s.Writes__MaxPages*8./1024.   ),
			[Writes__MMDelta] =	CONVERT(MONEY,   (s.Writes__MaxPages - s.Writes__MinPages)*8./1024.    ),
			[Writes_CASE] =		CASE WHEN [#Seen] = 1 OR s.Writes__MinPages = s.Writes__MaxPages
											THEN 0 
										WHEN s.Writes__FirstSeenPages = s.Writes__MinPages
											AND s.Writes__LastSeenPages = s.Writes__MaxPages
											THEN 1
										ELSE 2 END';
	END
	ELSE IF @units = N'native'
	BEGIN
		SET @lv__CalcSubQuery = @lv__CalcSubQuery + N'
			--these metrics can vary up or down as queries progress
			[tempdb_task__FirstSeen] =	CONVERT(MONEY,   s.tempdb_task__FirstSeenPages   ),
			[tempdb_task__LastSeen] =	CONVERT(MONEY,   s.tempdb_task__LastSeenPages   ),
			[tempdb_task__FLDelta] =	CONVERT(MONEY,   s.tempdb_task__LastSeenPages - s.tempdb_task__FirstSeenPages   ),
			[tempdb_task__Min] =		CONVERT(MONEY,   s.tempdb_task__MinPages   ),
			[tempdb_task__Max] =		CONVERT(MONEY,   s.tempdb_task__MaxPages   ),
			[tempdb_task__MMDelta] =	CONVERT(MONEY,   s.tempdb_task__MaxPages - s.tempdb_task__MinPages   ),
			[tempdb_task__Avg] =		CONVERT(MONEY,   s.tempdb_task__AvgPages   ),
			[tempdb_task_CASE] =		CASE WHEN [#Seen] = 1 OR s.tempdb_task__MinPages = s.tempdb_task__MaxPages THEN 0 ELSE 3 END,

			[tempdb_sess__FirstSeen] =	CONVERT(MONEY,   s.tempdb_sess__FirstSeenPages   ),
			[tempdb_sess__LastSeen] =	CONVERT(MONEY,   s.tempdb_sess__LastSeenPages   ),
			[tempdb_sess__FLDelta] =	CONVERT(MONEY,   s.tempdb_sess__LastSeenPages - s.tempdb_sess__FirstSeenPages   ),
			[tempdb_sess__Min] =		CONVERT(MONEY,   s.tempdb_sess__MinPages   ),
			[tempdb_sess__Max] =		CONVERT(MONEY,   s.tempdb_sess__MaxPages   ),
			[tempdb_sess__MMDelta] =	CONVERT(MONEY,   s.tempdb_sess__MaxPages - s.tempdb_sess__MinPages   ),
			[tempdb_sess__Avg] =		CONVERT(MONEY,   s.tempdb_sess__AvgPages   ),
			[tempdb_sess_CASE] =		CASE WHEN [#Seen] = 1 OR s.tempdb_sess__MinPages = s.tempdb_sess__MaxPages THEN 0 ELSE 3 END,

			[qmem_requested__FirstSeen] =	CONVERT(MONEY,   s.qmem_requested__FirstSeenKB   ),
			[qmem_requested__LastSeen] =	CONVERT(MONEY,   s.qmem_requested__LastSeenKB   ),
			[qmem_requested__Min] =		CONVERT(MONEY,   s.qmem_requested__MinKB   ),
			[qmem_requested__Max] =		CONVERT(MONEY,   s.qmem_requested__MaxKB   ),
			[qmem_requested__MMDelta] =	CONVERT(MONEY,   (s.qmem_requested__MaxKB - s.qmem_requested__MinKB)   ),
			[qmem_requested__Avg] =		CONVERT(MONEY,   s.qmem_requested__AvgKB   ),
			[qmem_requested_CASE] =		CASE WHEN [#Seen] = 1 OR s.qmem_requested__MinKB = s.qmem_requested__MaxKB THEN 0 ELSE 3 END,

			[qmem_granted__FirstSeen] =	CONVERT(MONEY,   s.qmem_granted__FirstSeenKB   ),
			[qmem_granted__LastSeen] =	CONVERT(MONEY,   s.qmem_granted__LastSeenKB   ),
			[qmem_granted__Min] =		CONVERT(MONEY,   s.qmem_granted__MinKB   ),
			[qmem_granted__Max] =		CONVERT(MONEY,   s.qmem_granted__MaxKB   ),
			[qmem_granted__MMDelta] =	CONVERT(MONEY,   (s.qmem_granted__MaxKB - s.qmem_granted__MinKB)   ),
			[qmem_granted__Avg] =		CONVERT(MONEY,   s.qmem_granted__AvgKB   ),
			[qmem_granted_CASE] =		CASE WHEN [#Seen] = 1 OR s.qmem_granted__MinKB = s.qmem_granted__MaxKB THEN 0 ELSE 3 END,

			[qmem_used__FirstSeen] =	CONVERT(MONEY,   s.qmem_used__FirstSeenKB   ),
			[qmem_used__LastSeen] =	CONVERT(MONEY,   s.qmem_used__LastSeenKB   ),
			[qmem_used__Min] =		CONVERT(MONEY,   s.qmem_used__MinKB   ),
			[qmem_used__Max] =		CONVERT(MONEY,   s.qmem_used__MaxKB   ),
			[qmem_used__MMDelta] =	CONVERT(MONEY,   (s.qmem_used__MaxKB - s.qmem_used__MinKB)   ),
			[qmem_used__Avg] =		CONVERT(MONEY,   s.qmem_used__AvgKB   ),
			[qmem_used_CASE] =		CASE WHEN [#Seen] = 1 OR s.qmem_used__MinKB = s.qmem_used__MaxKB THEN 0 ELSE 3 END,

			--these metrics should be ever-increasing for a given request
			[TlogUsed__FirstSeen] =	CONVERT(MONEY,   s.TlogUsed__FirstSeenBytes   ),
			[TlogUsed__LastSeen] =	CONVERT(MONEY,   s.TlogUsed__LastSeenBytes   ),
			[TlogUsed__FLDelta] =	CONVERT(MONEY,   (s.TlogUsed__LastSeenBytes - s.TlogUsed__FirstSeenBytes)    ),
			[TlogUsed__Min] =		CONVERT(MONEY,   s.TlogUsed__minBytes   ),
			[TlogUsed__Max] =		CONVERT(MONEY,   s.TlogUsed__maxBytes   ),
			[TlogUsed__MMDelta] =	CONVERT(MONEY,   (s.TlogUsed__maxBytes - s.TlogUsed__minBytes)    ),
			[TlogUsed_CASE] =		CASE WHEN [#Seen] = 1 OR s.TlogUsed__minBytes = s.TlogUsed__maxBytes 
											THEN 0 
										WHEN s.TlogUsed__FirstSeenBytes = s.TlogUsed__minBytes
											AND s.TlogUsed__LastSeenBytes = s.TlogUsed__maxBytes
											THEN 1
										ELSE 2 END,

			[LReads__FirstSeen] =	CONVERT(MONEY,   s.LReads__FirstSeenPages   ),
			[LReads__LastSeen] =	CONVERT(MONEY,   s.LReads__LastSeenPages   ),
			[LReads__FLDelta] =	CONVERT(MONEY,   (s.LReads__LastSeenPages - s.LReads__FirstSeenPages)    ),
			[LReads__Min] =		CONVERT(MONEY,   s.LReads__MinPages   ),
			[LReads__Max] =		CONVERT(MONEY,   s.LReads__MaxPages   ),
			[LReads__MMDelta] =	CONVERT(MONEY,   (s.LReads__MaxPages - s.LReads__MinPages)    ),
			[LReads_CASE] =		CASE WHEN [#Seen] = 1 OR s.LReads__MinPages = s.LReads__MaxPages
											THEN 0 
										WHEN s.LReads__FirstSeenPages = s.LReads__MinPages
											AND s.LReads__LastSeenPages = s.LReads__MaxPages
											THEN 1
										ELSE 2 END,

			[PReads__FirstSeen] =	CONVERT(MONEY,   s.PReads__FirstSeenPages   ),
			[PReads__LastSeen] =	CONVERT(MONEY,   s.PReads__LastSeenPages   ),
			[PReads__FLDelta] =	CONVERT(MONEY,   (s.PReads__LastSeenPages - s.PReads__FirstSeenPages)    ),
			[PReads__Min] =		CONVERT(MONEY,   s.PReads__MinPages   ),
			[PReads__Max] =		CONVERT(MONEY,   s.PReads__MaxPages   ),
			[PReads__MMDelta] =	CONVERT(MONEY,   (s.PReads__MaxPages - s.PReads__MinPages)    ),
			[PReads_CASE] =		CASE WHEN [#Seen] = 1 OR s.PReads__MinPages = s.PReads__MaxPages
											THEN 0 
										WHEN s.PReads__FirstSeenPages = s.PReads__MinPages
											AND s.PReads__LastSeenPages = s.PReads__MaxPages
											THEN 1
										ELSE 2 END,

			[Writes__FirstSeen] =	CONVERT(MONEY,   s.Writes__FirstSeenPages   ),
			[Writes__LastSeen] =	CONVERT(MONEY,   s.Writes__LastSeenPages   ),
			[Writes__FLDelta] =	CONVERT(MONEY,   (s.Writes__LastSeenPages - s.Writes__FirstSeenPages)    ),
			[Writes__Min] =		CONVERT(MONEY,   s.Writes__MinPages   ),
			[Writes__Max] =		CONVERT(MONEY,   s.Writes__MaxPages   ),
			[Writes__MMDelta] =	CONVERT(MONEY,   (s.Writes__MaxPages - s.Writes__MinPages)    ),
			[Writes_CASE] =		CASE WHEN [#Seen] = 1 OR s.Writes__MinPages = s.Writes__MaxPages
											THEN 0 
										WHEN s.Writes__FirstSeenPages = s.Writes__MinPages
											AND s.Writes__LastSeenPages = s.Writes__MaxPages
											THEN 1
										ELSE 2 END';
	END
	ELSE IF @units = N'pages'
	BEGIN
		SET @lv__CalcSubQuery = @lv__CalcSubQuery + N'
			--these metrics can vary up or down as queries progress
			[tempdb_task__FirstSeen] =	CONVERT(MONEY,   s.tempdb_task__FirstSeenPages   ),
			[tempdb_task__LastSeen] =	CONVERT(MONEY,   s.tempdb_task__LastSeenPages   ),
			[tempdb_task__FLDelta] =	CONVERT(MONEY,   s.tempdb_task__LastSeenPages - s.tempdb_task__FirstSeenPages   ),
			[tempdb_task__Min] =		CONVERT(MONEY,   s.tempdb_task__MinPages   ),
			[tempdb_task__Max] =		CONVERT(MONEY,   s.tempdb_task__MaxPages   ),
			[tempdb_task__MMDelta] =	CONVERT(MONEY,   s.tempdb_task__MaxPages - s.tempdb_task__MinPages   ),
			[tempdb_task__Avg] =		CONVERT(MONEY,   s.tempdb_task__AvgPages   ),
			[tempdb_task_CASE] =		CASE WHEN [#Seen] = 1 OR s.tempdb_task__MinPages = s.tempdb_task__MaxPages THEN 0 ELSE 3 END,

			[tempdb_sess__FirstSeen] =	CONVERT(MONEY,   s.tempdb_sess__FirstSeenPages   ),
			[tempdb_sess__LastSeen] =	CONVERT(MONEY,   s.tempdb_sess__LastSeenPages   ),
			[tempdb_sess__FLDelta] =	CONVERT(MONEY,   s.tempdb_sess__LastSeenPages - s.tempdb_sess__FirstSeenPages   ),
			[tempdb_sess__Min] =		CONVERT(MONEY,   s.tempdb_sess__MinPages   ),
			[tempdb_sess__Max] =		CONVERT(MONEY,   s.tempdb_sess__MaxPages   ),
			[tempdb_sess__MMDelta] =	CONVERT(MONEY,   s.tempdb_sess__MaxPages - s.tempdb_sess__MinPages   ),
			[tempdb_sess__Avg] =		CONVERT(MONEY,   s.tempdb_sess__AvgPages   ),
			[tempdb_sess_CASE] =		CASE WHEN [#Seen] = 1 OR s.tempdb_sess__MinPages = s.tempdb_sess__MaxPages THEN 0 ELSE 3 END,

			[qmem_requested__FirstSeen] =	CONVERT(MONEY,   s.qmem_requested__FirstSeenKB/8.   ),
			[qmem_requested__LastSeen] =	CONVERT(MONEY,   s.qmem_requested__LastSeenKB/8.   ),
			[qmem_requested__Min] =		CONVERT(MONEY,   s.qmem_requested__MinKB/8.   ),
			[qmem_requested__Max] =		CONVERT(MONEY,   s.qmem_requested__MaxKB/8.   ),
			[qmem_requested__MMDelta] =	CONVERT(MONEY,   (s.qmem_requested__MaxKB - s.qmem_requested__MinKB)/8.   ),
			[qmem_requested__Avg] =		CONVERT(MONEY,   s.qmem_requested__AvgKB/8.   ),
			[qmem_requested_CASE] =		CASE WHEN [#Seen] = 1 OR s.qmem_requested__MinKB = s.qmem_requested__MaxKB THEN 0 ELSE 3 END,

			[qmem_granted__FirstSeen] =	CONVERT(MONEY,   s.qmem_granted__FirstSeenKB/8.   ),
			[qmem_granted__LastSeen] =	CONVERT(MONEY,   s.qmem_granted__LastSeenKB/8.   ),
			[qmem_granted__Min] =		CONVERT(MONEY,   s.qmem_granted__MinKB/8.   ),
			[qmem_granted__Max] =		CONVERT(MONEY,   s.qmem_granted__MaxKB/8.   ),
			[qmem_granted__MMDelta] =	CONVERT(MONEY,   (s.qmem_granted__MaxKB - s.qmem_granted__MinKB)/8.   ),
			[qmem_granted__Avg] =		CONVERT(MONEY,   s.qmem_granted__AvgKB/8.   ),
			[qmem_granted_CASE] =		CASE WHEN [#Seen] = 1 OR s.qmem_granted__MinKB = s.qmem_granted__MaxKB THEN 0 ELSE 3 END,

			[qmem_used__FirstSeen] =	CONVERT(MONEY,   s.qmem_used__FirstSeenKB/8.   ),
			[qmem_used__LastSeen] =	CONVERT(MONEY,   s.qmem_used__LastSeenKB/8.   ),
			[qmem_used__Min] =		CONVERT(MONEY,   s.qmem_used__MinKB/8.   ),
			[qmem_used__Max] =		CONVERT(MONEY,   s.qmem_used__MaxKB/8.   ),
			[qmem_used__MMDelta] =	CONVERT(MONEY,   (s.qmem_used__MaxKB - s.qmem_used__MinKB)/8.   ),
			[qmem_used__Avg] =		CONVERT(MONEY,   s.qmem_used__AvgKB/8.   ),
			[qmem_used_CASE] =		CASE WHEN [#Seen] = 1 OR s.qmem_used__MinKB = s.qmem_used__MaxKB THEN 0 ELSE 3 END,

			--these metrics should be ever-increasing for a given request
			[TlogUsed__FirstSeen] =	CONVERT(MONEY,   s.TlogUsed__FirstSeenBytes/8192.   ),
			[TlogUsed__LastSeen] =	CONVERT(MONEY,   s.TlogUsed__LastSeenBytes/8192.   ),
			[TlogUsed__FLDelta] =	CONVERT(MONEY,   (s.TlogUsed__LastSeenBytes - s.TlogUsed__FirstSeenBytes)/8192.    ),
			[TlogUsed__Min] =		CONVERT(MONEY,   s.TlogUsed__minBytes/8192.   ),
			[TlogUsed__Max] =		CONVERT(MONEY,   s.TlogUsed__maxBytes/8192.   ),
			[TlogUsed__MMDelta] =	CONVERT(MONEY,   (s.TlogUsed__maxBytes - s.TlogUsed__minBytes)/8192.    ),
			[TlogUsed_CASE] =		CASE WHEN [#Seen] = 1 OR s.TlogUsed__minBytes = s.TlogUsed__maxBytes 
											THEN 0 
										WHEN s.TlogUsed__FirstSeenBytes = s.TlogUsed__minBytes
											AND s.TlogUsed__LastSeenBytes = s.TlogUsed__maxBytes
											THEN 1
										ELSE 2 END,

			[LReads__FirstSeen] =	CONVERT(MONEY,   s.LReads__FirstSeenPages   ),
			[LReads__LastSeen] =	CONVERT(MONEY,   s.LReads__LastSeenPages   ),
			[LReads__FLDelta] =	CONVERT(MONEY,   (s.LReads__LastSeenPages - s.LReads__FirstSeenPages)    ),
			[LReads__Min] =		CONVERT(MONEY,   s.LReads__MinPages   ),
			[LReads__Max] =		CONVERT(MONEY,   s.LReads__MaxPages   ),
			[LReads__MMDelta] =	CONVERT(MONEY,   (s.LReads__MaxPages - s.LReads__MinPages)    ),
			[LReads_CASE] =		CASE WHEN [#Seen] = 1 OR s.LReads__MinPages = s.LReads__MaxPages
											THEN 0 
										WHEN s.LReads__FirstSeenPages = s.LReads__MinPages
											AND s.LReads__LastSeenPages = s.LReads__MaxPages
											THEN 1
										ELSE 2 END,

			[PReads__FirstSeen] =	CONVERT(MONEY,   s.PReads__FirstSeenPages   ),
			[PReads__LastSeen] =	CONVERT(MONEY,   s.PReads__LastSeenPages   ),
			[PReads__FLDelta] =	CONVERT(MONEY,   (s.PReads__LastSeenPages - s.PReads__FirstSeenPages)    ),
			[PReads__Min] =		CONVERT(MONEY,   s.PReads__MinPages   ),
			[PReads__Max] =		CONVERT(MONEY,   s.PReads__MaxPages   ),
			[PReads__MMDelta] =	CONVERT(MONEY,   (s.PReads__MaxPages - s.PReads__MinPages)    ),
			[PReads_CASE] =		CASE WHEN [#Seen] = 1 OR s.PReads__MinPages = s.PReads__MaxPages
											THEN 0 
										WHEN s.PReads__FirstSeenPages = s.PReads__MinPages
											AND s.PReads__LastSeenPages = s.PReads__MaxPages
											THEN 1
										ELSE 2 END,

			[Writes__FirstSeen] =	CONVERT(MONEY,   s.Writes__FirstSeenPages   ),
			[Writes__LastSeen] =	CONVERT(MONEY,   s.Writes__LastSeenPages   ),
			[Writes__FLDelta] =	CONVERT(MONEY,   (s.Writes__LastSeenPages - s.Writes__FirstSeenPages)    ),
			[Writes__Min] =		CONVERT(MONEY,   s.Writes__MinPages   ),
			[Writes__Max] =		CONVERT(MONEY,   s.Writes__MaxPages   ),
			[Writes__MMDelta] =	CONVERT(MONEY,   (s.Writes__MaxPages - s.Writes__MinPages)    ),
			[Writes_CASE] =		CASE WHEN [#Seen] = 1 OR s.Writes__MinPages = s.Writes__MaxPages
											THEN 0 
										WHEN s.Writes__FirstSeenPages = s.Writes__MinPages
											AND s.Writes__LastSeenPages = s.Writes__MaxPages
											THEN 1
										ELSE 2 END';
	END	--end of @units conditional calculation block

	SET @lv__CalcSubQuery = @lv__CalcSubQuery + N'
		FROM #stmtstats s
			INNER JOIN #SQLStmtStore sss
				ON s.FKSQLStmtStoreID = sss.PKSQLStmtStoreID
			' + CASE WHEN @plan=N'n' THEN N''
				ELSE N'LEFT OUTER JOIN #QueryPlanStmtStore qp 
					ON qp.PKQueryPlanStmtStoreID = s.FKQueryPlanStmtStoreID' 
				END + N'
	';

	/* For debugging: 
		SELECT dyntxt, TxtLink
		from (SELECT @lv__CalcSubQuery AS dyntxt) t0
			cross apply (select TxtLink=(select [processing-instruction(q)]=dyntxt
                            for xml path(''),type)) F2
	*/
		SELECT dyntxt, TxtLink
		from (SELECT @lv__CalcSubQuery AS dyntxt) t0
			cross apply (select TxtLink=(select [processing-instruction(q)]=dyntxt
                            for xml path(''),type)) F2
	RETURN 0;

	EXEC sp_executesql @lv__FormatSubQuery;
	RETURN 0;

	SET @lv__FormatSubQuery = N'
	SELECT 
		BatchOrderBy,
		StmtStartOrderBy,
		TieBreaker,
		SPID, 
		[#Seen], 
		FirstSeen,
		LastSeen,
		[Extent(sec)],
		[DBObject],
		[Cmd], 
		[StatusCodes],
		[NonCXWaits],
		[CXWaits],' + 
		CASE WHEN @attr=N'n' AND @plan = N'n' THEN N'' 
			ELSE N'PNI,'
			END + N'
		[Tasks_First] = CONVERT(NVARCHAR(20), tasks_FirstSeen),
		[Tasks_Last] = CASE WHEN tasks_CASE = 0 THEN N'''' ELSE CONVERT(NVARCHAR(20), tasks_LastSeen) END,
		[Tasks_Min] =  CASE WHEN tasks_CASE = 0 THEN N'''' ELSE CONVERT(NVARCHAR(20), tasks_MinSeen) END,
		[Tasks_Max] =  CASE WHEN tasks_CASE = 0 THEN N'''' ELSE CONVERT(NVARCHAR(20), tasks_MaxSeen) END,
		[Tasks_Avg] =  CASE WHEN tasks_CASE = 0 THEN N'''' ELSE CONVERT(NVARCHAR(20), tasks_AvgSeen) END,

		[DOP_First] = CONVERT(NVARCHAR(20), DOP_FirstSeen),
		[DOP_Last] = CASE WHEN DOP_CASE = 0 THEN N'''' ELSE CONVERT(NVARCHAR(20), DOP_LastSeen) END,
		[DOP_Min] =  CASE WHEN DOP_CASE = 0 THEN N'''' ELSE CONVERT(NVARCHAR(20), DOP_MinSeen) END,
		[DOP_Max] =  CASE WHEN DOP_CASE = 0 THEN N'''' ELSE CONVERT(NVARCHAR(20), DOP_MaxSeen) END,
		[DOP_Avg] =  CASE WHEN DOP_CASE = 0 THEN N'''' ELSE CONVERT(NVARCHAR(20), DOP_AvgSeen) END,

		[CPUused_FirstSeen] = REPLACE(CONVERT(NVARCHAR(20),CPUused__FirstSeen,1),N''.00'',N''''),
		[CPUused_LastSeen] = CASE WHEN CPUused_CASE = 0 THEN N'''' 
								ELSE REPLACE(CONVERT(NVARCHAR(20),CPUused__LastSeen,1),N''.00'',N'''')
								END,
		[CPUused_FLDelta] = CASE WHEN CPUused_CASE = 0 THEN N'''' ELSE REPLACE(CONVERT(NVARCHAR(20),CPUused__FLDelta,1),N''.00'',N'''') END,
		[CPUused_Min] = CASE WHEN CPUused_CASE IN (0,1) THEN N'''' 
								ELSE REPLACE(CONVERT(NVARCHAR(20),CPUused__Min,1),N''.00'',N'''')
								END,
		[CPUused_Max] = CASE WHEN CPUused_CASE IN (0,1) THEN N'''' 
								ELSE REPLACE(CONVERT(NVARCHAR(20),CPUused__Max,1),N''.00'',N'''')
								END,
		[CPUused_MMDelta] = CASE WHEN CPUused_CASE IN (0,1) THEN N'''' 
								ELSE REPLACE(CONVERT(NVARCHAR(20),CPUused__MMDelta,1),N''.00'',N'''')
								END,

		[tempdb_task__FirstSeen] = ' + 
						CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_task__FirstSeen,1),N''.00'',N'''')'
							ELSE N'CONVERT(NVARCHAR(20),tempdb_task__FirstSeen,1)'
							END + N',
		[tempdb_task__LastSeen] = CASE WHEN tempdb_task_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_task__LastSeen,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),tempdb_task__LastSeen,1)'
								END + N' 
									END,
		[tempdb_task__FLDelta] = CASE WHEN tempdb_task_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_task__FLDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),tempdb_task__FLDelta,1)'
								END + N' 
									END,
		[tempdb_task__Min] = CASE WHEN tempdb_task_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_task__Min,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),tempdb_task__Min,1)'
								END + N' 
									END,
		[tempdb_task__Max] = CASE WHEN tempdb_task_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_task__Max,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),tempdb_task__Max,1)'
								END + N' 
									END,
		[tempdb_task__MMDelta] = CASE WHEN tempdb_task_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_task__MMDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),tempdb_task__MMDelta,1)'
								END + N' 
									END,
		[tempdb_task__Avg] = CASE WHEN tempdb_task_CASE = 0 THEN N''''
									ELSE CONVERT(NVARCHAR(20),tempdb_task__Avg,1)
									END,

		[tempdb_sess__FirstSeen] = ' + 
						CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_sess__FirstSeen,1),N''.00'',N'''')'
							ELSE N'CONVERT(NVARCHAR(20),tempdb_sess__FirstSeen,1)'
							END + N',
		[tempdb_sess__LastSeen] = CASE WHEN tempdb_sess_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_sess__LastSeen,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),tempdb_sess__LastSeen,1)'
								END + N' 
									END,
		[tempdb_sess__FLDelta] = CASE WHEN tempdb_sess_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_sess__FLDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),tempdb_sess__FLDelta,1)'
								END + N' 
									END,
		[tempdb_sess__Min] = CASE WHEN tempdb_sess_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_sess__Min,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),tempdb_sess__Min,1)'
								END + N' 
									END,
		[tempdb_sess__Max] = CASE WHEN tempdb_sess_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_sess__Max,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),tempdb_sess__Max,1)'
								END + N' 
									END,
		[tempdb_sess__MMDelta] = CASE WHEN tempdb_sess_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'pages', N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),tempdb_sess__MMDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),tempdb_sess__MMDelta,1)'
								END + N' 
									END,
		[tempdb_sess__Avg] = CASE WHEN tempdb_sess_CASE = 0 THEN N''''
									ELSE CONVERT(NVARCHAR(20),tempdb_sess__Avg,1)
									END,
									';

	SET @lv__FormatSubQuery = @lv__FormatSubQuery + N'
		[qmem_requested__FirstSeen] = ' + 
						CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_requested__FirstSeen,1),N''.00'',N'''')'
							ELSE N'CONVERT(NVARCHAR(20),qmem_requested__FirstSeen,1)'
							END + N',
		[qmem_requested__LastSeen] = CASE WHEN qmem_requested_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_requested__LastSeen,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_requested__LastSeen,1)'
								END + N' 
									END,
		[qmem_requested__Min] = CASE WHEN qmem_requested_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_requested__Min,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_requested__Min,1)'
								END + N' 
									END,
		[qmem_requested__Max] = CASE WHEN qmem_requested_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_requested__Max,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_requested__Max,1)'
								END + N' 
									END,
		[qmem_requested__MMDelta] = CASE WHEN qmem_requested_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_requested__MMDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_requested__MMDelta,1)'
								END + N' 
									END,
		[qmem_requested__Avg] = CASE WHEN qmem_requested_CASE = 0 THEN N''''
									ELSE CONVERT(NVARCHAR(20),qmem_requested__Avg,1)
									END,

		[qmem_granted__FirstSeen] = ' + 
						CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_granted__FirstSeen,1),N''.00'',N'''')'
							ELSE N'CONVERT(NVARCHAR(20),qmem_granted__FirstSeen,1)'
							END + N',
		[qmem_granted__LastSeen] = CASE WHEN qmem_granted_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_granted__LastSeen,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_granted__LastSeen,1)'
								END + N' 
									END,
		[qmem_granted__Min] = CASE WHEN qmem_granted_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_granted__Min,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_granted__Min,1)'
								END + N' 
									END,
		[qmem_granted__Max] = CASE WHEN qmem_granted_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_granted__Max,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_granted__Max,1)'
								END + N' 
									END,
		[qmem_granted__MMDelta] = CASE WHEN qmem_granted_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_granted__MMDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_granted__MMDelta,1)'
								END + N' 
									END,
		[qmem_granted__Avg] = CASE WHEN qmem_granted_CASE = 0 THEN N''''
									ELSE CONVERT(NVARCHAR(20),qmem_granted__Avg,1)
									END,

		[qmem_used__FirstSeen] = ' + 
						CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_used__FirstSeen,1),N''.00'',N'''')'
							ELSE N'CONVERT(NVARCHAR(20),qmem_used__FirstSeen,1)'
							END + N',
		[qmem_used__LastSeen] = CASE WHEN qmem_used_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_used__LastSeen,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_used__LastSeen,1)'
								END + N' 
									END,
		[qmem_used__Min] = CASE WHEN qmem_used_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_used__Min,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_used__Min,1)'
								END + N' 
									END,
		[qmem_used__Max] = CASE WHEN qmem_used_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_used__Max,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_used__Max,1)'
								END + N' 
									END,
		[qmem_used__MMDelta] = CASE WHEN qmem_used_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),qmem_used__MMDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),qmem_used__MMDelta,1)'
								END + N' 
									END,
		[qmem_used__Avg] = CASE WHEN qmem_used_CASE = 0 THEN N''''
									ELSE CONVERT(NVARCHAR(20),qmem_used__Avg,1)
									END,

		[TlogUsed_FirstSeen] = ' + 
						CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),TlogUsed_FirstSeen,1),N''.00'',N'''')'
							ELSE N'CONVERT(NVARCHAR(20),TlogUsed_FirstSeen,1)'
							END + N',
		[TlogUsed_LastSeen] = CASE WHEN TlogUsed_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),TlogUsed_LastSeen,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),TlogUsed_LastSeen,1)'
								END + N' 
									END,
		[TlogUsed_FLDelta] = CASE WHEN TlogUsed_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),TlogUsed_FLDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),TlogUsed_FLDelta,1)'
								END + N' 
									END,
		[TlogUsed_Min] = CASE WHEN TlogUsed_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),TlogUsed_Min,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),TlogUsed_Min,1)'
								END + N' 
									END,
		[TlogUsed_Max] = CASE WHEN TlogUsed_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),TlogUsed_Max,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),TlogUsed_Max,1)'
								END + N' 
									END,
		[TlogUsed_MMDelta] = CASE WHEN TlogUsed_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native') THEN N'REPLACE(CONVERT(NVARCHAR(20),TlogUsed_MMDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),TlogUsed_MMDelta,1)'
								END + N' 
									END,

		[LReads_FirstSeen] = ' + 
						CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),LReads_FirstSeen,1),N''.00'',N'''')'
							ELSE N'CONVERT(NVARCHAR(20),LReads_FirstSeen,1)'
							END + N',
		[LReads_LastSeen] = CASE WHEN LReads_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),LReads_LastSeen,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),LReads_LastSeen,1)'
								END + N' 
									END,
		[LReads_FLDelta] = CASE WHEN LReads_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),LReads_FLDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),LReads_FLDelta,1)'
								END + N' 
									END,
		[LReads_Min] = CASE WHEN LReads_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),LReads_Min,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),LReads_Min,1)'
								END + N' 
									END,
		[LReads_Max] = CASE WHEN LReads_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),LReads_Max,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),LReads_Max,1)'
								END + N' 
									END,
		[LReads_MMDelta] = CASE WHEN LReads_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),LReads_MMDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),LReads_MMDelta,1)'
								END + N' 
									END,';


	SET @lv__FormatSubQuery = @lv__FormatSubQuery + N'
		[PReads_FirstSeen] = ' + 
						CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),PReads_FirstSeen,1),N''.00'',N'''')'
							ELSE N'CONVERT(NVARCHAR(20),PReads_FirstSeen,1)'
							END + N',
		[PReads_LastSeen] = CASE WHEN PReads_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),PReads_LastSeen,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),PReads_LastSeen,1)'
								END + N' 
									END,
		[PReads_FLDelta] = CASE WHEN PReads_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),PReads_FLDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),PReads_FLDelta,1)'
								END + N' 
									END,
		[PReads_Min] = CASE WHEN PReads_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),PReads_Min,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),PReads_Min,1)'
								END + N' 
									END,
		[PReads_Max] = CASE WHEN PReads_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),PReads_Max,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),PReads_Max,1)'
								END + N' 
									END,
		[PReads_MMDelta] = CASE WHEN PReads_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),PReads_MMDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),PReads_MMDelta,1)'
								END + N' 
									END,

		[Writes_FirstSeen] = ' + 
						CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),Writes_FirstSeen,1),N''.00'',N'''')'
							ELSE N'CONVERT(NVARCHAR(20),Writes_FirstSeen,1)'
							END + N',
		[Writes_LastSeen] = CASE WHEN Writes_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),Writes_LastSeen,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),Writes_LastSeen,1)'
								END + N' 
									END,
		[Writes_FLDelta] = CASE WHEN Writes_CASE = 0 THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),Writes_FLDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),Writes_FLDelta,1)'
								END + N' 
									END,
		[Writes_Min] = CASE WHEN Writes_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),Writes_Min,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),Writes_Min,1)'
								END + N' 
									END,
		[Writes_Max] = CASE WHEN Writes_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),Writes_Max,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),Writes_Max,1)'
								END + N' 
									END,
		[Writes_MMDelta] = CASE WHEN Writes_CASE IN (0,1) THEN N''''
									ELSE ' + 
							CASE WHEN @units IN (N'native',N'pages') THEN N'REPLACE(CONVERT(NVARCHAR(20),Writes_MDelta,1),N''.00'',N'''')'
								ELSE N'CONVERT(NVARCHAR(20),Writes_MMDelta,1)'
								END + N' 
									END
	FROM (' + @lv__CalcSubQuery + N'
		) calc
	';

	/* For debugging: 
		SELECT dyntxt, TxtLink
		from (SELECT @lv__FormatSubQuery AS dyntxt) t0
			cross apply (select TxtLink=(select [processing-instruction(q)]=dyntxt
                            for xml path(''),type)) F2
	*/
		SELECT dyntxt, TxtLink
		from (SELECT @lv__FormatSubQuery AS dyntxt) t0
			cross apply (select TxtLink=(select [processing-instruction(q)]=dyntxt
                            for xml path(''),type)) F2
	RETURN 0;

	EXEC sp_executesql @lv__FormatSubQuery;
	RETURN 0;


	SET @lv__UnionSubQuery = N'
	SELECT 
		--ordering fields
		[BatchOrderBy] =		BatchIdentifier, 
		[StmtStartOrderBy] =	rqst__start_time, 
		[TieBreaker] =			1,

		--visible fields
		[SPID] =				CASE 
									WHEN request_id = 0 
										THEN CONVERT(NVARCHAR(20), session_id) 
									ELSE CONVERT(NVARCHAR(20), session_id) + N'':'' + CONVERT(NVARCHAR(20), request_id) 
								END,
		[#Seen] =				N'''',
		[FirstSeen] =			CONVERT(NVARCHAR(20),lb.rqst__start_time),
		[LastSeen] =			CONVERT(NVARCHAR(20),lb.LastSeen),
		[Extent(sec)] =			CONVERT(NVARCHAR(20),DATEDIFF(SECOND, rqst__start_time, LastSeen)),
		[DBObject] =			ISNULL(DB_NAME(StartingDBID),N''''),
		[Cmd] =					xapp1.inputbuffer_xml,
		[StatusCodes] =			N'''',
		[NonCXWaits] =			N'''',
		[CXWaits] =				N'''',
		' + CASE WHEN @attr=N'n' AND @plan = N'n' THEN N'' 
			ELSE (CASE WHEN @attr=N'n' THEN N'PNI = N'''','
					ELSE N'PNI = CONVERT(XML,lb.SessAttr),'
					END)
			END + N'

		[Tasks_First]=N'''',
		[Tasks_Last]=N'''',
		[Tasks_Min]=N'''',
		[Tasks_Max]=N'''',
		[Tasks_Avg]=N'''',

		[DOP_First]=N'''',
		[DOP_Last]=N'''',
		[DOP_Min]=N'''',
		[DOP_Max]=N'''',
		[DOP_Avg]=N'''',

		[CPUused_FirstSeen]=N'''',
		[CPUused_LastSeen]=N'''',
		[CPUused_FLDelta]=N'''',
		[CPUused_Min]=N'''',
		[CPUused_Max]=N'''',
		[CPUused_MMDelta]=N'''',

		[tempdb_task__FirstSeen]=N'''',
		[tempdb_task__LastSeen]=N'''',
		[tempdb_task__FLDelta]=N'''',
		[tempdb_task__Min]=N'''',
		[tempdb_task__Max]=N'''',
		[tempdb_task__MMDelta]=N'''',
		[tempdb_task__Avg]=N'''',

		[tempdb_sess__FirstSeen]=N'''',
		[tempdb_sess__LastSeen]=N'''',
		[tempdb_sess__FLDelta]=N'''',
		[tempdb_sess__Min]=N'''',
		[tempdb_sess__Max]=N'''',
		[tempdb_sess__MMDelta]=N'''',
		[tempdb_sess__Avg]=N'''',

		[qmem_requested__FirstSeen]=N'''',
		[qmem_requested__LastSeen]=N'''',
		[qmem_requested__Min]=N'''',
		[qmem_requested__Max]=N'''',
		[qmem_requested__MMDelta]=N'''',
		[qmem_requested__Avg]=N'''',

		[qmem_granted__FirstSeen]=N'''',
		[qmem_granted__LastSeen]=N'''',
		[qmem_granted__Min]=N'''',
		[qmem_granted__Max]=N'''',
		[qmem_granted__MMDelta]=N'''',
		[qmem_granted__Avg]=N'''',

		[qmem_used__FirstSeen]=N'''',
		[qmem_used__LastSeen]=N'''',
		[qmem_used__Min]=N'''',
		[qmem_used__Max]=N'''',
		[qmem_used__MMDelta]=N'''',
		[qmem_used__Avg]=N'''',

		[TlogUsed_FirstSeen]=N'''',
		[TlogUsed_LastSeen]=N'''',
		[TlogUsed_FLDelta]=N'''',
		[TlogUsed_Min]=N'''',
		[TlogUsed_Max]=N'''',
		[TlogUsed_MMDelta]=N'''',

		[LReads_FirstSeen]=N'''',
		[LReads_LastSeen]=N'''',
		[LReads_FLDelta]=N'''',
		[LReads_Min]=N'''',
		[LReads_Max]=N'''',
		[LReads_MMDelta]=N'''',

		[PReads_FirstSeen]=N'''',
		[PReads_LastSeen]=N'''',
		[PReads_FLDelta]=N'''',
		[PReads_Min]=N'''',
		[PReads_Max]=N'''',
		[PReads_MMDelta]=N'''',

		[Writes_FirstSeen]=N'''',
		[Writes_LastSeen]=N'''',
		[Writes_FLDelta]=N'''',
		[Writes_Min]=N'''',
		[Writes_Max]=N'''',
		[Writes_MMDelta]=N''''
		
	FROM #LongBatches lb
		OUTER APPLY (
			SELECT TOP 1 ib.inputbuffer_xml
			FROM #InputBufferStore ib
			WHERE ib.PKInputBufferStoreID = lb.FKInputBufferStore 
		) xapp1

	UNION ALL

	SELECT 
		--ordering fields
		BatchOrderBy,
		StmtStartOrderBy,
		TieBreaker,

		--Visible fields
		SPID, 
		[#Seen], 
		FirstSeen,
		LastSeen,
		[Extent(sec)],
		[DBObject],
		[Cmd], 
		[StatusCodes],
		[NonCXWaits],
		[CXWaits],' + 
		CASE WHEN @attr=N'n' AND @plan = N'n' THEN N'' 
			ELSE N'PNI,'
			END + N'
		[Tasks_First],
		[Tasks_Last],
		[Tasks_Min],
		[Tasks_Max],
		[Tasks_Avg],

		[DOP_First],
		[DOP_Last],
		[DOP_Min],
		[DOP_Max],
		[DOP_Avg],

		[CPUused_FirstSeen],
		[CPUused_LastSeen],
		[CPUused_FLDelta],
		[CPUused_Min],
		[CPUused_Max],
		[CPUused_MMDelta],

		[tempdb_task__FirstSeen],
		[tempdb_task__LastSeen],
		[tempdb_task__FLDelta],
		[tempdb_task__Min],
		[tempdb_task__Max],
		[tempdb_task__MMDelta],
		[tempdb_task__Avg],

		[tempdb_sess__FirstSeen],
		[tempdb_sess__LastSeen],
		[tempdb_sess__FLDelta],
		[tempdb_sess__Min],
		[tempdb_sess__Max],
		[tempdb_sess__MMDelta],
		[tempdb_sess__Avg],

		[qmem_requested__FirstSeen],
		[qmem_requested__LastSeen],
		[qmem_requested__Min],
		[qmem_requested__Max],
		[qmem_requested__MMDelta],
		[qmem_requested__Avg],

		[qmem_granted__FirstSeen],
		[qmem_granted__LastSeen],
		[qmem_granted__Min],
		[qmem_granted__Max],
		[qmem_granted__MMDelta],
		[qmem_granted__Avg],

		[qmem_used__FirstSeen],
		[qmem_used__LastSeen],
		[qmem_used__Min],
		[qmem_used__Max],
		[qmem_used__MMDelta],
		[qmem_used__Avg],

		[TlogUsed_FirstSeen],
		[TlogUsed_LastSeen],
		[TlogUsed_FLDelta],
		[TlogUsed_Min],
		[TlogUsed_Max],
		[TlogUsed_MMDelta],

		[LReads_FirstSeen],
		[LReads_LastSeen],
		[LReads_FLDelta],
		[LReads_Min],
		[LReads_Max],
		[LReads_MMDelta],

		[PReads_FirstSeen],
		[PReads_LastSeen],
		[PReads_FLDelta],
		[PReads_Min],
		[PReads_Max],
		[PReads_MMDelta],

		[Writes_FirstSeen],
		[Writes_LastSeen],
		[Writes_FLDelta],
		[Writes_Min],
		[Writes_Max],
		[Writes_MMDelta]
	FROM (' + @lv__FormatSubQuery + N'
	) fmt
	';

	/* For debugging: 
		SELECT dyntxt, TxtLink
		from (SELECT @lv__UnionSubQuery AS dyntxt) t0
			cross apply (select TxtLink=(select [processing-instruction(q)]=dyntxt
                            for xml path(''),type)) F2
	*/
		SELECT dyntxt, TxtLink
		from (SELECT @lv__UnionSubQuery AS dyntxt) t0
			cross apply (select TxtLink=(select [processing-instruction(q)]=dyntxt
                            for xml path(''),type)) F2
	RETURN 0;

	EXEC sp_executesql @lv__UnionSubQuery;
	RETURN 0;

	SET @lv__OuterSelect = N'
	SELECT 
		--ordering fields
		BatchOrderBy,
		StmtStartOrderBy,
		TieBreaker,

		--Visible fields
		SPID, 
		[#Seen], 
		FirstSeen,
		LastSeen,
		[Extent(sec)],
		[DBObject],
		[Cmd], 
		[StatusCodes],
		[NonCXWaits],
		[CXWaits],' + 
		CASE WHEN @attr=N'n' AND @plan = N'n' THEN N'' 
			ELSE N'PNI,'
			END + N'
		[Tasks_First],
		[Tasks_Last],
		[Tasks_Min],
		[Tasks_Max],
		[Tasks_Avg],

		[DOP_First],
		[DOP_Last],
		[DOP_Min],
		[DOP_Max],
		[DOP_Avg],

		[CPUused_FirstSeen],
		[CPUused_LastSeen],
		[CPUused_FLDelta],
		[CPUused_Min],
		[CPUused_Max],
		[CPUused_MMDelta],

		[tempdb_task__FirstSeen],
		[tempdb_task__LastSeen],
		[tempdb_task__FLDelta],
		[tempdb_task__Min],
		[tempdb_task__Max],
		[tempdb_task__MMDelta],
		[tempdb_task__Avg],

		[tempdb_sess__FirstSeen],
		[tempdb_sess__LastSeen],
		[tempdb_sess__FLDelta],
		[tempdb_sess__Min],
		[tempdb_sess__Max],
		[tempdb_sess__MMDelta],
		[tempdb_sess__Avg],

		[qmem_requested__FirstSeen],
		[qmem_requested__LastSeen],
		[qmem_requested__Min],
		[qmem_requested__Max],
		[qmem_requested__MMDelta],
		[qmem_requested__Avg],

		[qmem_granted__FirstSeen],
		[qmem_granted__LastSeen],
		[qmem_granted__Min],
		[qmem_granted__Max],
		[qmem_granted__MMDelta],
		[qmem_granted__Avg],

		[qmem_used__FirstSeen],
		[qmem_used__LastSeen],
		[qmem_used__Min],
		[qmem_used__Max],
		[qmem_used__MMDelta],
		[qmem_used__Avg],

		[TlogUsed_FirstSeen],
		[TlogUsed_LastSeen],
		[TlogUsed_FLDelta],
		[TlogUsed_Min],
		[TlogUsed_Max],
		[TlogUsed_MMDelta],

		[LReads_FirstSeen],
		[LReads_LastSeen],
		[LReads_FLDelta],
		[LReads_Min],
		[LReads_Max],
		[LReads_MMDelta],

		[PReads_FirstSeen],
		[PReads_LastSeen],
		[PReads_FLDelta],
		[PReads_Min],
		[PReads_Max],
		[PReads_MMDelta],

		[Writes_FirstSeen],
		[Writes_LastSeen],
		[Writes_FLDelta],
		[Writes_Min],
		[Writes_Max],
		[Writes_MMDelta]
	FROM (' + @lv__UnionSubQuery + N'
	) un1
	ORDER BY BatchOrderBy, 
		-- orders the statements
		StmtStartOrderBy, TieBreaker
	;';

	
	/* For debugging: 
		SELECT dyntxt, TxtLink
		from (SELECT @lv__OuterSelect AS dyntxt) t0
			cross apply (select TxtLink=(select [processing-instruction(q)]=dyntxt
                            for xml path(''),type)) F2
	*/
		SELECT dyntxt, TxtLink
		from (SELECT @lv__OuterSelect AS dyntxt) t0
			cross apply (select TxtLink=(select [processing-instruction(q)]=dyntxt
                            for xml path(''),type)) F2
RETURN 0;

	EXEC sp_executesql @lv__OuterSelect;
	RETURN 0;
	--everything below is going to be thrown away once we get this working
	

	/****************************************************************************************************
	**********								Final Result Set								   **********
	*****************************************************************************************************/
	SET @lv__OuterSelect = N'
	SELECT 
		[SPID], 
		[FirstSeen], 
		[LastSeen], 
		[Extent(sec)],
		[#Seen], 
		[DB&Object] = DBObject, 
		[Cmd],
		[Statuses] = StatusCodes,
		[NonCXwaits],
		[CXWaits],
		[Tasks], 
		[DOP],
		[MaxTdb], 
		[MaxQMem],
		[MaxUsedMem],
		[Tlog],
		[CPU] = CASE WHEN CPU = N'''' THEN N'''' ELSE 
			SUBSTRING(CPU, 1, CHARINDEX(N''.'',CPU)-1) END, 
		[MemReads],
		[PhysReads],
		[Writes]
	' + CASE WHEN @attr = N'y' OR @plan = N'y' THEN N',[Plan&Info] = PNI'
			ELSE N'' END

	SET @lv__UnionSubQuery = N'

	FROM (
		SELECT 
			--ordering fields
			[BatchOrderBy] =		BatchIdentifier, 
			[StmtStartOrderBy] =	rqst__start_time, 
			[TieBreaker] =			1,

			--visible fields
			[SPID] =				CASE 
										WHEN request_id = 0 
											THEN CONVERT(NVARCHAR(20), session_id) 
										ELSE CONVERT(NVARCHAR(20), session_id) + N'':'' + CONVERT(NVARCHAR(20), request_id) 
									END,
			[CPU] =					N'''',
			[PhysReads] =			N'''',
			[Writes] =				N'''',
			[MemReads] =			N'''',
			[Tlog] =				N'''',
			[MaxTdb] =				N'''', 
			[MaxQMem] =				N'''',
			[MaxUsedMem] =			N'''',
			[#Seen] =				N'''',
			[FirstSeen] =			CONVERT(NVARCHAR(20),lb.rqst__start_time),
			[LastSeen] =			CONVERT(NVARCHAR(20),lb.LastSeen),
			[Extent(sec)] =			CONVERT(NVARCHAR(20),DATEDIFF(SECOND, rqst__start_time, LastSeen)),

			[DBObject] =			ISNULL(DB_NAME(StartingDBID),N''''),
			[Cmd] =					xapp1.inputbuffer_xml,
			[StatusCodes] =			N'''',
			[NonCXWaits] =			N'''',
			[CXWaits] =				N'''',
			[Tasks] =				N'''',
			[DOP] =					N'''' 
			' + CASE WHEN @attr=N'n' AND @plan = N'n' THEN N'' 
				ELSE (CASE WHEN @attr=N'n' THEN N',PNI = N'''' '
						ELSE N',PNI = CONVERT(XML,lb.SessAttr)' 
						END)
				END + N'
		FROM #LongBatches lb
			OUTER APPLY (
				SELECT TOP 1 ib.inputbuffer_xml
				FROM #InputBufferStore ib
				WHERE ib.PKInputBufferStoreID = lb.FKInputBufferStore 
			) xapp1

		UNION ALL

		SELECT 
			--ordering fields
			[BatchOrderBy] =		s.BatchIdentifier, 
			[StmtStartOrderBy] =	FirstSeen, 
			[TieBreaker] =			2,

			--Visible fields
			[SPID] =				N'''', 
			[CPU] =					CONVERT(NVARCHAR(20),CONVERT(MONEY,s.MaxCPUTime),1),';

		IF @units = N'mb'
		BEGIN
			SET @lv__UnionSubQuery = @lv__UnionSubQuery + N'
			[PhysReads] =			CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxReads*8./1024.   ),1),
			[Writes] =				CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxWrites*8./1024.   ),1),
			[MemReads] =			CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxLogicalReads*8./1024.   ),1),
			[Tlog] =				ISNULL(CONVERT(NVARCHAR(20), CONVERT(MONEY, s.MaxTlog/1024./1024.   ),1),N''''),

			[TaskTdb] =				

			[MaxTdb] =				CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxTempDB__CurrentlyAllocatedPages*8./1024.   ),1), 
			[MaxQMem] =				ISNULL(CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxGrantedMemoryKB/1024.   ),1),N''''), 
			[MaxUsedMem] =			ISNULL(CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxUsedMemoryKB/1024.   ),1),N''''),';
		END
		ELSE IF @units = N'native'
		BEGIN
			SET @lv__UnionSubQuery = @lv__UnionSubQuery + N'
			[PhysReads] =			CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxReads   ),1),
			[Writes] =				CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxWrites   ),1),
			[MemReads] =			CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxLogicalReads   ),1),
			[Tlog] =				ISNULL(CONVERT(NVARCHAR(20), CONVERT(MONEY,   s.MaxTlog   ),1),N''''),
			[MaxTdb] =				CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxTempDB__CurrentlyAllocatedPages   ),1), 
			[MaxQMem] =				ISNULL(CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxGrantedMemoryKB   ),1),N''''), 
			[MaxUsedMem] =			ISNULL(CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxUsedMemoryKB   ),1),N''''),';
		END
		ELSE IF @units = N'pages'
		BEGIN
			SET @lv__UnionSubQuery = @lv__UnionSubQuery + N'
			[PhysReads] =			CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxReads   ),1),
			[Writes] =				CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxWrites   ),1),
			[MemReads] =			CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxLogicalReads   ),1),
			[Tlog] =				ISNULL(CONVERT(NVARCHAR(20), CONVERT(MONEY,   s.MaxTlog/8192.   ),1),N''''),


			------LEFT OFF HERE: was in the middle of calculating TaskTdb, but cannot format it as string yet because I need to 
			--- get rid of the decimal part (here and in the "native" formatting) since it is in its base units.
			--  That means doing the final formatting at a different layer. Maybe for simplicity I need to introduce
			-- a new layer of subquery so that there is "calc subquery", "formatting" subquery, and "final display outer SELECT"


			[TaskTdb] =				CASE WHEN [#Seen] = 1 OR tempdb__MinTaskPages = tempdb__MaxTaskPages 
											THEN CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.tempdb__MaxTaskPages   ),1)
										WHEN [#Seen] = 2 
											THEN CONVERT(NVARCHAR(20), s.MinNumTasks) + N'' / '' + CONVERT(NVARCHAR(20), s.MaxNumTasks)

			[MaxTdb] =				CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxTempDB__CurrentlyAllocatedPages   ),1), 
			[MaxQMem] =				ISNULL(CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxGrantedMemoryKB/8.   ),1),N''''), 
			[MaxUsedMem] =			ISNULL(CONVERT(NVARCHAR(20),CONVERT(MONEY,   s.MaxUsedMemoryKB/8.   ),1),N''''),';
		END	--unit-conditional logic

		SET @lv__UnionSubQuery = @lv__UnionSubQuery + N'

			[Tasks] =				CASE WHEN [#Seen] = 1 OR MinNumTasks = MaxNumTasks THEN CONVERT(NVARCHAR(20), s.MaxNumTasks)
										WHEN [#Seen] = 2 THEN CONVERT(NVARCHAR(20), s.MinNumTasks) + N'' / '' + CONVERT(NVARCHAR(20), s.MaxNumTasks)
										ELSE CONVERT(NVARCHAR(20), s.MinNumTasks) + N'' / '' + CONVERT(NVARCHAR(20), s.MaxNumTasks) + N'' ['' + CONVERT(NVARCHAR(20), s.AvgNumTasks) + N'']''
									END,
			[DOP] =					CASE WHEN [#Seen] = 1 OR LoDOP = HiDOP THEN CONVERT(NVARCHAR(20), s.HiDOP)
										WHEN [#Seen] = 2 THEN CONVERT(NVARCHAR(20), s.LoDOP) + N'' / '' + CONVERT(NVARCHAR(20), s.HiDOP)
										ELSE CONVERT(NVARCHAR(20), s.LoDOP) + N'' / '' + CONVERT(NVARCHAR(20), s.HiDOP) + N'' ['' + CONVERT(NVARCHAR(20), s.AvgDOP) + N'']''
									END
			' + CASE WHEN @attr=N'n' AND @plan = N'n' THEN N'' 
				ELSE (CASE WHEN @plan=N'n' THEN N',PNI = N'''' '
						ELSE N',PNI = qp.query_plan_xml' 
						END)
				END + N'
		FROM #stmtstats s
			INNER JOIN #SQLStmtStore sss
				ON s.FKSQLStmtStoreID = sss.PKSQLStmtStoreID
			' + CASE WHEN @plan=N'n' THEN N''
				ELSE N'LEFT OUTER JOIN #QueryPlanStmtStore qp 
					ON qp.PKQueryPlanStmtStoreID = s.FKQueryPlanStmtStoreID' 
				END + N'
	) ss
	ORDER BY BatchOrderBy, 
		-- orders the statements
		StmtStartOrderBy, TieBreaker
	;
	';

	SET @lv__DynSQL = @lv__OuterSelect + @lv__UnionSubQuery;
	SET @lv__errorloc = N'Exec final dyn sql';
	--print @lv__DynSQL;
	EXEC sp_executesql @lv__DynSQL;
	RETURN 0;
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK;
	SET @lv__errsev = ERROR_SEVERITY();
	SET @lv__errstate = ERROR_STATE();

	IF @lv__errorloc IN (N'Exec first dyn sql')
	BEGIN
		PRINT @lv__DynSQL;
	END

	SET @lv__msg = N'Exception occurred at location ("' + @lv__errorloc + N'"). Error #: ' + CONVERT(NVARCHAR(20),ERROR_NUMBER()) + 
		N'; Severity: ' + CONVERT(NVARCHAR(20),ERROR_SEVERITY()) + N'; State: ' + CONVERT(NVARCHAR(20),ERROR_STATE()) + 
		N'; Msg: ' + ERROR_MESSAGE();

	RAISERROR(@lv__msg, @lv__errsev, @lv__errstate);
	RETURN -1
END CATCH

END
GO
