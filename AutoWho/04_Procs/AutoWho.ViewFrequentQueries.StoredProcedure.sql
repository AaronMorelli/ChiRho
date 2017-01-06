SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ViewFrequentQueries] 
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

	FILE NAME: AutoWho.ViewFrequentQueries.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.ViewFrequentQueries

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Called by the sp_XR_FrequentQueries user-facing procedure. 
		The logic below pulls data from the various AutoWho tables, based on parameter values, and combines
		and formats the data as appropriate. 


	FORMATTING: There are 3 types of "output groups". See the large "Identifying Rows" comment below.

		idle spid, input buffer
			Always just 1 row. Relevant stats are tran-related, tempdb-related, session-related

		active spid, query hash
			The top row has data is aggregated for *ALL* unique requests that have this query hash
			Then, in "sub-rows", 
			we show up to 5 unique StmtStoreID/PlanStoreID combos that are representative for this query hash
			Those "up-to-5" should be the *most* expensive in terms of duration or resources (not sure which yet, prob duration)

		active spid, based on an individual StmtStoreID for an object statement or ad-hoc-with-NULL-query-hash
			The top row has data is aggregated for *ALL* unique plan IDs that have this stmt store ID

			If there is only 1 unique combo of StmtStoreID/PlanID, there is only 1 row.

			Then, in "sub-rows", we show the most expensive 5 unique PlanIDs with their individual stats.


	METRIC NOTES: 

	FUTURE ENHANCEMENTS: 

To Execute
------------------------

*/
(
	@init		TINYINT,
	@start		DATETIME, 
	@end		DATETIME,
	@minocc		INT,
	@spids		NVARCHAR(128)=N'',
	@xspids		NVARCHAR(128)=N'',
	@dbs		NVARCHAR(512)=N'',
	@xdbs		NVARCHAR(512)=N'',
	@attr		NCHAR(1),
	@plan		NCHAR(1),
	@context	NCHAR(1),
	@units		NVARCHAR(20)
)
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SET XACT_ABORT ON;
	SET ANSI_WARNINGS ON;

	/* Definition of our "Identifying rows" logic

		1. Obtain "identifying rows". Here are the possible identifiers. (a, b, c)
			The sub-cases are *NOT* identifying rows, though they will affect the output rows and how data is displayed

			a. Idle spid, FKInputBufferID

			b. Active spid, ad-hoc query (no object id in stmt store), non-null Query Hash ID

				i. only 1 StmtStore/PlanID representative  (take @plan variable into account here)

				ii. more than 1 StmtStore/PlanID representative (take @plan variable into account here)

			c. Active spid, object query (object id in stmt store) or null query hash ID

				i. only 1 PlanID (always the case if @plan='n')

				ii. more than 1 PlanID 

			d. if @context='y', then we add session_database_id as a grouping column for each of the above scenarios.

		2. We calculate input buffer stats and place into #InputBufferStats table. These are a bit more straightforward 
			since there's only 1 output row per Input Buffer

		3. We put "identifying rows" into the #TopIdentifiers table. If possible (TODO, find out), we also calculate
			the number of "representatives" (for "b" above) and number of plans (for "c" above) as attributes on
			this table. I think we can also calculate FirstSeen, LastSeen, Number of Unique requests, and TimesSeen.
			
		

	*/


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
		@lv__DynSQL					NVARCHAR(MAX),
		@lv__DynSQL_base			NVARCHAR(MAX);

	DECLARE 
		@lv__NumQueryHash		INT,
		@lv__NumIB				INT,
		@lv__NumStmtStore		INT;

	DECLARE @startMinus1		DATETIME;

BEGIN TRY
	/********************************************************************************************************************************
						 SSSS    EEEE   TTTTT   U    U    PPPP  
						S        E        T     U    U    P   P
						 SSSS    EEEE     T     U    U    PPPP
							 S   E        T     U    U    P
						 SSSS    EEEE     T      UUUU     P

	********************************************************************************************************************************/
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
	); --TODO: need to implement this logic.

	CREATE TABLE #TimeMinus1 (
		SPIDCaptureTime		DATETIME NOT NULL,
		PrevCaptureTime		DATETIME,
		diffMS				INT
	);

	CREATE TABLE #IBHeaders (
		PKInputBufferStoreID	BIGINT NOT NULL,
		sess__database_id		SMALLINT NULL,		--If @context=N'N', then we leave this NULL so that it is not a differentiator
													--If @context=N'Y', then we pull it and group by it, so that it IS a differentiator

		UniqueOccurrences		INT NOT NULL,
		NumCaptureRows			INT NOT NULL,
		FirstSeen				DATETIME NOT NULL,
		LastSeen				DATETIME NOT NULL,
		DisplayOrder			INT NULL
	);

	CREATE TABLE #IBInstances (
		session_id				SMALLINT NOT NULL,
		TimeIdentifier			DATETIME NOT NULL,

		PKInputBufferStoreID	BIGINT NULL,
		sess__database_id		SMALLINT NULL,		--If @context=N'N', then we leave this NULL so that it is not a differentiator
													--If @context=N'Y', then we pull it and group by it, so that it IS a differentiator

		NumCaptureRows			INT NOT NULL,
		FirstSeen				DATETIME NOT NULL,
		LastSeen				DATETIME NOT NULL
	);

	--Cache the most important data that we need
	CREATE TABLE #IBRawStats (
		--identifier fields
		session_id				SMALLINT,
		TimeIdentifier			DATETIME NOT NULL,

		PKInputBufferStoreID	BIGINT NOT NULL,
		sess__database_id		SMALLINT NULL,		--If @context=N'N', then we leave this NULL so that it is not a differentiator
													--If @context=N'Y', then we pull it and group by it, so that it IS a differentiator

		--Attributes. Each of these values is from the last time the idle spid (spid/TimeIdentifier) was seen
		sess__cpu_time			INT NULL,
		sess__reads				BIGINT NULL,
		sess__writes			BIGINT NULL,
		sess__logical_reads		BIGINT NULL,
		sess__open_transaction_count	INT NULL,
		calc__duration_ms		BIGINT NULL,
		TempDBAlloc_pages		BIGINT,
		TempDBUsed_pages		BIGINT,
		LongestTranLength_ms	BIGINT,
		NumLogRecords			BIGINT,
		LogUsed_bytes			BIGINT,
		LogReserved_bytes		BIGINT,
		HasSnapshotTran			TINYINT
	);

	CREATE TABLE #QHHeaders (
		query_hash				BINARY(8) NOT NULL,
		sess__database_id		SMALLINT NULL,		--If @context=N'N', then we leave this NULL so that it is not a differentiator
													--If @context=N'Y', then we pull it and group by it, so that it IS a differentiator

		UniqueOccurrences		INT NOT NULL,
		NumCaptureRows			INT NOT NULL,
		FirstSeen				DATETIME NOT NULL,
		LastSeen				DATETIME NOT NULL,
		DisplayOrder			INT NULL
	);

	CREATE TABLE #QHInstances (
		session_id				SMALLINT NOT NULL,
		request_id				SMALLINT NOT NULL,
		TimeIdentifier			DATETIME NOT NULL,

		query_hash				BINARY(8) NOT NULL,
		sess__database_id		SMALLINT NULL,		--If @context=N'N', then we leave this NULL so that it is not a differentiator
													--If @context=N'Y', then we pull it and group by it, so that it IS a differentiator

		NumCaptureRows			INT NOT NULL,
		FirstSeen				DATETIME NOT NULL,
		LastSeen				DATETIME NOT NULL
	);


	CREATE TABLE #SARRawStats (
		--This is the identifier of a batch
		session_id				SMALLINT NOT NULL,
		request_id				SMALLINT NOT NULL,
		TimeIdentifier			DATETIME NOT NULL,
		SPIDCaptureTime			DATETIME NOT NULL,


		query_hash				BINARY(8) NOT NULL,
		sess__database_id		SMALLINT NULL,		--If @context=N'N', then we leave this NULL so that it is not a differentiator
													--If @context=N'Y', then we pull it and group by it, so that it IS a differentiator

		PKSQLStmtStoreID		BIGINT NOT NULL,
		PKQueryPlanStmtStoreID	BIGINT NULL,		--If @plan=N'N', then we leave this NULL so that it is not a differentiator
													--If @plan=N'Y', then we pull it and group by it, so that it IS a differentiator.
													--For now, we are going to allow NULL values and they *WILL* be a differentiator.
													--This annoyingly means we could have 2 rows for the same SQLStmtStoreID value, one for a
													--NULL plan and the other for a valid QPSS ID, but the alternative, if we omitted NULL
													--would be to potentially omit all representative rows/sub-rows if we didn't grab the plan
													--at all (e.g. all under capture threshold).
													--TODO: still need to think through how I'm going to handle this.

		rqst__status_code					TINYINT,
		rqst__open_transaction_count		INT,

		rqst__transaction_isolation_level	TINYINT,

		--We need to delta these values with the previous SPIDCaptureTime
		rqst__cpu_time						INT,
		TempDBAlloc_pages			BIGINT,		
		TempDBUsed_pages			BIGINT,		

		mgrant__request_time		DATETIME,		--can derive a "milliseconds to grant" metric
		mgrant__grant_time			DATETIME,		--with these 2 fields

		mgrant__requested_memory_kb	BIGINT,
		mgrant__used_memory_kb		BIGINT,
		mgrant__dop					SMALLINT,

		calc__duration_ms			BIGINT,
		calc__blocking_session_id	SMALLINT,
		calc__is_blocker			BIT
		--calc__tmr_wait		maybe consider this later
		--calc__node_info		maybe consider this later
		--calc__status_info		maybe consider this later

	);

	

	INSERT INTO #SARRawStats (
		session_id,
		request_id,
		TimeIdentifier,
		SPIDCaptureTime,

		PKSQLStmtStoreID,

		rqst__cpu_time,
		calc__duration_ms
	)
	SELECT 
		sar.session_id,
		sar.request_id,
		sar.TimeIdentifier,
		sar.SPIDCaptureTime,
		sar.FKSQLStmtStoreID,
		sar.rqst__cpu_time,
		sar.calc__duration_ms
	FROM AutoWho.SessionsAndRequests sar
	WHERE sar.CollectionInitiatorID = @init
	AND sar.SPIDCaptureTime BETWEEN @startMinus1 AND @end		--we want the statement stats for the SPID Capture Time that precedes @start. (See above logic for @startMinus1)
	AND sar.sess__is_user_process = 1
	AND sar.calc__threshold_ignore = 0
	AND --Active spid with stmt text. (On occasion active requests can have null stmt text)
			sar.request_id <> @lv__nullsmallint AND sar.FKSQLStmtStoreID IS NOT NULL
	;

	--An intermediate table to find the start/stop times for statements in our SAR data
	--This helps performance for our next statement, which figures out when each statement
	-- starts and stops.
	CREATE TABLE #StmtCalcIntervals (
		--This is the identifier of a batch
		session_id				SMALLINT NOT NULL,
		request_id				SMALLINT NOT NULL,
		TimeIdentifier			DATETIME NOT NULL,

		PKSQLStmtStoreID		BIGINT NOT NULL,

		SPIDCaptureTime			DATETIME NOT NULL,
		StmtIsDifferent			BIT NOT NULL,
		PrevSPIDCaptureTime		DATETIME NULL
	);
	CREATE UNIQUE CLUSTERED INDEX CL1 ON #StmtCalcIntervals (
		session_id,
		request_id,
		TimeIdentifier,
		SPIDCaptureTime
	);


	INSERT INTO #StmtCalcIntervals (
		--This is the identifier of a batch
		session_id,
		request_id,
		TimeIdentifier,

		PKSQLStmtStoreID,

		SPIDCaptureTime,
		StmtIsDifferent,
		PrevSPIDCaptureTime
	)
	SELECT 
		r.session_id,
		r.request_id,
		r.TimeIdentifier,

		r.PKSQLStmtStoreID,

		r.SPIDCaptureTime,
		[StmtIsDifferent] = CASE WHEN p.SPIDCaptureTime IS NULL THEN 1 --no prev row, thus this is by def the start of a new stmt
								WHEN ISNULL(r.PKSQLStmtStoreID,0) <> ISNULL(p.PKSQLStmtStoreID,0) THEN 1 
								ELSE 0 END,
		p.SPIDCaptureTime
	FROM #SARRawStats r
		INNER JOIN #TimeMinus1 tm
			ON r.SPIDCaptureTime = tm.SPIDCaptureTime
		LEFT OUTER JOIN #SARRawStats p
			ON p.SPIDCaptureTime = tm.PrevCaptureTime
			AND r.session_id = p.session_id
			AND r.request_id = p.request_id
			AND r.TimeIdentifier = p.TimeIdentifier

	CREATE TABLE #StmtStats (
		--This is the identifier of a batch
		session_id				SMALLINT NOT NULL,
		request_id				SMALLINT NOT NULL,
		TimeIdentifier			DATETIME NOT NULL,

		PKSQLStmtStoreID		BIGINT NOT NULL,
		FirstSPIDCaptureTime	DATETIME NOT NULL,
		LastSPIDCaptureTime		DATETIME NOT NULL,
		PreviousSPIDCaptureTime	DATETIME NULL,		--the capture time immediately previous to FirstSPIDCaptureTime. Allows us to calc delta stats

		calc__duration_ms_delta	BIGINT,
		cpu_time_delta			INT
	);


	INSERT INTO #StmtStats (
		--This is the identifier of a batch
		session_id,
		request_id,
		TimeIdentifier,

		PKSQLStmtStoreID,
		FirstSPIDCaptureTime,
		LastSPIDCaptureTime,
		PreviousSPIDCaptureTime
	)
	SELECT 
		s.session_id,
		s.request_id,
		s.TimeIdentifier,

		s.PKSQLStmtStoreID,
		FirstSPIDCaptureTime = s.SPIDCaptureTime,
		LastSPIDCaptureTime = ISNULL(lastCap.SPIDCaptureTime,s.SPIDCaptureTime),
		s.PrevSPIDCaptureTime
	FROM #StmtCalcIntervals s
		OUTER APPLY (
			--find the next time this spid/request/TimeIdentifier changes statement
			SELECT TOP 1
				FirstCapture = s2.SPIDCaptureTime
			FROM #StmtCalcIntervals s2
			WHERE s2.session_id = s.session_id
			AND s2.request_id = s.request_id
			AND s2.TimeIdentifier = s.TimeIdentifier
			AND s2.StmtIsDifferent = 1
			AND s2.SPIDCaptureTime > s.SPIDCaptureTime
			ORDER BY s2.SPIDCaptureTime ASC
		) nextStmt
		OUTER APPLY (
			--Now, get the max SPIDCaptureTime for this spid/request/TimeIdentifier BEFORE the "next statement"
			SELECT TOP 1 
				s3.SPIDCaptureTime
			FROM #StmtCalcIntervals s3
			WHERE s3.session_id = s.session_id
			AND s3.request_id = s.request_id
			AND s3.TimeIdentifier = s.TimeIdentifier
			AND s3.StmtIsDifferent = 0
			AND s3.SPIDCaptureTime > s.SPIDCaptureTime
			AND s3.SPIDCaptureTime < ISNULL(nextStmt.FirstCapture, CONVERT(DATETIME, '3000-01-01'))
			ORDER BY s3.SPIDCaptureTime DESC
		) lastCap
	WHERE s.StmtIsDifferent = 1;


	UPDATE targ 
	SET calc__duration_ms_delta = l.calc__duration_ms - ISNULL(p.calc__duration_ms,0),
		cpu_time_delta = l.rqst__cpu_time - ISNULL(p.rqst__cpu_time,0)
	FROM #StmtStats targ
		INNER JOIN #SARRawStats l		--last
			ON targ.session_id = l.session_id
			AND targ.request_id = l.request_id
			AND targ.TimeIdentifier = l.TimeIdentifier
			AND targ.LastSPIDCaptureTime = l.SPIDCaptureTime
		LEFT OUTER JOIN #SARRawStats p
			ON targ.session_id = p.session_id
			AND targ.request_id = p.request_id
			AND targ.TimeIdentifier = p.TimeIdentifier
			AND targ.PreviousSPIDCaptureTime = l.SPIDCaptureTime;
		




	/* We need to think about data at 3 main levels

		1. The "header row" level. This is a PKInputBufferStoreID w/aggregated stats for our first result query,
		and then either a query_hash or a PKSQLStmtStoreID w/aggregated stats (and sub-rows) for our second result query.
		The key here is the header row key: Either PKInputBufferStoreID, query_hash, or PKSQLStmtStoreID.

		2. An "instance" level. This is an instance of one of the header rows. For example, for PKInputBufferStoreID = 77,
		there might be 23 unique session_id/TimeIdentifier keys, i.e. 23 times when a spid went idle and its IBuf was PK 77. 

		3. A row-level. This is a specific row inside the SAR table. Thus, for PKInputBufferStoreID = 77, the 5th instance
		of that (e.g. SPID = 104 and TimeIdentifier = 'July 4th, 2017 01:01') that instance might have stayed idle for 
		2 minutes, so we would have 1 row every 15 seconds so 8 rows.
		
		
		The final result set needs the header row level, duh. It also needs aggregated stats. How we calculate those
		aggregated stats varies, but we always want to first isolate the stats at the instance level (Level 2).
		For IBufs, the stats at level 2 are simply "what were the stats for the FINAL time we saw that instance?"
		Thus, we need to examine data at Level #3 and find the final SPIDCaptureTime for each session_id/TimeIdentifier
		combo, and use those stats as our "per-instance" stats. We can then derive our aggs for the header level for
		IBufs from those.

		Anyways, the #IdentifierCoreData table holds a cache basically of the necessary data to get level #3, then Level #2,
		then #Level 1.
	*/
	CREATE TABLE #IdentifierLevel3 (
		--Level #1 identifier (aka "Header row identifier").
		--Only 1 of these should be NOT NULL for any given row.
		PKInputBufferStoreID	BIGINT NULL,
		query_hash				BINARY(8) NULL,
		PKSQLStmtStoreID		BIGINT NULL,

		sess__database_id		SMALLINT NULL,		--If @context=N'N', then we leave this NULL so that it is not a differentiator
													--If @context=N'Y', then we pull it and group by it, so that it IS a differentiator


		--Level #2 identifier
		session_id				SMALLINT NOT NULL,
		request_id				SMALLINT NOT NULL,
		TimeIdentifier			DATETIME NOT NULL,

		--Level #3 identifier
		SPIDCaptureTime			DATETIME NOT NULL
	);

	CREATE TABLE #IdentifierLevel2 (
		IdentifierType			TINYINT NOT NULL,		--1, 2, or 3 corresponding to the Identifying Rows comment above

		--Level #1 identifier (aka "Header row identifier").
		--Only 1 of these should be NOT NULL for any given row.
		PKInputBufferStoreID	BIGINT NULL,
		query_hash				BINARY(8) NULL,
		PKSQLStmtStoreID		BIGINT NULL,

		sess__database_id		SMALLINT NULL,		--If @context=N'N', then we leave this NULL so that it is not a differentiator
													--If @context=N'Y', then we pull it and group by it, so that it IS a differentiator

		--Level #2 identifier
		session_id				SMALLINT NOT NULL,
		request_id				SMALLINT NOT NULL,
		TimeIdentifier			DATETIME NOT NULL,

		NumCaptureRows			INT NOT NULL,
		FirstSeen				DATETIME NOT NULL,
		LastSeen				DATETIME NOT NULL
	);

	CREATE TABLE #IdentifierLevel1 (
		IdentifierType			TINYINT NOT NULL,		--1, 2, or 3 corresponding to the Identifying Rows comment above

		--Level #1 identifier (aka "Header row identifier").
		--Only 1 of these should be NOT NULL for any given row.
		PKInputBufferStoreID	BIGINT NULL,
		query_hash				BINARY(8) NULL,
		PKSQLStmtStoreID		BIGINT NULL,

		sess__database_id		SMALLINT NULL,		--If @context=N'N', then we leave this NULL so that it is not a differentiator
													--If @context=N'Y', then we pull it and group by it, so that it IS a differentiator

		UniqueOccurrences		INT NOT NULL,
		NumCaptureRows			INT NOT NULL,
		FirstSeen				DATETIME NOT NULL,
		LastSeen				DATETIME NOT NULL,
		DisplayOrder			INT NULL
	);


	/*
		We display "representative rows" aka "sub-rows" in our output for active queries, where the key of the sub-row
		is actually the parent key + several additional identifying fields.
		For query hash data, the high-level key (Lev 1) is query_hash. The Lev 2 key is session_id/request_id/TimeIdentifier.
		However, the sub-rows 
		
		we need to first
		calculate our stats for these sub-rows 
	*/
	CREATE TABLE #QHSubRowStats (
		--identifier fields
		session_id				SMALLINT,
		request_id				SMALLINT,
		TimeIdentifier			DATETIME NOT NULL,

		query_hash				BINARY(8) NOT NULL,
		sess__database_id		SMALLINT NULL,		--If @context=N'N', then we leave this NULL so that it is not a differentiator
													--If @context=N'Y', then we pull it and group by it, so that it IS a differentiator

		/* TODO: do these belong in this table? Or in the separate "sub-rows" table?
		FKSQLStmtStoreID			BIGINT NULL,
		FKQueryPlanStmtStoreID		BIGINT NULL,		--TODO: when we are capturing plan info, we need to 
														--handle the case where the QP is null for the first capture, and non-null
														--for further captures.
		*/

		--TODO: Consider adding sess__open_transaction_count at a later time
		--TODO: Consider adding rqst__open_transaction_count at a later time

		rqst__status_code			TINYINT,
		rqst__cpu_time				INT,		--max observed for the key (session_id/request_id/TimeIdentifier)
		rqst__reads					BIGINT,		--max observed
		rqst__writes				BIGINT,		--max observed
		rqst__logical_reads			BIGINT,		--max observed

		TempDBAlloc_pages			BIGINT,		--we store the max observed for the key over any of its SPIDCaptureTimes
		TempDBUsed_pages			BIGINT,		--max observed
		tempdb__CalculatedNumberOfTasks	SMALLINT,	--max observed (usually goes down over time but it CAN increase sometimes,
													-- so we take the max rather than the first observed)

		mgrant__requested_memory_kb	BIGINT,		--max observed
		mgrant__granted_memory_kb	BIGINT,		--max observed
		mgrant__used_memory_kb		BIGINT,		--max observed
		mgrant__dop					BIGINT,		--max observed

		calc__duration_ms			BIGINT,		--max observed
		calc__block_relevant		TINYINT,	--max observed
		calc__is_blocker			TINYINT,	--max observed

		--TODO: potentially add logic based on calc__tmr_wait if we find that useful
		--TODO: potentially add logic based on these fields: calc__node_info, calc__status_info
		--TODO: add in stuff from TAW table
		--TODO: consider adding in stuff from TransactionDetails table
	);

	CREATE TABLE #QH_SARcache (
		query_hash				BINARY(8) NOT NULL,
		SPIDCaptureTime			DATETIME NOT NULL,
		session_id				INT,
		request_id				INT,

		PKSQLStmtStoreID		BIGINT,
		PKQueryPlanStmtStoreID	BIGINT,

		
		
		cpu						BIGINT,
		reads					BIGINT,
		lreads					BIGINT,
		writes					BIGINT,
		tdb_alloc				INT,
		tdb_used				INT,
		mgrant_req				INT,
		mgrant_gr				INT,
		mgrant_used				INT
	);

	CREATE TABLE #QH_SARagg (
		query_hash				BINARY(8) NOT NULL,

		PKSQLStmtStoreID		BIGINT,
		PKQueryPlanStmtStoreID	BIGINT,

		NumRows					INT,

		status_running			INT,
		status_runnable			INT,
		status_suspended		INT,
		status_other			INT,

		duration_sum	BIGINT,
		--duration_min	BIGINT,			since we capture via polling, we could easily capture at 0.0. But that doesn't tell us anything.
		duration_max	BIGINT,
		duration_avg	DECIMAL(21,1),
		--unitless counts:
		duration_0toP5	INT,
		duration_P5to1	INT,
		duration_1to2	INT,
		duration_2to5	INT,
		duration_5to10	INT,
		duration_10to20	INT,
		duration_20plus	INT,

		--request cpu. unit is native (milliseconds)
		cpu_sum			BIGINT,
		--cpu_min		INT,		see above notes on "min" fields
		cpu_max			BIGINT,
		cpu_avg			DECIMAL(21,1),

		--reads. unit is native (8k page reads)
		reads_sum		BIGINT,
		reads_max		BIGINT,
		reads_avg		DECIMAL(21,1),

		writes_sum		BIGINT,
		writes_max		BIGINT,
		writes_avg		DECIMAL(21,1),

		lreads_sum		BIGINT,
		lreads_max		BIGINT,
		lreads_avg		DECIMAL(21,1),

		--from dm_exec_query_memory_grants. unit is native (kb)
		mgrant_req_min	INT,
		mgrant_req_max	INT,
		mgrant_req_avg	DECIMAL(21,1),

		mgrant_gr_min	INT,
		mgrant_gr_max	INT,
		mgrant_gr_avg	DECIMAL(21,1),

		mgrant_used_min	INT,
		mgrant_used_max	INT,
		mgrant_used_avg	DECIMAL(21,1),

		DisplayOrderWithinGroup INT
	);

	--object stmt cache
	CREATE TABLE #Stmt_SARcache (
		PKSQLStmtStoreID		BIGINT,
		SPIDCaptureTime			DATETIME NOT NULL,
		session_id				INT,
		request_id				INT,

		PKQueryPlanStmtStoreID	BIGINT,

		rqst__status_code		TINYINT,
		calc__duration_ms		BIGINT,
		tempdb__CalculatedNumberOfTasks	BIGINT,
		cpu						BIGINT,
		reads					BIGINT,
		lreads					BIGINT,
		writes					BIGINT,
		tdb_alloc				INT,
		tdb_used				INT,
		mgrant_req				INT,
		mgrant_gr				INT,
		mgrant_used				INT
	);

	CREATE TABLE #St_SARagg (
		PKSQLStmtStoreID		BIGINT,
		PKQueryPlanStmtStoreID	BIGINT,

		NumRows					INT,

		status_running			INT,
		status_runnable			INT,
		status_suspended		INT,
		status_other			INT,

		duration_sum	BIGINT,
		--duration_min	BIGINT,			since we capture via polling, we could easily capture at 0.0. But that doesn't tell us anything.
		duration_max	BIGINT,
		duration_avg	DECIMAL(21,1),
		--unitless counts:
		duration_0toP5	INT,
		duration_P5to1	INT,
		duration_1to2	INT,
		duration_2to5	INT,
		duration_5to10	INT,
		duration_10to20	INT,
		duration_20plus	INT,

		--request cpu. unit is native (milliseconds)
		cpu_sum			BIGINT,
		--cpu_min		INT,		see above notes on "min" fields
		cpu_max			BIGINT,
		cpu_avg			DECIMAL(21,1),

		--reads. unit is native (8k page reads)
		reads_sum		BIGINT,
		reads_max		BIGINT,
		reads_avg		DECIMAL(21,1),

		writes_sum		BIGINT,
		writes_max		BIGINT,
		writes_avg		DECIMAL(21,1),

		lreads_sum		BIGINT,
		lreads_max		BIGINT,
		lreads_avg		DECIMAL(21,1),

		--from dm_exec_query_memory_grants. unit is native (kb)
		mgrant_req_min	INT,
		mgrant_req_max	INT,
		mgrant_req_avg	DECIMAL(21,1),

		mgrant_gr_min	INT,
		mgrant_gr_max	INT,
		mgrant_gr_avg	DECIMAL(21,1),

		mgrant_used_min	INT,
		mgrant_used_max	INT,
		mgrant_used_avg	DECIMAL(21,1),

		DisplayOrderWithinGroup INT
	);

	-- There is also the possibility that conversion to XML will fail, so we don't want to wait until the final join.
	-- This temp table is our workspace for that resolution/conversion work.
	CREATE TABLE #InputBufferStore (
		PKInputBufferStoreID	BIGINT NOT NULL,
		inputbuffer				NVARCHAR(4000) NOT NULL,
		inputbuffer_xml			XML
	);

	--Ditto, Stmt Store conversions to XML can fail.
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

	CREATE TABLE #QH_Identifiers (
		--identifier columns
		query_hash				BINARY(8) NOT NULL,

		--attributes
		NumUnique				SMALLINT NOT NULL,
		TimesSeen				INT NOT NULL,
		FirstSeen				DATETIME NOT NULL,
		LastSeen				DATETIME NOT NULL
	);

	SET @lv__errorloc = N'Obtain #TimeMinus1';
	IF @init = 255
	BEGIN
		INSERT INTO #TimeMinus1 (
			SPIDCaptureTime,
			PrevCaptureTime,
			diffMS
		)
		SELECT ct.SPIDCaptureTime, xapp1.SPIDCaptureTime,
			CASE WHEN xapp1.SPIDCaptureTime IS NULL THEN NULL 
				ELSE DATEDIFF(millisecond, xapp1.SPIDCaptureTime, ct.SPIDCaptureTime)
				END
		FROM AutoWho.CaptureTimes ct
			OUTER APPLY (
				SELECT TOP 1 ct2.SPIDCaptureTime
				FROM AutoWho.CaptureTimes ct2
				WHERE ct2.SPIDCaptureTime < ct.SPIDCaptureTime
				--Don't pull the prev time if it is much earlier than @start
				AND ct2.SPIDCaptureTime > DATEADD(MINUTE, -60, @start)
				ORDER BY ct2.SPIDCaptureTime DESC
			) xapp1
		WHERE ct.SPIDCaptureTime BETWEEN @start AND @end
		;
	END
	ELSE
	BEGIN
		INSERT INTO #TimeMinus1 (
			SPIDCaptureTime,
			PrevCaptureTime,
			diffMS
		)
		SELECT ct.SPIDCaptureTime, xapp1.SPIDCaptureTime,
			CASE WHEN xapp1.SPIDCaptureTime IS NULL THEN NULL 
				ELSE DATEDIFF(millisecond, xapp1.SPIDCaptureTime, ct.SPIDCaptureTime)
				END
		FROM AutoWho.UserCollectionTimes ct
			OUTER APPLY (
				SELECT TOP 1 ct2.SPIDCaptureTime
				FROM AutoWho.CaptureTimes ct2
				WHERE ct2.SPIDCaptureTime < ct.SPIDCaptureTime
				AND ct2.SPIDCaptureTime > DATEADD(MINUTE, -60, @start)
				ORDER BY ct2.SPIDCaptureTime DESC
			) xapp1
		WHERE ct.SPIDCaptureTime BETWEEN @start AND @end
		;
	END

	SELECT 
		@startMinus1 = ISNULL(ss.PrevCaptureTime, ss.SPIDCaptureTime)
	FROM (
		SELECT TOP 1 
			tm.SPIDCaptureTime,
			tm.PrevCaptureTime
		FROM #TimeMinus1 tm
		ORDER BY tm.SPIDCaptureTime
	) ss;

	IF @startMinus1 IS NULL
	BEGIN
		SET @startMinus1 = @start;
	END
	/*******************************************************************************************************************************
											End of setup
	********************************************************************************************************************************/

	
	/********************************************************************************************************************************
						IIIII   DDDD      RRRR     OOO    W          W    SSSS
						  I     D   D     R   R   O   O    W        W    S
						  I     D   D     RRRR   O     O    W  W   W      SSSS
						  I     D   D     R  R    O   O      W WW W           S
					    IIIII   DDDD      R   R    OOO        W  W        SSSS

	********************************************************************************************************************************/
	SET @lv__errorloc = N'Initial pop #IdentifierCoreData';
	INSERT INTO #IdentifierLevel3 (
		--Level #1 identifier (aka "Header row identifier")
		--Only 1 of these should be NOT NULL for any given row.
		PKInputBufferStoreID,
		query_hash,
		PKSQLStmtStoreID,

		sess__database_id,

		--Level #2 identifier
		session_id,
		request_id,
		TimeIdentifier,

		--Level #3 identifier
		SPIDCaptureTime
	)
	SELECT 
		--We only want 1 of these 3 to be non-null. If the spid is idle, input buffer.
		-- If the active request is calling a proc, the Store ID. If the active request is
		-- calling an ad-hoc statement, the query hash. We then group by the columns
		-- and count to find the items that match our threshold.

		--Group 1: only non-null when request is idle
		[PKInputBufferStoreID] = CASE WHEN sar.request_id = @lv__nullsmallint THEN sar.FKInputBufferStoreID ELSE NULL END,

		--Group 2: active request, ad-hoc query
		[query_hash] = CASE WHEN sar.request_id <> @lv__nullsmallint AND sss.objectid = @lv__nullsmallint 
								THEN sar.rqst__query_hash ELSE NULL END,

		--Group 3: active request, object statement or ad-hoc w/null query hash
		[PKSQLStmtStoreID] = CASE WHEN sar.request_id <> @lv__nullsmallint 
									AND (
										sss.objectid <> @lv__nullsmallint OR sar.rqst__query_hash IS NULL
									)		--if the query hash is null,
											--we go with the stmt store even for ad-hoc SQL
									THEN sar.FKSQLStmtStoreID ELSE NULL END,

		[sess__database_id] = CASE WHEN @context = N'N' THEN NULL ELSE sar.sess__database_id END,

		session_id,
		request_id,
		TimeIdentifier,		--this is the start time of the SAR record (idle or active start)
		SPIDCaptureTime
	FROM AutoWho.SessionsAndRequests sar
		LEFT OUTER JOIN CoreXR.SQLStmtStore sss
			ON sar.FKSQLStmtStoreID = sss.PKSQLStmtStoreID
	WHERE sar.CollectionInitiatorID = @init
	AND sar.SPIDCaptureTime BETWEEN @start AND @end 
	AND sar.sess__is_user_process = 1
	AND sar.calc__threshold_ignore = 0
	AND (
			--Idle spid with an input buffer. (Idle spids w/durations shorter than our threshold won't be captured)
			(sar.request_id = @lv__nullsmallint AND sar.FKInputBufferStoreID IS NOT NULL)
			OR 
			--Active spid with stmt text. (On occasion active requests can have null stmt text)
			(sar.request_id <> @lv__nullsmallint AND sar.FKSQLStmtStoreID IS NOT NULL)
		);


	INSERT INTO #IdentifierLevel2 (
		IdentifierType,

		--Level #1 identifier (aka "Header row identifier").
		--Only 1 of these should be NOT NULL for any given row.
		PKInputBufferStoreID,
		query_hash,
		PKSQLStmtStoreID,

		sess__database_id,

		--Level #2 identifier
		session_id,
		request_id,
		TimeIdentifier,

		NumCaptureRows,
		FirstSeen,
		LastSeen
	)
	SELECT 
		[IdentifierType] = CASE WHEN PKInputBufferStoreID IS NOT NULL THEN 1
								WHEN query_hash IS NOT NULL THEN 2
								WHEN PKSQLStmtStoreID IS NOT NULL THEN 3
							END,
		i.PKInputBufferStoreID,
		i.query_hash,
		i.PKSQLStmtStoreID,

		i.sess__database_id,

		i.session_id,
		i.request_id,
		i.TimeIdentifier,

		NumCaptureRows = SUM(1),
		FirstSeen = MIN(SPIDCaptureTime),
		LastSeen = MAX(SPIDCaptureTime)
	FROM #IdentifierLevel3 i
	GROUP BY i.PKInputBufferStoreID,
		i.query_hash,
		i.PKSQLStmtStoreID,
		i.sess__database_id,

		i.session_id,
		i.request_id,
		i.TimeIdentifier;


	INSERT INTO #IdentifierLevel1 (
		IdentifierType,

		--Level #1 identifier (aka "Header row identifier").
		--Only 1 of these should be NOT NULL for any given row.
		PKInputBufferStoreID,
		query_hash,
		PKSQLStmtStoreID,
		
		sess__database_id,

		UniqueOccurrences,
		NumCaptureRows,
		FirstSeen,
		LastSeen
	)
	SELECT 
		i.IdentifierType,
		i.PKInputBufferStoreID,
		i.query_hash,
		i.PKSQLStmtStoreID,
		i.sess__database_id,

		[UniqueOccurrences] = SUM(1),
		[NumCaptureRows] = SUM(i.NumCaptureRows),
		[FirstSeen] = MIN(i.FirstSeen),
		[LastSeen] = MAX(i.FirstSeen)
	FROM #IdentifierLevel2 i
	GROUP BY 
		i.IdentifierType,
		i.PKInputBufferStoreID,
		i.query_hash,
		i.PKSQLStmtStoreID,
		i.sess__database_id;

	/*
	SELECT 
		@lv__NumQueryHash = ss.QH,
		@lv__NumStmtStore = ss.SS,
		@lv__NumIB = ss.IB
	FROM (
		SELECT 
			[IB] = SUM(CASE WHEN IdentifierType = 1 THEN 1 ELSE 0 END),
			[QH] = SUM(CASE WHEN IdentifierType = 2 THEN 1 ELSE 0 END),
			[SS] = SUM(CASE WHEN IdentifierType = 3 THEN 1 ELSE 0 END)
		FROM #IdentifierLevel1
	) ss;

	SELECT 
		@lv__NumQueryHash = ISNULL(@lv__NumQueryHash,0),
		@lv__NumStmtStore = ISNULL(@lv__NumStmtStore,0),
		@lv__NumIB = ISNULL(@lv__NumIB,0);
	*/

	/*******************************************************************************************************************************
											End of Identifier Row setup
	********************************************************************************************************************************/

	/********************************************************************************************************************************
						 IIIII   N   N   PPPP   U    U   TTTTT      BBBB    U    U   FFFF   SSSS 
						   I     NN  N   P   P  U    U 	   T        B   B   U    U   F	   S     
						   I     N N N   PPPP	U    U 	   T        BBBB    U    U   FFFF   SSSS 
						   I     N  NN   P		U    U 	   T        B   B   U    U   F	   	    S
					     IIIII   N   N   P		 UUUU  	   T        BBBB     UUUU    F	    SSSS 

	********************************************************************************************************************************/
	SET @lv__errorloc = N'Populate #IBInstances';
	INSERT INTO #IBInstances (
		session_id,
		TimeIdentifier,

		PKInputBufferStoreID,
		sess__database_id,

		NumCaptureRows,
		FirstSeen,
		LastSeen
	)
	SELECT 
		session_id,
		TimeIdentifier,

		PKInputBufferStoreID,
		sess__database_id,

		[NumCaptureRows] = SUM(1),
		[FirstSeen] = MIN(SPIDCaptureTime),
		[LastSeen] = MAX(SPIDCaptureTime)
	FROM (
		SELECT 
			session_id,
			TimeIdentifier,		--this is the start time of the SAR record, i.e. the idle start time

			[PKInputBufferStoreID] = CASE WHEN sar.request_id = @lv__nullsmallint THEN sar.FKInputBufferStoreID ELSE NULL END,
			[sess__database_id] = CASE WHEN @context = N'N' THEN NULL ELSE sar.sess__database_id END,

			SPIDCaptureTime
		FROM AutoWho.SessionsAndRequests sar
			LEFT OUTER JOIN CoreXR.SQLStmtStore sss
				ON sar.FKSQLStmtStoreID = sss.PKSQLStmtStoreID
		WHERE sar.CollectionInitiatorID = @init
		AND sar.SPIDCaptureTime BETWEEN @start AND @end 
		AND sar.sess__is_user_process = 1
		AND sar.calc__threshold_ignore = 0
		AND sar.request_id = @lv__nullsmallint AND sar.FKInputBufferStoreID IS NOT NULL
		--Idle spid with an input buffer. (Idle spids w/durations shorter than our threshold won't be captured)
	) ss
	GROUP BY session_id, 
		TimeIdentifier, 
		PKInputBufferStoreID, 
		sess__database_id;

	SET @lv__errorloc = N'Populate #IBHeaders';
	INSERT INTO #IBHeaders (
		PKInputBufferStoreID,
		sess__database_id,

		UniqueOccurrences,
		NumCaptureRows,
		FirstSeen,
		LastSeen,
		DisplayOrder
	)
	SELECT 
		PKInputBufferStoreID,
		sess__database_id,
		UniqueOccurrences,
		NumCaptureRows,
		FirstSeen,
		LastSeen,
		DisplayOrder = ROW_NUMBER() OVER (ORDER BY UniqueOccurrences DESC)
	FROM (
		SELECT 
			ib.PKInputBufferStoreID,
			ib.sess__database_id,

			UniqueOccurrences = SUM(1),
			NumCaptureRows = SUM(NumCaptureRows),
			FirstSeen = MIN(FirstSeen),
			LastSeen = MAX(LastSeen)
		FROM #IBInstances ib
		GROUP BY ib.PKInputBufferStoreID,
				ib.sess__database_id
	) ss;

	SET @lv__errorloc = N'Populate #IBRawStats';
	INSERT INTO #IBRawStats (
		--identifier fields
		session_id,
		TimeIdentifier,

		PKInputBufferStoreID,
		sess__database_id,

		--Attributes. Each of these values is from the last time the idle spid (spid/TimeIdentifier) was seen
		sess__cpu_time,
		sess__reads,
		sess__writes,
		sess__logical_reads,
		sess__open_transaction_count,
		calc__duration_ms,
		TempDBAlloc_pages,
		TempDBUsed_pages,
		LongestTranLength_ms,
		NumLogRecords,
		LogUsed_bytes,
		LogReserved_bytes,
		HasSnapshotTran
	)
	SELECT 
		ibi.session_id,
		ibi.TimeIdentifier,

		ibi.PKInputBufferStoreID,
		ibi.sess__database_id,
		sar.sess__cpu_time,
		sar.sess__reads,
		sar.sess__writes,
		sar.sess__logical_reads,
		sar.sess__open_transaction_count,
		sar.calc__duration_ms,
		TempDBAlloc_pages = (
				ISNULL(tempdb__sess_user_objects_alloc_page_count,0) + ISNULL(tempdb__sess_internal_objects_alloc_page_count,0) + 
				ISNULL(tempdb__task_user_objects_alloc_page_count,0) + ISNULL(tempdb__task_internal_objects_alloc_page_count,0)),
		TempDBUsed_pages = (
				CASE WHEN (ISNULL(sar.tempdb__sess_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_user_objects_dealloc_page_count,0)) < 0 THEN 0
					ELSE (ISNULL(sar.tempdb__sess_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_user_objects_dealloc_page_count,0)) END + 
				CASE WHEN (ISNULL(sar.tempdb__sess_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_internal_objects_dealloc_page_count,0)) < 0 THEN 0
					ELSE (ISNULL(sar.tempdb__sess_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_internal_objects_dealloc_page_count,0)) END + 
				CASE WHEN (ISNULL(sar.tempdb__task_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_user_objects_dealloc_page_count,0)) < 0 THEN 0
					ELSE (ISNULL(sar.tempdb__task_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_user_objects_dealloc_page_count,0)) END + 
				CASE WHEN (ISNULL(sar.tempdb__task_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_internal_objects_dealloc_page_count,0)) < 0 THEN 0
					ELSE (ISNULL(sar.tempdb__task_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_internal_objects_dealloc_page_count,0)) END),
		td.LongestTranLength_ms,
		td.NumLogRecords,
		td.LogUsed_bytes,
		td.LogReserved_bytes,
		td.HasSnapshotTran
	FROM #IBInstances ibi
		INNER JOIN AutoWho.SessionsAndRequests sar
			ON ibi.session_id = sar.session_id
			AND ibi.TimeIdentifier = sar.TimeIdentifier
			AND ibi.LastSeen = sar.SPIDCaptureTime		--we want the stats of the idle spid as of the last time it was seen
		INNER JOIN (
			SELECT 
				td.SPIDCaptureTime,
				td.session_id,
				LongestTranLength_ms = MAX(DATEDIFF(MILLISECOND, td.dtat_transaction_begin_time, td.SPIDCaptureTime)),
				NumLogRecords = SUM(ISNULL(td.dtdt_database_transaction_log_record_count,0)),
				LogUsed_bytes = SUM(ISNULL(td.dtdt_database_transaction_log_bytes_used,0) + ISNULL(td.dtdt_database_transaction_log_bytes_used_system,0)),
				LogReserved_bytes = SUM(ISNULL(td.dtdt_database_transaction_log_bytes_reserved,0) + ISNULL(td.dtdt_database_transaction_log_bytes_reserved_system,0)),
				HasSnapshotTran = MAX(CONVERT(TINYINT,td.dtasdt_tran_exists))
			FROM AutoWho.TransactionDetails td
			WHERE td.CollectionInitiatorID = @init
			AND td.SPIDCaptureTime BETWEEN @start AND @end
			GROUP BY td.SPIDCaptureTime, td.session_id
		) td
			ON sar.SPIDCaptureTime = td.SPIDCaptureTime
			AND sar.session_id = td.session_id
	WHERE sar.CollectionInitiatorID = @init
	AND sar.SPIDCaptureTime BETWEEN @start AND @end 
	AND sar.sess__is_user_process = 1
	AND sar.calc__threshold_ignore = 0;

	--Resolve the Input Buffer IDs to the corresponding text.
	SET @lv__errorloc = N'Obtain IB raw text';
	INSERT INTO #InputBufferStore (
		PKInputBufferStoreID,
		inputbuffer
		--inputbuffer_xml
	)
	SELECT ibs.PKInputBufferStoreID,
		ibs.InputBuffer
	FROM CoreXR.InputBufferStore ibs
		INNER JOIN #IBHeaders ibh
			ON ibs.PKInputBufferStoreID = ibh.PKInputBufferStoreID;

	SET @lv__errorloc = N'Declare IB cursor';
	DECLARE resolveInputBufferStore  CURSOR LOCAL FAST_FORWARD FOR 
	SELECT 
		PKInputBufferStoreID,
		inputbuffer
	FROM #InputBufferStore;

	SET @lv__errorloc = N'Open IB cursor';
	OPEN resolveInputBufferStore;
	FETCH resolveInputBufferStore INTO @PKInputBufferStore,@ibuf_text;

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

		FETCH resolveInputBufferStore INTO @PKInputBufferStore,@ibuf_text;
	END

	CLOSE resolveInputBufferStore;
	DEALLOCATE resolveInputBufferStore;

	SET @lv__errorloc = N'Construct IB dyn sql';
	SET @lv__DynSQL_base = N'
		SELECT 
			ibh.PKInputBufferStoreID,
			ibh.sess__database_id,
			ibs.inputbuffer_xml,
			ibh.UniqueOccurrences,
			ibh.NumCaptureRows,
			ibh.FirstSeen,
			ibh.LastSeen,
			ibh.DisplayOrder,

			ib.MinIdleDuration_ms,
			ib.MaxIdleDuration_ms,
			ib.AvgIdleDuration_ms,

			ib.MinTempDBAlloc_pages,
			ib.MaxTempDBAlloc_pages,
			ib.AvgTempDBAlloc_pages,

			ib.MinTempDBUsed_pages,
			ib.MaxTempDBUsed_pages,
			ib.AvgTempDBUsed_pages,

			ib.MinTranCount,
			ib.MaxTranCount,
			ib.AvgTranCount,

			ib.MinLongestTranLength_ms,
			ib.MaxLongestTranLength_ms,
			ib.AvgLongestTranLength_ms,

			ib.MinLogReserved_bytes,
			ib.MaxLogReserved_bytes,
			ib.AvgLogReserved_bytes,

			ib.MinLogUsed_bytes,
			ib.MaxLogUsed_bytes,
			ib.AvgLogUsed_bytes,

			ib.MinNumLogRecords,
			ib.MaxNumLogRecords,
			ib.AvgNumLogRecords,

			ib.MinPhysReads_pages,
			ib.MaxPhysReads_pages,
			ib.AvgPhysReads_pages,

			ib.MinLogicReads_pages,
			ib.MaxLogicReads_pages,
			ib.AvgLogicReads_pages,

			ib.MinWrites_pages,
			ib.MaxWrites_pages,
			ib.AvgWrites_pages
		FROM #IBHeaders ibh
			INNER JOIN #InputBufferStore ibs
				ON ibh.PKInputBufferStoreID = ibs.PKInputBufferStoreID
			INNER JOIN (
				SELECT 
					PKInputBufferStoreID,
					sess__database_id,

					MinCPUTime_ms = MIN(sess__cpu_time),
					MaxCPUTime_ms = MAX(sess__cpu_time),
					AvgCPUTime_ms = AVG(sess__cpu_time),

					MinPhysReads_pages = MIN(sess__reads),
					MaxPhysReads_pages = MAX(sess__reads),
					AvgPhysReads_pages = AVG(sess__reads),

					MinLogicReads_pages = MIN(sess__logical_reads),
					MaxLogicReads_pages = MAX(sess__logical_reads),
					AvgLogicReads_pages = AVG(sess__logical_reads),

					MinWrites_pages = MIN(sess__writes),
					MaxWrites_pages = MAX(sess__writes),
					AvgWrites_pages = AVG(sess__writes),

					MinTranCount = MIN(sess__open_transaction_count),
					MaxTranCount = MAX(sess__open_transaction_count),
					AvgTranCount = AVG(sess__open_transaction_count),

					MinIdleDuration_ms = MIN(calc__duration_ms),
					MaxIdleDuration_ms = MAX(calc__duration_ms),
					AvgIdleDuration_ms = AVG(calc__duration_ms),

					MinTempDBAlloc_pages = MIN(TempDBAlloc_pages),
					MaxTempDBAlloc_pages = MAX(TempDBAlloc_pages),
					AvgTempDBAlloc_pages = AVG(TempDBAlloc_pages),

					MinTempDBUsed_pages = MIN(TempDBUsed_pages),
					MaxTempDBUsed_pages = MAX(TempDBUsed_pages),
					AvgTempDBUsed_pages = AVG(TempDBUsed_pages),

					MinLongestTranLength_ms = MIN(LongestTranLength_ms),
					MaxLongestTranLength_ms = MAX(LongestTranLength_ms),
					AvgLongestTranLength_ms = AVG(LongestTranLength_ms),

					MinNumLogRecords = MIN(NumLogRecords),
					MaxNumLogRecords = MAX(NumLogRecords),
					AvgNumLogRecords = AVG(NumLogRecords),

					MinLogUsed_bytes = MIN(LogUsed_bytes),
					MaxLogUsed_bytes = MAX(LogUsed_bytes),
					AvgLogUsed_bytes = AVG(LogUsed_bytes),

					MinLogReserved_bytes = MIN(LogReserved_bytes),
					MaxLogReserved_bytes = MAX(LogReserved_bytes),
					AvgLogReserved_bytes = AVG(LogReserved_bytes)
				FROM #IBRawStats ib
				GROUP BY PKInputBufferStoreID, sess__database_id
			) ib
				ON ib.PKInputBufferStoreID = ibh.PKInputBufferStoreID
	';

	SET @lv__DynSQL = N'
	SELECT 
		[ContextDB] = ib_base.sess__database_id,
		[IBuf] = ib_base.inputbuffer_xml,
		[#UniqSeen] = ib_base.UniqueOccurrences,
		[TotalTimesSeen] = ib_base.NumCaptureRows,
		[FirstSeen] = ib_base.FirstSeen,
		[LastSeen] = ib_base.LastSeen,
		[IdleDur (Min)] = ib_base.MinIdleDuration_ms,
		[(Max)] = ib_base.MaxIdleDuration_ms,
		[(Avg)] = ib_base.AvgIdleDuration_ms,
		[TDB Alloc (Min)] = ib_base.MinTempDBAlloc_pages,
		[(Max)] = ib_base.MaxTempDBAlloc_pages,
		[(Avg)] = ib_base.AvgTempDBAlloc_pages,
		[TDB Used (Min)] = ib_base.MinTempDBUsed_pages,
		[(Max)] = ib_base.MaxTempDBUsed_pages,
		[(Avg)] = ib_base.AvgTempDBUsed_pages,
		[TranCount (Min)] = ib_base.MinTranCount,
		[(Max)] = ib_base.MaxTranCount,
		[(Avg)] = ib_base.AvgTranCount,
		[TranLength (Min)] = ib_base.MinLongestTranLength_ms,
		[(Max)] = ib_base.MaxLongestTranLength_ms,
		[(Avg)] = ib_base.AvgLongestTranLength_ms,
		[LogRsvd (Min)] = ib_base.MinLogReserved_bytes,
		[(Max)] = ib_base.MaxLogReserved_bytes,
		[(Avg)] = ib_base.AvgLogReserved_bytes,
		[LogUsed (Min)] = ib_base.MinLogUsed_bytes,
		[(Max)] = ib_base.MaxLogUsed_bytes,
		[(Avg)] = ib_base.AvgLogUsed_bytes,
		[PhysReads (Min)] = ib_base.MinPhysReads_pages,
		[(Max)] = ib_base.MaxPhysReads_pages,
		[(Avg)] = ib_base.AvgPhysReads_pages,
		[LogicReads (Min)] = ib_base.MinLogicReads_pages,
		[(Max)] = ib_base.MaxLogicReads_pages,
		[(Avg)] = ib_base.AvgLogicReads_pages,
		[Writes (Min)] = ib_base.MinWrites_pages,
		[(Max)] = ib_base.MaxWrites_pages,
		[(Avg)] = ib_base.AvgWrites_pages
	FROM (
	' + @lv__DynSQL_base + '
		) ib_base
	ORDER BY DisplayOrder;
	';

	EXEC sp_executesql @stmt=@lv__DynSQL;
	RETURN 0;

	/*******************************************************************************************************************************
											End of Input Buffers section
	********************************************************************************************************************************/


	/********************************************************************************************************************************
						  QQQQ     H   H      PPPP    PPPP    EEEEE   PPPP  
						 Q    Q    H   H      P   P	  P   P   E		  P   P	
						 Q    Q    HHHHH      PPPP	  PPPP    EEEEE	  PPPP	
						 Q    Q    H   H      P		  P  R    E		  P		
					      QQQQ     H   H      P		  P   R   EEEEE	  P		
						      Q
	********************************************************************************************************************************/

	/*
		The "QH" section focuses on ad-hoc SQL (NULL AutoWho.SQLStmtStore.object_id field). The identifier here is a query_hash, the
		signature of the text of a sql statement. We want to show queries that have run many times, and aggregate their stats.
		However, unlike the Input Buffer set where we only have 1 row per query, we want to show a representative sample of the
		data associated with a single query_hash value. This takes the form "top X StmtStoreID rows" under each query hash,
		where "top" means top # of executions. (We may give more ordering options at a later time). 
		If query plans are desired, then the key for each "representative row" changes from PKSQLStmtStoreID to PKSQLStmtStoreID/PKQueryPlanStmtStoreID.
		Thus, the same SQLStmtStoreID value could occur multiple times in the "representative rows" section.


			0. (We already have all of the SPIDCaptureTimes for the query_hash values in the @start/@end timeframe)

			1. We construct a list of 


	*/
	
	/*
	INSERT INTO #QH_Identifiers (
		query_hash,
		NumUnique,
		TimesSeen,
		FirstSeen,
		LastSeen
	)
	SELECT
		t.query_hash,
		t.NumUnique,
		t.TimesSeen,
		t.FirstSeen,
		t.LastSeen
	FROM #TopIdentifiers t
	WHERE t.query_hash IS NOT NULL;

	CREATE TABLE #Stmt_Identifiers (
		--identifier columns
		PKSQLStmtStoreID		BIGINT NOT NULL,

		--attributes
		NumUnique				SMALLINT NOT NULL,
		TimesSeen				INT NOT NULL,
		FirstSeen				DATETIME NOT NULL,
		LastSeen				DATETIME NOT NULL
	);

	INSERT INTO #Stmt_Identifiers (
		PKSQLStmtStoreID,
		NumUnique,
		TimesSeen,
		FirstSeen,
		LastSeen
	)
	SELECT
		t.PKSQLStmtStoreID,
		t.NumUnique,
		t.TimesSeen,
		t.FirstSeen,
		t.LastSeen
	FROM #TopIdentifiers t
	WHERE t.PKSQLStmtStoreID IS NOT NULL;
	*/
	/*
	CREATE TABLE #ActiveStats (
		PrepID		INT NOT NULL,
			--this surrogate represents both the identifier fields (hash or stmt store id, context DB if requested) and
			-- the query plan (which may just always be NULL if the user did not request the plan)

		--duration fields, unit is our standard "time" formatting
		duration_sum	BIGINT,
		--duration_min	BIGINT,			since we capture via polling, we could easily capture at 0.0. But that doesn't tell us anything.
		duration_max	BIGINT,
		duration_avg	DECIMAL(21,1),
		--unitless counts:
		duration_0toP5	INT,
		duration_P5to1	INT,
		duration_1to2	INT,
		duration_2to5	INT,
		duration_6to10	INT,
		duration_10to20	INT,
		duration_20plus	INT,

		
		--blocking. We measure this by the top-priority wait type if the wait is a lock wait (LCK)
		--unit is our standard "time" formatting
		TimesBlocked	INT,
		blocked_sum		INT,
		--blocked_min		INT,		see above note about polling & capturing at 0.0
		blocked_max		INT,
		blocked_avg		DECIMAL(21,1),
		--unitless counts:
		blocked_0toP5	INT,
		blocked_P5to1	INT,
		blocked_1to2	INT,
		blocked_2to5	INT,
		blocked_6to10	INT,
		blocked_10to20	INT,
		blocked_20plus	INT,

		--request status codes. unitless counts
		numRunning		INT,
		numRunnable		INT,
		numSuspended	INT,
		numOther		INT,		--sleeping, background

		--request cpu. unit is native (milliseconds)
		cpu_sum			BIGINT,
		--cpu_min		INT,		see above notes on "min" fields
		cpu_max			BIGINT,
		cpu_avg			DECIMAL(21,1),

		--reads. unit is native (8k page reads)
		reads_sum		BIGINT,
		reads_max		BIGINT,
		reads_avg		DECIMAL(21,1),

		writes_sum		BIGINT,
		writes_max		BIGINT,
		writes_avg		DECIMAL(21,1),

		lreads_sum		BIGINT,
		lreads_max		BIGINT,
		lreads_avg		DECIMAL(21,1),

		--from dm_db_tasks_space_usage. unit is native (8k page allocations)
		tdb_alloc_sum	BIGINT,
		tdb_alloc_max	BIGINT,
		tdb_alloc_avg	DECIMAL(21,1),
		tdb_used_sum	BIGINT,
		tdb_used_max	BIGINT,
		tdb_used_avg	DECIMAL(21,1),

		--from dm_exec_query_memory_grants. unit is native (kb)
		mgrant_req_min	INT,
		mgrant_req_max	INT,
		mgrant_req_avg	DECIMAL(21,1),

		mgrant_gr_min	INT,
		mgrant_gr_max	INT,
		mgrant_gr_avg	DECIMAL(21,1),

		mgrant_used_min	INT,
		mgrant_used_max	INT,
		mgrant_used_avg	DECIMAL(21,1)
	);
	*/
	





	--query hash cache
	
	/*
	INSERT INTO #QH_SARcache (
		query_hash,
		SPIDCaptureTime,
		session_id,
		request_id,

		PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID,

		rqst__status_code,
		calc__duration_ms,
		tempdb__CalculatedNumberOfTasks,
		cpu,
		reads,
		lreads,
		writes,
		tdb_alloc,
		tdb_used,
		mgrant_req,
		mgrant_gr,
		mgrant_used
	)
	SELECT 
		sar.rqst__query_hash,
		sar.SPIDCaptureTime,
		sar.session_id,
		sar.request_id,

		sar.FKSQLStmtStoreID,
		sar.FKQueryPlanStmtStoreID,

		sar.rqst__status_code,
		DurationMS = CASE WHEN sar.calc__duration_ms >= tm.diffMS THEN sar.calc__duration_ms - tm.diffMS ELSE sar.calc__duration_ms END,
		sar.tempdb__CalculatedNumberOfTasks,
		sar.rqst__cpu_time,
		sar.rqst__reads,
		sar.rqst__logical_reads,
		sar.rqst__writes,
		-1,
		-1, 
		sar.mgrant__requested_memory_kb,
		sar.mgrant__granted_memory_kb,
		sar.mgrant__used_memory_kb
	FROM AutoWho.SessionsAndRequests sar
		INNER JOIN #QH_Identifiers qh
			ON qh.query_hash = sar.rqst__query_hash
		INNER JOIN #TimeMinus1 tm
			ON sar.SPIDCaptureTime = tm.SPIDCaptureTime
	WHERE sar.CollectionInitiatorID = @init
	AND sar.SPIDCaptureTime BETWEEN @start AND @end 
	AND sar.sess__is_user_process = 1
	AND sar.calc__threshold_ignore = 0
	;

	--TODO: for durations > 15 seconds, need to correct that in my sar cache table.
	-- (And do this for waits also). 
	-- Easiest to grab list of times from AutoWho.CaptureTimes (or UserCaptureTimes),
	-- and then self-join to grab the NOW minus 1 match for each time. Then, take
	-- that table and join it to SAR above to provide the value for another SAR join
	-- to get the previous duration. 

	*/

	

	/*
	INSERT INTO #QH_SARagg (
		query_hash, 

		PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID,

		NumRows,

		status_running,
		status_runnable,
		status_suspended,
		status_other,

		duration_sum,
		--duration_min,
		duration_max,
		duration_avg,
		--unitless counts:
		duration_0toP5,
		duration_P5to1,
		duration_1to2,
		duration_2to5,
		duration_5to10,
		duration_10to20,
		duration_20plus,

		--request cpu. unit is native (milliseconds)
		cpu_sum,
		--cpu_min		INT,		see above notes on "min" fields
		cpu_max,
		cpu_avg,

		--reads. unit is native (8k page reads)
		reads_sum,
		reads_max,
		reads_avg,

		writes_sum,
		writes_max,
		writes_avg,

		lreads_sum,
		lreads_max,
		lreads_avg,

		--from dm_exec_query_memory_grants. unit is native (kb)
		mgrant_req_min,
		mgrant_req_max,
		mgrant_req_avg,

		mgrant_gr_min,
		mgrant_gr_max,
		mgrant_gr_avg,

		mgrant_used_min,
		mgrant_used_max,
		mgrant_used_avg,

		DisplayOrderWithinGroup
	)
	SELECT 
		query_hash, 
		PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID,

		NumRows = COUNT(*),

		status_running = SUM(CASE WHEN qh.rqst__status_code=1 THEN 1 ELSE 0 END), 
		status_runnable = SUM(CASE WHEN qh.rqst__status_code=2 THEN 1 ELSE 0 END),
		status_suspended = SUM(CASE WHEN qh.rqst__status_code=4 THEN 1 ELSE 0 END),
		status_other = SUM(CASE WHEN qh.rqst__status_code NOT IN (1,2,4) THEN 1 ELSE 0 END),

		duration_sum = SUM(qh.calc__duration_ms),
		duration_max = MAX(qh.calc__duration_ms),
		duration_avg = AVG(qh.calc__duration_ms),
		duration_0toP5 = SUM(CASE WHEN qh.calc__duration_ms >= 0 AND qh.calc__duration_ms < 0.5 THEN 1 ELSE 0 END),
		duration_P5to1 = SUM(CASE WHEN qh.calc__duration_ms >= 0.5 AND qh.calc__duration_ms < 1.0 THEN 1 ELSE 0 END),
		duration_1to2 = SUM(CASE WHEN qh.calc__duration_ms >= 1.0 AND qh.calc__duration_ms < 2.0 THEN 1 ELSE 0 END),
		duration_2to5 = SUM(CASE WHEN qh.calc__duration_ms >= 2.0 AND qh.calc__duration_ms < 5.0 THEN 1 ELSE 0 END),
		duration_5to10 = SUM(CASE WHEN qh.calc__duration_ms >= 5.0 AND qh.calc__duration_ms < 10.0 THEN 1 ELSE 0 END),
		duration_10to20 = SUM(CASE WHEN qh.calc__duration_ms >= 10.0 AND qh.calc__duration_ms < 20.0 THEN 1 ELSE 0 END),
		duration_20plus = SUM(CASE WHEN qh.calc__duration_ms >= 20.0 THEN 1 ELSE 0 END),

		cpu_sum = SUM(qh.cpu),
		cpu_max = MAX(qh.cpu),
		cpu_avg = AVG(qh.cpu),

		reads_sum = SUM(qh.reads),
		reads_max = MAX(qh.reads), 
		reads_avg = AVG(qh.reads),

		writes_sum = SUM(qh.writes),
		writes_max = MAX(qh.writes),
		writes_avg = AVG(qh.writes), 

		lreads_sum = SUM(qh.lreads), 
		lreads_max = MAX(qh.lreads), 
		lreads_avg = AVG(qh.lreads),

		mgrant_req_min = MIN(qh.mgrant_req),
		mgrant_req_max = MAX(qh.mgrant_req),
		mgrant_req_avg = AVG(qh.mgrant_req),

		mgrant_gr_min = MIN(qh.mgrant_gr),
		mgrant_gr_max = MAX(qh.mgrant_gr),
		mgrant_gr_avg = AVG(qh.mgrant_gr),

		mgrant_used_min = MIN(qh.mgrant_used), 
		mgrant_used_max = MAX(qh.mgrant_used), 
		mgrant_used_avg = AVG(qh.mgrant_used),

		[DisplayOrderWithinGroup] = ROW_NUMBER() OVER (PARTITION BY query_hash ORDER BY SUM(qh.calc__duration_ms) DESC)
	FROM #QH_SARcache qh
	GROUP BY query_hash, 
		PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID
		;


	INSERT INTO #QH_SARagg (
		query_hash, 

		PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID,

		NumRows,

		status_running,
		status_runnable,
		status_suspended,
		status_other,

		duration_sum,
		--duration_min,
		duration_max,
		duration_avg,
		--unitless counts:
		duration_0toP5,
		duration_P5to1,
		duration_1to2,
		duration_2to5,
		duration_5to10,
		duration_10to20,
		duration_20plus,

		--request cpu. unit is native (milliseconds)
		cpu_sum,
		--cpu_min		INT,		see above notes on "min" fields
		cpu_max,
		cpu_avg,

		--reads. unit is native (8k page reads)
		reads_sum,
		reads_max,
		reads_avg,

		writes_sum,
		writes_max,
		writes_avg,

		lreads_sum,
		lreads_max,
		lreads_avg,

		DisplayOrderWithinGroup
	)
	SELECT 
		query_hash, 
		PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID,
		NumRows, 
		status_running,
		status_runnable,
		status_suspended,
		status_other, 
		duration_sum, 
		duration_max,
		duration_avg = CONVERT(DECIMAL(21,1),(duration_sum*1.) / (NumRows*1.)),
		duration_0toP5,
		duration_P5to1,
		duration_1to2,
		duration_2to5,
		duration_5to10,
		duration_10to20,
		duration_20plus,

		cpu_sum,
		cpu_max,
		cpu_avg = CONVERT(DECIMAL(21,1),(cpu_sum*1.) / (NumRows*1.)),

		reads_sum, 
		reads_max, 
		reads_avg = CONVERT(DECIMAL(21,1),(reads_sum*1.) / (NumRows*1.)),

		writes_sum, 
		writes_max, 
		writes_avg = CONVERT(DECIMAL(21,1),(writes_sum*1.) / (NumRows*1.)),

		lreads_sum,
		lreads_max,
		lreads_avg = CONVERT(DECIMAL(21,1),(lreads_sum*1.) / (NumRows*1.)),

		DisplayOrderWithinGroup
	FROM (
		SELECT 
			query_hash, 
			PKSQLStmtStoreID = NULL,
			PKQueryPlanStmtStoreID = NULL,

			NumRows = SUM(NumRows),

			status_running = SUM(qh.status_running), 
			status_runnable = SUM(qh.status_runnable),
			status_suspended = SUM(qh.status_suspended),
			status_other = SUM(qh.status_other),

			duration_sum = SUM(qh.duration_sum),
			duration_max = MAX(qh.duration_max),
			--duration_avg = AVG(qh.calc__duration_ms),
			duration_0toP5 = SUM(duration_0toP5),
			duration_P5to1 = SUM(duration_P5to1),
			duration_1to2 = SUM(duration_1to2),
			duration_2to5 = SUM(duration_2to5),
			duration_5to10 = SUM(duration_5to10),
			duration_10to20 = SUM(duration_10to20),
			duration_20plus = SUM(duration_20plus),

			cpu_sum = SUM(cpu_sum),
			cpu_max = MAX(cpu_max),
			--cpu_avg = AVG(qh.cpu),

			reads_sum = SUM(reads_sum),
			reads_max = MAX(reads_max), 
			--reads_avg = AVG(reads),

			writes_sum = SUM(writes_sum),
			writes_max = MAX(writes_max),
			--writes_avg = AVG(qh.writes), 

			lreads_sum = SUM(lreads_sum), 
			lreads_max = MAX(lreads_max), 
			--lreads_avg = AVG(qh.lreads),

			[DisplayOrderWithinGroup] = 0
		FROM #QH_SARagg qh
		GROUP BY query_hash
	) ss
	;
	*/

	/*
	SELECT q.query_hash, q.NumUnique, q.TimesSeen, q.FirstSeen, q.LastSeen,
		agg.*
	from #QH_Identifiers q
		inner join #QH_SARagg agg
			on q.query_hash = agg.query_hash
			--and agg.DisplayOrderWithinGroup <= 3
	;

	return 0;
	*/



	/*

	INSERT INTO #Stmt_SARcache (
		PKSQLStmtStoreID,
		SPIDCaptureTime,
		session_id,
		request_id,

		PKQueryPlanStmtStoreID,

		rqst__status_code,
		calc__duration_ms,
		tempdb__CalculatedNumberOfTasks,
		cpu,
		reads,
		lreads,
		writes,
		tdb_alloc,
		tdb_used,
		mgrant_req,
		mgrant_gr,
		mgrant_used
	)
	SELECT 
		sar.FKSQLStmtStoreID,
		sar.SPIDCaptureTime,
		sar.session_id,
		sar.request_id,

		sar.FKQueryPlanStmtStoreID,

		sar.rqst__status_code,
		sar.calc__duration_ms,
		sar.tempdb__CalculatedNumberOfTasks,
		sar.rqst__cpu_time,
		sar.rqst__reads,
		sar.rqst__logical_reads,
		sar.rqst__writes,
		-1,
		-1, 
		sar.mgrant__requested_memory_kb,
		sar.mgrant__granted_memory_kb,
		sar.mgrant__used_memory_kb
	FROM AutoWho.SessionsAndRequests sar
		INNER JOIN #Stmt_Identifiers st
			ON st.PKSQLStmtStoreID = sar.FKQueryPlanStmtStoreID
	WHERE sar.CollectionInitiatorID = @init
	AND sar.SPIDCaptureTime BETWEEN @start AND @end 
	AND sar.sess__is_user_process = 1
	AND sar.calc__threshold_ignore = 0
	;



	


	INSERT INTO #St_SARagg (
		PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID,

		NumRows,

		status_running,
		status_runnable,
		status_suspended,
		status_other,

		duration_sum,
		--duration_min,
		duration_max,
		duration_avg,
		--unitless counts:
		duration_0toP5,
		duration_P5to1,
		duration_1to2,
		duration_2to5,
		duration_5to10,
		duration_10to20,
		duration_20plus,

		--request cpu. unit is native (milliseconds)
		cpu_sum,
		--cpu_min		INT,		see above notes on "min" fields
		cpu_max,
		cpu_avg,

		--reads. unit is native (8k page reads)
		reads_sum,
		reads_max,
		reads_avg,

		writes_sum,
		writes_max,
		writes_avg,

		lreads_sum,
		lreads_max,
		lreads_avg,

		--from dm_exec_query_memory_grants. unit is native (kb)
		mgrant_req_min,
		mgrant_req_max,
		mgrant_req_avg,

		mgrant_gr_min,
		mgrant_gr_max,
		mgrant_gr_avg,

		mgrant_used_min,
		mgrant_used_max,
		mgrant_used_avg,

		DisplayOrderWithinGroup
	)
	SELECT 
		PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID,

		NumRows = COUNT(*),

		status_running = SUM(CASE WHEN qh.rqst__status_code=1 THEN 1 ELSE 0 END), 
		status_runnable = SUM(CASE WHEN qh.rqst__status_code=2 THEN 1 ELSE 0 END),
		status_suspended = SUM(CASE WHEN qh.rqst__status_code=4 THEN 1 ELSE 0 END),
		status_other = SUM(CASE WHEN qh.rqst__status_code NOT IN (1,2,4) THEN 1 ELSE 0 END),

		duration_sum = SUM(qh.calc__duration_ms),
		duration_max = MAX(qh.calc__duration_ms),
		duration_avg = AVG(qh.calc__duration_ms),
		duration_0toP5 = SUM(CASE WHEN qh.calc__duration_ms >= 0 AND qh.calc__duration_ms < 0.5 THEN 1 ELSE 0 END),
		duration_P5to1 = SUM(CASE WHEN qh.calc__duration_ms >= 0.5 AND qh.calc__duration_ms < 1.0 THEN 1 ELSE 0 END),
		duration_1to2 = SUM(CASE WHEN qh.calc__duration_ms >= 1.0 AND qh.calc__duration_ms < 2.0 THEN 1 ELSE 0 END),
		duration_2to5 = SUM(CASE WHEN qh.calc__duration_ms >= 2.0 AND qh.calc__duration_ms < 5.0 THEN 1 ELSE 0 END),
		duration_5to10 = SUM(CASE WHEN qh.calc__duration_ms >= 5.0 AND qh.calc__duration_ms < 10.0 THEN 1 ELSE 0 END),
		duration_10to20 = SUM(CASE WHEN qh.calc__duration_ms >= 10.0 AND qh.calc__duration_ms < 20.0 THEN 1 ELSE 0 END),
		duration_20plus = SUM(CASE WHEN qh.calc__duration_ms >= 20.0 THEN 1 ELSE 0 END),

		cpu_sum = SUM(qh.cpu),
		cpu_max = MAX(qh.cpu),
		cpu_avg = AVG(qh.cpu),

		reads_sum = SUM(qh.reads),
		reads_max = MAX(qh.reads), 
		reads_avg = AVG(qh.reads),

		writes_sum = SUM(qh.writes),
		writes_max = MAX(qh.writes),
		writes_avg = AVG(qh.writes), 

		lreads_sum = SUM(qh.lreads), 
		lreads_max = MAX(qh.lreads), 
		lreads_avg = AVG(qh.lreads),

		mgrant_req_min = MIN(qh.mgrant_req),
		mgrant_req_max = MAX(qh.mgrant_req),
		mgrant_req_avg = AVG(qh.mgrant_req),

		mgrant_gr_min = MIN(qh.mgrant_gr),
		mgrant_gr_max = MAX(qh.mgrant_gr),
		mgrant_gr_avg = AVG(qh.mgrant_gr),

		mgrant_used_min = MIN(qh.mgrant_used), 
		mgrant_used_max = MAX(qh.mgrant_used), 
		mgrant_used_avg = AVG(qh.mgrant_used),

		[DisplayOrderWithinGroup] = ROW_NUMBER() OVER (PARTITION BY PKSQLStmtStoreID ORDER BY SUM(qh.calc__duration_ms) DESC)
	FROM #Stmt_SARcache qh
	GROUP BY PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID
		;
		


	INSERT INTO #St_SARagg (
		PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID,

		NumRows,

		status_running,
		status_runnable,
		status_suspended,
		status_other,

		duration_sum,
		--duration_min,
		duration_max,
		duration_avg,
		--unitless counts:
		duration_0toP5,
		duration_P5to1,
		duration_1to2,
		duration_2to5,
		duration_5to10,
		duration_10to20,
		duration_20plus,

		--request cpu. unit is native (milliseconds)
		cpu_sum,
		--cpu_min		INT,		see above notes on "min" fields
		cpu_max,
		cpu_avg,

		--reads. unit is native (8k page reads)
		reads_sum,
		reads_max,
		reads_avg,

		writes_sum,
		writes_max,
		writes_avg,

		lreads_sum,
		lreads_max,
		lreads_avg,

		DisplayOrderWithinGroup
	)
	SELECT 
		PKSQLStmtStoreID,
		PKQueryPlanStmtStoreID,
		NumRows, 
		status_running,
		status_runnable,
		status_suspended,
		status_other, 
		duration_sum, 
		duration_max,
		duration_avg = CONVERT(DECIMAL(21,1),(duration_sum*1.) / (NumRows*1.)),
		duration_0toP5,
		duration_P5to1,
		duration_1to2,
		duration_2to5,
		duration_5to10,
		duration_10to20,
		duration_20plus,

		cpu_sum,
		cpu_max,
		cpu_avg = CONVERT(DECIMAL(21,1),(cpu_sum*1.) / (NumRows*1.)),

		reads_sum, 
		reads_max, 
		reads_avg = CONVERT(DECIMAL(21,1),(reads_sum*1.) / (NumRows*1.)),

		writes_sum, 
		writes_max, 
		writes_avg = CONVERT(DECIMAL(21,1),(writes_sum*1.) / (NumRows*1.)),

		lreads_sum,
		lreads_max,
		lreads_avg = CONVERT(DECIMAL(21,1),(lreads_sum*1.) / (NumRows*1.)),

		DisplayOrderWithinGroup
	FROM (
		SELECT 
			PKSQLStmtStoreID, 
			PKQueryPlanStmtStoreID = NULL,

			NumRows = SUM(NumRows),

			status_running = SUM(qh.status_running), 
			status_runnable = SUM(qh.status_runnable),
			status_suspended = SUM(qh.status_suspended),
			status_other = SUM(qh.status_other),

			duration_sum = SUM(qh.duration_sum),
			duration_max = MAX(qh.duration_max),
			--duration_avg = AVG(qh.calc__duration_ms),
			duration_0toP5 = SUM(duration_0toP5),
			duration_P5to1 = SUM(duration_P5to1),
			duration_1to2 = SUM(duration_1to2),
			duration_2to5 = SUM(duration_2to5),
			duration_5to10 = SUM(duration_5to10),
			duration_10to20 = SUM(duration_10to20),
			duration_20plus = SUM(duration_20plus),

			cpu_sum = SUM(cpu_sum),
			cpu_max = MAX(cpu_max),
			--cpu_avg = AVG(qh.cpu),

			reads_sum = SUM(reads_sum),
			reads_max = MAX(reads_max), 
			--reads_avg = AVG(reads),

			writes_sum = SUM(writes_sum),
			writes_max = MAX(writes_max),
			--writes_avg = AVG(qh.writes), 

			lreads_sum = SUM(lreads_sum), 
			lreads_max = MAX(lreads_max), 
			--lreads_avg = AVG(qh.lreads),

			[DisplayOrderWithinGroup] = 0
		FROM #ST_SARagg qh
		GROUP BY PKSQLStmtStoreID
	) ss
	;

	*/

	/*
	SELECT q.PKSQLStmtStoreID, q.NumUnique, q.TimesSeen, q.FirstSeen, q.LastSeen,
		agg.*
	FROM #Stmt_Identifiers q
		INNER JOIN #St_SARagg agg
			ON q.PKSQLStmtStoreID = agg.PKSQLStmtStoreID
			AND agg.DisplayOrderWithinGroup <= 3
	;
	*/




	
	/*

	
	

	




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

		SELECT q.PKSQLStmtStoreID
		FROM #QH_SARagg q

		UNION 

		SELECT s.PKSQLStmtStoreID
		FROM #St_SARagg s
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


	
	*/
	
	/*

	SELECT GroupID = CONVERT(VARCHAR(20),q.query_hash), 
		[Statement] = CASE WHEN DisplayOrderWithinGroup = 0 THEN N'' ELSE sss.stmt_xml END,
		[NumUnique] = CASE WHEN DisplayOrderWithinGroup = 0 THEN N'' ELSE NumUnique END,
		[TimesSeen] = CASE WHEN DisplayOrderWithinGroup = 0 THEN N'' ELSE q.TimesSeen END, 
		[FirstSeen] = CASE WHEN DisplayOrderWithinGroup = 0 THEN N'' ELSE q.FirstSeen END, 
		[LastSeen] = CASE WHEN DisplayOrderWithinGroup = 0 THEN N'' ELSE q.LastSeen END,
		[Statii] = CASE WHEN status_running > 0 THEN CONVERT(VARCHAR(20),status_running) + 'xRun' ELSE '' END + 
					CASE WHEN status_runnable > 0 THEN CONVERT(VARCHAR(20),status_runnable) + 'xRabl' ELSE '' END + 
					CASE WHEN status_suspended > 0 THEN CONVERT(VARCHAR(20),status_suspended) + 'xSus' ELSE '' END + 
					CASE WHEN status_other > 0 THEN CONVERT(VARCHAR(20),status_other) + 'xOth' ELSE '' END,
		[Duration (Sum)] = duration_sum,
		[(Max)] = duration_max,
		[(Avg)] = duration_avg,
		[(0-0.5)] = duration_0toP5,
		[(0.5-1)] = duration_P5to1,
		[(1-2)] = duration_1to2,
		[(2-5)] = duration_2to5,
		[(5-10)] = duration_5to10,
		[(10-20)] = duration_10to20,
		[(20+)] = duration_20plus,
		
		[CPU (Sum)] = cpu_sum,
		[(Max)] = cpu_max,
		[(Avg)] = cpu_avg,
		
		[PReads (Sum)] = reads_sum,
		[(Max)] = reads_max,
		[(Avg)] = reads_avg,
		
		[Writes (Sum)] = writes_sum,
		[(Max)] = writes_max,
		[(Avg)] = writes_avg,
		
		[LReads (Sum)] = lreads_sum,
		[(Max)] = lreads_max,
		[(Avg)] = lreads_avg,
		
		[M Req (Min)] = mgrant_req_min,
		[(Max)] = mgrant_req_max,
		[(Avg)] = mgrant_req_avg,
		
		[M Grnt (Min)] = mgrant_gr_min,
		[(Max)] = mgrant_gr_max,
		[(Avg)] = mgrant_gr_avg,
		
		[M Used (Min)] = mgrant_used_min,
		[(Max)] = mgrant_used_max,
		[(Avg)] = mgrant_used_avg,
		
		DisplayOrderWithinGroup

	FROM #QH_Identifiers q
		INNER JOIN #QH_SARagg qagg
			on q.query_hash = qagg.query_hash
		LEFT OUTER JOIN #SQLStmtStore sss
			ON qagg.PKSQLStmtStoreID = sss.PKSQLStmtStoreID

	UNION ALL

	SELECT GroupIdentifier = '',
		[Statement] = CASE WHEN DisplayOrderWithinGroup = 0 THEN N'' ELSE sss.stmt_xml END,
		[NumUnique] = CASE WHEN DisplayOrderWithinGroup = 0 THEN N'' ELSE NumUnique END,
		[TimesSeen] = CASE WHEN DisplayOrderWithinGroup = 0 THEN N'' ELSE s.TimesSeen END, 
		[FirstSeen] = CASE WHEN DisplayOrderWithinGroup = 0 THEN N'' ELSE s.FirstSeen END, 
		[LastSeen] = CASE WHEN DisplayOrderWithinGroup = 0 THEN N'' ELSE s.LastSeen END,
		[Statii] = CASE WHEN status_running > 0 THEN CONVERT(VARCHAR(20),status_running) + 'xRun' ELSE '' END + 
					CASE WHEN status_runnable > 0 THEN CONVERT(VARCHAR(20),status_runnable) + 'xRabl' ELSE '' END + 
					CASE WHEN status_suspended > 0 THEN CONVERT(VARCHAR(20),status_suspended) + 'xSus' ELSE '' END + 
					CASE WHEN status_other > 0 THEN CONVERT(VARCHAR(20),status_other) + 'xOth' ELSE '' END,
		[Duration (Sum)] = duration_sum,
		[(Max)] = duration_max,
		[(Avg)] = duration_avg,
		[(0-0.5)] = duration_0toP5,
		[(0.5-1)] = duration_P5to1,
		[(1-2)] = duration_1to2,
		[(2-5)] = duration_2to5,
		[(5-10)] = duration_5to10,
		[(10-20)] = duration_10to20,
		[(20+)] = duration_20plus,
		
		[CPU (Sum)] = cpu_sum,
		[(Max)] = cpu_max,
		[(Avg)] = cpu_avg,
		
		[PReads (Sum)] = reads_sum,
		[(Max)] = reads_max,
		[(Avg)] = reads_avg,
		
		[Writes (Sum)] = writes_sum,
		[(Max)] = writes_max,
		[(Avg)] = writes_avg,
		
		[LReads (Sum)] = lreads_sum,
		[(Max)] = lreads_max,
		[(Avg)] = lreads_avg,
		
		[M Req (Min)] = mgrant_req_min,
		[(Max)] = mgrant_req_max,
		[(Avg)] = mgrant_req_avg,
		
		[M Grnt (Min)] = mgrant_gr_min,
		[(Max)] = mgrant_gr_max,
		[(Avg)] = mgrant_gr_avg,
		
		[M Used (Min)] = mgrant_used_min,
		[(Max)] = mgrant_used_max,
		[(Avg)] = mgrant_used_avg,
		
		DisplayOrderWithinGroup
	FROM #Stmt_Identifiers s
		INNER JOIN #St_SARagg sagg
			ON s.PKSQLStmtStoreID = sagg.PKSQLStmtStoreID
		LEFT OUTER JOIN #SQLStmtStore sss
			ON sagg.PKSQLStmtStoreID = sss.PKSQLStmtStoreID

	ORDER BY duration_sum DESC, DisplayOrderWithinGroup ASC;
	*/

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
	RETURN -1;

END CATCH


	RETURN 0;
END