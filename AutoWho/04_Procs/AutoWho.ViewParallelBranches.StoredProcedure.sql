USE [Sentinel]
GO
/****** Object:  StoredProcedure [AutoWho].[ViewParallelBranches]    Script Date: 2/23/2018 9:22:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ViewParallelBranches] 
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

	FILE NAME: AutoWho.ViewParallelBranches.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.ViewParallelBranches

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
	@rqststart	DATETIME,
	@stmtid		BIGINT
)
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	DECLARE 
		@lv__nullstring				NVARCHAR(8),
		@lv__nullint				INT,
		@lv__nullsmallint			SMALLINT,
		@err__RowCount				BIGINT,
		@err__msg					VARCHAR(8000),
		@CXPWaitTypeID				SMALLINT,
		@NULLWaitTypeID				SMALLINT,
		@stmtStartUTC				DATETIME,
		@stmtEndUTC					DATETIME;

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value

	--First, validate that the @spid/@rqst/@rqststart/@stmtid is a valid statement in AutoWho.StatementCaptureTimes in the time window specified
	IF NOT EXISTS (
		SELECT *
		FROM AutoWho.StatementCaptureTimes sct
		WHERE sct.session_id = @spid
		AND sct.request_id = @rqst
		AND sct.TimeIdentifier = @rqststart
		AND sct.PKSQLStmtStoreID = @stmtid
		AND sct.UTCCaptureTime BETWEEN @startUTC AND @endUTC
		)
	BEGIN
		--In SAR but not SCT, probably just need to re-run the Master job to catch SCT up
		RAISERROR('Statement specified in @spid/@rqst/@rqststart/@stmtid parameters was not found in the time range specified by @start/@end. Please check parameters and consider running the ChiRho master job to update recent data.', 16, 1);
		RETURN -1;
	END --IF request identifiers don't exist in SCT

	CREATE TABLE #StmtStats (
		FirstUTCCaptureTime	DATETIME NOT NULL,	--This is irrespective of @startUTC and @endUTC
		LastUTCCaptureTime	DATETIME NOT NULL,
		Observed_duration_sec BIGINT NOT NULL,

		mgrant__dop			SMALLINT NULL,
		[#Samples]			INT NULL,
		rqst__cpu_time		INT NULL,
		rqst__reads			BIGINT NULL,
		rqst__writes		BIGINT NULL,
		rqst__logical_reads	BIGINT NULL,
		min__task_user_objects_page_count	BIGINT NULL,
		max__task_user_objects_page_count	BIGINT NULL,
		min__task_internal_objects_page_count	BIGINT NULL,
		max__task_internal_objects_page_count	BIGINT NULL,
		min__NumTasks		SMALLINT NULL,
		max__NumTasks		SMALLINT NULL,
		requested_memory_kb	BIGINT NULL,
		granted_memory_kb	BIGINT NULL,
		max_used_memory_kb	BIGINT NULL
	);

	INSERT INTO #StmtStats (
		FirstUTCCaptureTime,
		LastUTCCaptureTime,
		Observed_duration_sec
	)
	SELECT TOP 1	--TODO: If a request has executed the same SQL statement (@stmtid) multiple times, we always choose the first one. 
					--We'll re-evaluate the logic we want for this complex case at a later time.
		ss.StatementFirstCaptureUTC,
		ss.LastUTCCaptureTime,
		DATEDIFF(SECOND, ss.StatementFirstCaptureUTC, ss.LastUTCCaptureTime)
	FROM (
		SELECT 
			sct.StatementFirstCaptureUTC,
			LastUTCCaptureTime = MAX(CASE WHEN sct.IsStmtLastCapture = 1 OR sct.IsCurrentLastRowOfBatch = 1 THEN sct.UTCCaptureTime ELSE NULL END)
		FROM AutoWho.StatementCaptureTimes sct
		WHERE sct.session_id = @spid
		AND sct.request_id = @rqst
		AND sct.TimeIdentifier = @rqststart
		AND sct.PKSQLStmtStoreID = @stmtid
		GROUP BY sct.StatementFirstCaptureUTC
	) ss
	ORDER BY ss.StatementFirstCaptureUTC;

	UPDATE targ
	SET mgrant__dop = ss.mgrant__dop,
		[#Samples] = ss.NumRequestCaptures,
		rqst__cpu_time = ss.rqst__cpu_time,
		rqst__reads = ss.rqst__reads,
		rqst__writes = ss.rqst__writes,
		rqst__logical_reads = ss.rqst__logical_reads,
		min__task_user_objects_page_count = ss.min__task_user_objects_page_count,
		max__task_user_objects_page_count = ss.max__task_user_objects_page_count,
		min__task_internal_objects_page_count = ss.min__task_internal_objects_page_count,
		max__task_internal_objects_page_count = ss.max__task_internal_objects_page_count,
		min__NumTasks = ss.min__NumTasks,
		max__NumTasks = ss.max__NumTasks,
		requested_memory_kb = ss.requested_memory_kb,
		granted_memory_kb = ss.granted_memory_kb,
		max_used_memory_kb = ss.max_used_memory_kb
	FROM #StmtStats targ
		CROSS APPLY (
		SELECT 
			[mgrant__dop] = MAX(sar.mgrant__dop),
			[NumRequestCaptures] = COUNT(*),
			[rqst__cpu_time] = MAX(sar.rqst__cpu_time),
			[rqst__reads] = MAX(sar.rqst__reads),
			[rqst__writes] = MAX(sar.rqst__writes),
			[rqst__logical_reads] = MAX(sar.rqst__logical_reads),
			[min__task_user_objects_page_count] = MIN(sar.tempdb__task_user_objects_alloc_page_count - sar.tempdb__task_user_objects_dealloc_page_count),
			[max__task_user_objects_page_count] = MAX(sar.tempdb__task_user_objects_alloc_page_count - sar.tempdb__task_user_objects_dealloc_page_count),
			[min__task_internal_objects_page_count] = MIN(sar.tempdb__task_internal_objects_alloc_page_count - sar.tempdb__task_internal_objects_dealloc_page_count),
			[max__task_internal_objects_page_count] = MAX(sar.tempdb__task_internal_objects_alloc_page_count - sar.tempdb__task_internal_objects_dealloc_page_count),
			[min__NumTasks] = MIN(sar.tempdb__CalculatedNumberOfTasks),
			[max__NumTasks] = MAX(sar.tempdb__CalculatedNumberOfTasks),
			[requested_memory_kb] = MAX(sar.mgrant__requested_memory_kb),
			[granted_memory_kb] = MAX(sar.mgrant__granted_memory_kb),
			[max_used_memory_kb] = MAX(sar.mgrant__max_used_memory_kb)
		FROM AutoWho.SessionsAndRequests sar
		WHERE sar.CollectionInitiatorID = @init
		AND sar.session_id = @spid
		AND sar.request_id = @rqst
		AND sar.rqst__start_time = @rqststart
		AND sar.FKSQLStmtStoreID = @stmtid	--this SHOULD be unnecessary
		AND sar.UTCCaptureTime BETWEEN targ.FirstUTCCaptureTime AND targ.LastUTCCaptureTime
	) ss;

	SELECT 
		@stmtStartUTC = s.FirstUTCCaptureTime,
		@stmtEndUTC = s.LastUTCCaptureTime
	FROM #StmtStats s;

	SELECT 
		@CXPWaitTypeID = dwt.DimWaitTypeID
	FROM AutoWho.DimWaitType dwt
	WHERE dwt.wait_type = 'CXPACKET';

	SELECT 
		@NULLWaitTypeID = dwt.DimWaitTypeID
	FROM AutoWho.DimWaitType dwt
	WHERE dwt.wait_type = @lv__nullstring;
	

	--Let's persist the relevant TAW data, just for this SPID, into a temp table for easy repeated access
	CREATE TABLE #tasks_and_waits (
		[UTCCaptureTime]			[datetime]			NOT NULL,
		[SPIDCaptureTime]			[datetime]			NOT NULL,
		[task_address]				[varbinary](8)		NOT NULL,
		[parent_task_address]		[varbinary](8)		NULL,
		--[session_id]				[smallint]			NOT NULL,	--  Instead of using @lv__nullsmallint, we use -998 bc it has a special value "tasks not tied to spids",
		--															--		and our display logic will take certain action if a spid is = -998
		--[request_id]				[smallint]			NOT NULL,	--  can hold @lv__nullsmallint
		[exec_context_id]			[smallint]			NOT NULL,	--	ditto
		[tstate]					[nchar](1)			NOT NULL,
		[scheduler_id]				[int]				NULL, 
		[context_switches_count]	[bigint]			NOT NULL,	-- 0 if null
		[FKDimWaitType]				[smallint]			NOT NULL,	-- LATCH_xx string has been converted to "subtype(xx)"; null has been converted to @lv__nullstring
		[wait_duration_ms]			[bigint]			NOT NULL,	-- 0 if null

		[wait_duration_ms_adjusted]	[bigint]			NOT NULL,	--When a wait is longer than the interval between 2 AutoWho capture times, we need to 
																	--adjust the wait time to the length of the interval. This allows the interval time to be
																	--additive across captures.

		[wait_special_category]		[tinyint]			NOT NULL,	--we use the special category of "none" if this is NULL
		[wait_order_category]		[tinyint]			NOT NULL, 
		[wait_special_number]		[int]				NULL,		-- node id for CXP, lock type for lock waits, file id for page latches
																	-- left NULL for the temp table, but not-null for the perm table
		[wait_special_tag]			[nvarchar](100)		NULL,		-- varies by wait type:
																		--lock waits --> the mode from resource_description
																		--cxpacket --> the sub-wait type. One of
																				--GetRow
																				--NewRow

																				--PortOpen
																				--PortClose
																				--SynchConsumer
																				--Range
																				--?

																		--page/io latch --> the DBID:FileID:PageID string at first; if DBCC PAGE is run, then the Obj/Idx results
																		-- are placed here.
																	-- left NULL for the temp table, but not-null for the perm table
		[task_priority]				[int]				NOT NULL,	-- = 1 for the top (aka "most relevant/important") task in a parallel query.
																	-- every spid in #sar should have 1. (If not, prob due to timing issues between #sar and #taw capture)
		[blocking_task_address]		[varbinary](8)		NULL,
		[blocking_session_id]		[smallint]			NULL,		--null if = session_id in the base waiting tasks DMV
		[blocking_exec_context_id]	[smallint]			NULL,
		[resource_description]		[nvarchar](3072)	NULL,
		[resource_dbid]				[int]				NULL,		--dbid; populated for lock and latch waits
		[resource_associatedobjid]	[bigint]			NULL,		--the page # for latch waits, the "associatedobjid=" value for lock waits
		[resolution_successful]		[bit]				NULL,
		[resolved_name]				[nvarchar](512)		NULL,
		[IsInferredRow]				[bit]				NOT NULL,
		[RowIsFragmented]			[bit]				NOT NULL,
		[UseFragmentedRowAnyway]	[bit]				NOT NULL,
		[PortOpenIsProducerWait]	[bit]				NOT NULL,
		[PortOpenRowNumber]			[int]				NULL,		--In order to set whether a PortOpen row is a daisy chain PO, we need to uniquely identify a PO row. See logic below.
		[PortOpenIsDaisyChain]		[bit]				NOT NULL,
		[ThreadZeroPortOpenIsAdjacent] [bit]				NOT NULL,	--Thread 0 can often be in PortOpen waits on a non-adjacent branch. If we find that a PortOpen blocking_task_address IS adjacent, we set this to 1
		[ProducerNodeID_FromNewRowOrThreadZero]		[int]	NULL,	--if a task is NewRow waiting on a node, it is a producer for that node.
		[ProducerNodeID_FromSafeConsumerWaits]		[int]	NULL,		--GetRow, PortClose, and Range waits are considered to always be "safe"
																		--meaning they are always consumer-side and always point to a task on the OTHER side of the ADJACENT exchange.
		[ProducerNodeID_FromPortOpen]				[int]	NULL, --but only PortOpen waits that we've validated are legitimately pointing to tasks on the other side of the exchange, and excludes Thread 0
		[ProducerNodeID_FromThreadZeroPortOpen]		[int]	NULL
	);

	CREATE TABLE #TaskStats (
		task_address	VARBINARY(8) NOT NULL,
		FirstSeenUTC		DATETIME NOT NULL,
		LastSeenUTC			DATETIME NOT NULL,
		MaxContextSwitches	BIGINT NOT NULL,
		ProducerNodeID		INT NOT NULL		--32767 means "not yet assigned to a node"
		
	);

	CREATE TABLE #NodeConsumers (
		NodeID			INT NOT NULL,
		task_address	VARBINARY(8) NOT NULL,
		HeuristicID		INT NOT NULL,
		NumDirect		INT NOT NULL		--how many occurrences there are that tie this task_address DIRECTLY to the Node ID
	);
	CREATE CLUSTERED INDEX CL1 ON #NodeConsumers (task_address);
	CREATE NONCLUSTERED INDEX NCL1 ON #NodeConsumers (NodeID) INCLUDE (task_address, HeuristicID, NumDirect);

	CREATE TABLE #NodeProducers (
		NodeID			INT NOT NULL,
		task_address	VARBINARY(8) NOT NULL,
		HeuristicID		INT NOT NULL,
		NumDirect		INT NOT NULL,
		IsSafe			BIT NOT NULL
	);
	CREATE CLUSTERED INDEX CL1 ON #NodeProducers (task_address);
	CREATE NONCLUSTERED INDEX NCL1 ON #NodeProducers (NodeID) INCLUDE (task_address, HeuristicID, NumDirect, IsSafe);
	/*********************************************************************************************************************************************************************************
	********************************************************************************************************************************************************************************

															*BEGIN*  Assemble task and waits info and handle fragmented and missing rows  *BEGIN*

	********************************************************************************************************************************************************************************
	********************************************************************************************************************************************************************************/

	INSERT INTO #tasks_and_waits (
		[UTCCaptureTime],
		[SPIDCaptureTime],
		[task_address],
		[parent_task_address],
		[exec_context_id],
		[tstate],
		[scheduler_id],
		[context_switches_count],
		[FKDimWaitType],
		[wait_duration_ms],
		[wait_duration_ms_adjusted],
		[wait_special_category],
		[wait_order_category],
		[wait_special_number],
		[wait_special_tag],
		[task_priority],
		[blocking_task_address],
		[blocking_session_id],
		[blocking_exec_context_id],
		[resource_description],
		[resource_dbid],
		[resource_associatedobjid],
		[resolution_successful],
		[resolved_name],
		[IsInferredRow],
		[RowIsFragmented],
		[UseFragmentedRowAnyway],
		[PortOpenIsProducerWait],
		[PortOpenIsDaisyChain],
		[ThreadZeroPortOpenIsAdjacent]
	)
	SELECT 
		taw.UTCCaptureTime,
		taw.SPIDCaptureTime,
		task_address,
		parent_task_address,
		exec_context_id,
		tstate,
		scheduler_id,
		context_switches_count,
		taw.FKDimWaitType,
		wait_duration_ms,
		[wait_duration_ms_adjusted] = CASE WHEN taw.wait_duration_ms > DATEDIFF(MILLISECOND, ct.PrevSuccessfulUTCCaptureTime, ct.UTCCaptureTime) 
										THEN DATEDIFF(MILLISECOND, ct.PrevSuccessfulUTCCaptureTime, ct.UTCCaptureTime)
										ELSE taw.wait_duration_ms END,
		wait_special_category,
		wait_order_category,
		wait_special_number,
		wait_special_tag,
		task_priority,
		blocking_task_address,
		blocking_session_id,
		blocking_exec_context_id,
		resource_description,
		resource_dbid,
		resource_associatedobjid,
		taw.resolution_successful,
		taw.resolved_name,
		[IsInferredRow] = 0,
		[RowIsFragmented] = CASE WHEN taw.FKDimWaitType = @CXPWaitTypeID
									AND (taw.wait_special_tag = '?' OR taw.resource_description IS NULL OR taw.wait_special_number = @lv__nullsmallint)
									THEN 1
									ELSE 0
									END,
		[UseFragmentedRowAnyway] = 0,
		[PortOpenIsProducerWait] = 0,		--start off assuming that PortOpen is a consumer-side wait (the common case)
		[PortOpenIsDaisyChain] = 0,			--start off assuming not, though many PO waits will be found to be daisy-chain waits
		[ThreadZeroPortOpenIsAdjacent] = 0	--start off assuming not.
	FROM AutoWho.StatementCaptureTimes sct
		INNER JOIN AutoWho.TasksAndWaits taw
			ON taw.CollectionInitiatorID = @init
			AND taw.UTCCaptureTime = sct.UTCCaptureTime
			AND taw.session_id = sct.session_id
			AND taw.request_id = sct.request_id
		INNER JOIN AutoWho.CaptureTimes ct
			ON ct.CollectionInitiatorID = @init
			AND ct.UTCCaptureTime = sct.UTCCaptureTime
	WHERE sct.session_id = @spid
	AND sct.request_id = @rqst
	AND sct.TimeIdentifier = @rqststart
	AND sct.PKSQLStmtStoreID = @stmtid
	AND sct.UTCCaptureTime BETWEEN @stmtStartUTC AND @stmtEndUTC;
		--We use the statement start/end rather than our requested time window to gather all of the data. 
		--Having the full set of samples allows us to resolve tasks to their producer and consumer nodes more easily.
		--In the logic below, we'll limit the actual time range for metric calculations to
		--@startUTC AND @endUTC;

	CREATE CLUSTERED INDEX CL1 ON #tasks_and_waits (task_address, UTCCaptureTime);


	CREATE TABLE #FragmentedTasks (
		task_address		VARBINARY(8) NOT NULL,
		UTCCaptureTime		DATETIME NOT NULL,
		NumTasksForTime		INT NOT NULL,
		NumFragmentedTasksForTime	INT NOT NULL
	);
	CREATE UNIQUE CLUSTERED INDEX UCL1 ON #FragmentedTasks (task_address, UTCCaptureTime);

	INSERT INTO #FragmentedTasks (
		task_address,
		UTCCaptureTime,
		NumTasksForTime,
		NumFragmentedTasksForTime
	)
	SELECT 
		ss.task_address,
		ss.UTCCaptureTime,
		xapp1.NumTasks,
		xapp1.NumFragmentedTasks
	FROM (
		SELECT DISTINCT
			taw.task_address,
			taw.UTCCaptureTime
		FROM #tasks_and_waits taw
		WHERE taw.RowIsFragmented = 1
	) ss
	CROSS APPLY (
		SELECT
			NumTasks = SUM(1),
			NumFragmentedTasks = SUM(CASE WHEN taw2.RowIsFragmented = 1 THEN 1 ELSE 0 END)
		FROM #tasks_and_waits taw2
		WHERE taw2.UTCCaptureTime = ss.UTCCaptureTime
		AND taw2.task_address = ss.task_address
	) xapp1;

	SET @err__RowCount = ROWCOUNT_BIG();
	IF @err__RowCount > 0
	BEGIN
		SET @err__msg = CONVERT(VARCHAR(20),@err__RowCount) + ' fragmented rows found';
		RAISERROR(@err__msg, 10,1);
	END

	/*
		If a row is fragmented (CXP wait with bad data), we can still make use of it if 
			1. there is no other row with the same task_address for the same UTCCaptureTime.
				In that case, we adjust the row to "running" and assume that the task was transitioning to running 
			2. If multiple rows for a task_address/UTCCaptureTime exist and they are all fragmented, we select 1
				randomly and set it to running.
	*/

	UPDATE targ 
	SET tstate = 'R',
		FKDimWaitType = @NULLWaitTypeID,
		wait_duration_ms = 0,
		wait_special_category = 0,
		wait_order_category = 250,
		wait_special_number = @lv__nullint,
		wait_special_tag = '',
		blocking_task_address = NULL,
		blocking_session_id = @lv__nullsmallint,
		blocking_exec_context_id = @lv__nullsmallint,
		resource_description = NULL,
		resource_dbid = @lv__nullsmallint,
		resource_associatedobjid = @lv__nullsmallint,
		[UseFragmentedRowAnyway] = 1
	FROM #FragmentedTasks ft
		INNER JOIN #tasks_and_waits targ
			ON ft.UTCCaptureTime = targ.UTCCaptureTime
			AND ft.task_address = targ.task_address
	WHERE ft.NumTasksForTime = 1
	AND ft.NumFragmentedTasksForTime = 1;

	SET @err__RowCount = ROWCOUNT_BIG();
	IF @err__RowCount > 0
	BEGIN
		SET @err__msg = CONVERT(VARCHAR(20),@err__RowCount) + ' isolated fragmented rows adjusted';
		RAISERROR(@err__msg, 10,1);
	END


	;WITH cte1 AS (
		SELECT
			rn = ROW_NUMBER() OVER (PARTITION BY task_address, UTCCaptureTime ORDER BY (SELECT NULL)),
			tstate,
			FKDimWaitType,
			wait_duration_ms,
			wait_special_category,
			wait_order_category,
			wait_special_number,
			wait_special_tag,
			blocking_task_address,
			blocking_session_id,
			blocking_exec_context_id,
			resource_description,
			resource_dbid,
			resource_associatedobjid,
			[UseFragmentedRowAnyway]
		FROM #tasks_and_waits targ
		WHERE targ.RowIsFragmented = 1
		--AND UseFragmentedRowAnyway = 0	unnecessary, b/c the next clause limits to just cases where there are multiple tasks
		AND EXISTS (
			SELECT *
			FROM #FragmentedTasks ft
			WHERE ft.UTCCaptureTime = targ.UTCCaptureTime
			AND ft.task_address = targ.task_address
			AND ft.NumTasksForTime > 1
			AND ft.NumTasksForTime = ft.NumFragmentedTasksForTime
		)
	)
	UPDATE cte1
	SET tstate = 'R',
		FKDimWaitType = @NULLWaitTypeID,
		wait_duration_ms = 0,
		wait_special_category = 0,
		wait_order_category = 250,
		wait_special_number = @lv__nullint,
		wait_special_tag = '',
		blocking_task_address = NULL,
		blocking_session_id = @lv__nullsmallint,
		blocking_exec_context_id = @lv__nullsmallint,
		resource_description = NULL,
		resource_dbid = @lv__nullsmallint,
		resource_associatedobjid = @lv__nullsmallint,
		[UseFragmentedRowAnyway] = 1
	WHERE rn = 1;

	SET @err__RowCount = ROWCOUNT_BIG();
	IF @err__RowCount > 0
	BEGIN
		SET @err__msg = CONVERT(VARCHAR(20),@err__RowCount) + ' fragmented rows (that are part of multi-fragment sets) adjusted';
		RAISERROR(@err__msg, 10,1);
	END


	--Inferred row logic
	--Sometimes a task_address is blocked by another task (blocking_task_address) whose row is not present in the results for that UTCCaptureTime
	--If that "missing blocking_task_address" has a corresponding row in a different UTCCaptureTime, then we can take the non-volatile attributes from
	--another of those rows and use them to construct an "inferred row" for the original UTCCaptureTime. We create this task as "running"
	;WITH MissingRows AS (
		SELECT DISTINCT
			taw.UTCCaptureTime,
			taw.SPIDCaptureTime,
			taw.blocking_task_address
		FROM #tasks_and_waits taw
		WHERE taw.blocking_task_address IS NOT NULL
		AND taw.blocking_session_id = @lv__nullsmallint
		AND NOT EXISTS (
			SELECT *
			FROM #tasks_and_waits taw2
			WHERE taw2.UTCCaptureTime = taw.UTCCaptureTime
			AND taw2.task_address = taw.blocking_task_address
			)
	),
	ConstructMissingRows AS (
		SELECT 
			mr.UTCCaptureTime,
			mr.SPIDCaptureTime,
			mr.blocking_task_address,
			xapp1.parent_task_address,
			xapp1.exec_context_id,
			xapp1.scheduler_id
		FROM MissingRows mr
			OUTER APPLY (
				SELECT TOP 1
					taw3.parent_task_address,
					taw3.exec_context_id,
					taw3.scheduler_id
				FROM #tasks_and_waits taw3
				WHERE mr.blocking_task_address = taw3.task_address
			) xapp1
	)
	INSERT INTO #tasks_and_waits (
		[UTCCaptureTime],
		[SPIDCaptureTime],
		[task_address],
		[parent_task_address],
		[exec_context_id],
		[tstate],
		[scheduler_id],
		[context_switches_count],
		[FKDimWaitType],
		[wait_duration_ms],
		[wait_duration_ms_adjusted],
		[wait_special_category],
		[wait_order_category],
		[wait_special_number],
		[wait_special_tag],
		[task_priority],
		[IsInferredRow],
		[RowIsFragmented],
		[UseFragmentedRowAnyway],
		[PortOpenIsProducerWait],
		[PortOpenIsDaisyChain]
	)
	SELECT
		c.UTCCaptureTime,
		c.SPIDCaptureTime,
		c.blocking_task_address,
		c.parent_task_address,
		c.exec_context_id,
		[tstate] = 'R',
		c.scheduler_id,
		[context_switches_count] = 0,		--we don't even guess. It doesn't matter anyway since metrics are always built off of MAX
		@NULLWaitTypeID,
		[wait_duration_ms] = 0,
		[wait_duration_ms_adjusted] = 0,
		[wait_special_category] = 0,
		[wait_order_category] = 250,
		[wait_special_number] = @lv__nullint,
		[wait_special_tag] = '',
		[task_priority] = 32767,
		[IsInferredRow] = 1,
		[RowIsFragmented] = 0,
		[UseFragmentedRowAnyway] = 0,
		[PortOpenIsProducerWait] = 0,
		[PortOpenIsDaisyChain] = 0
	FROM ConstructMissingRows c;

	SET @err__RowCount = ROWCOUNT_BIG();
	IF @err__RowCount > 0
	BEGIN
		SET @err__msg = CONVERT(VARCHAR(20),@err__RowCount) + ' inferred rows inserted';
		RAISERROR(@err__msg, 10,1);
	END


	INSERT INTO #TaskStats (
		task_address,
		FirstSeenUTC,
		LastSeenUTC,
		MaxContextSwitches,
		ProducerNodeID
	)
	SELECT 
		taw.task_address,
		[FirstSeenUTC] = MIN(taw.UTCCaptureTime),
		[LastSeenUTC] = MAX(taw.UTCCaptureTime),
		[MaxContextSwitches] = MAX(taw.context_switches_count),
		[ProducerNodeID] = 32767		--set all tasks to our special "not yet assigned to a node" code
	FROM #tasks_and_waits taw
	WHERE taw.RowIsFragmented = 0
	OR (taw.RowIsFragmented = 1 AND taw.UseFragmentedRowAnyway = 1)
	GROUP BY taw.task_address;

	DECLARE @Thread0TaskAddress VARBINARY(8);
	SELECT
		@Thread0TaskAddress = ss.task_address
	FROM (
		SELECT 
			taw.task_address,
			rn = ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC)
		FROM #tasks_and_waits taw
		WHERE (taw.exec_context_id = 0 OR taw.parent_task_address IS NULL)
		AND (
				taw.RowIsFragmented = 0
				OR (taw.RowIsFragmented = 1 AND taw.UseFragmentedRowAnyway = 1)
			)
		GROUP BY taw.task_address
	) ss
	WHERE rn = 1;	--just in case the data is so weird that somehow there are 2 different task_addresses for Thread 0? (Yeah, seems paranoid)

	--TODO: if @Thread0TaskAddress is NULL, raise a critical error. We can't proceed if the data is that mucked up.
	/*********************************************************************************************************************************************************************************
	********************************************************************************************************************************************************************************

															*END*  Assemble task and waits info and handle fragmented and missing rows  *END*

	********************************************************************************************************************************************************************************
	********************************************************************************************************************************************************************************/




	/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

																*BEGIN*  Assign tasks to nodes based on direct relationships  *BEGIN*
																			NOTE: This section excludes most PortOpen logic

	~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
	INSERT INTO #NodeProducers (
		NodeID,
		task_address,
		HeuristicID,
		NumDirect,
		IsSafe
	)
	SELECT -1,
		@Thread0TaskAddress,
		1,
		1,
		1;

	--Now, find the Node ID that Thread 0 is a consumer on (always the left-most Gather Streams)
	INSERT INTO #NodeConsumers (
		NodeID,
		task_address,
		HeuristicID
	)
	SELECT
		ss.wait_special_number,
		@Thread0TaskAddress,
		2
	FROM (
		SELECT
			taw.wait_special_number,
			rn = ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC)
		FROM #tasks_and_waits taw
		WHERE taw.task_address = @Thread0TaskAddress
		AND (
				taw.RowIsFragmented = 0
				OR (taw.RowIsFragmented = 1 AND taw.UseFragmentedRowAnyway = 1)
			)
		AND taw.wait_special_tag IN ('PortOpen', 'PortClose', 'Range', 'GetRow', 'SynchConsumer')
		GROUP BY taw.wait_special_number
	) ss
	WHERE rn = 1;	--Thread 0 never consumes from more than 1 node. If the data has that, it is wrong, and we take the most common Node ID

	SET @err__RowCount = ROWCOUNT_BIG();
	IF @err__RowCount = 0
	BEGIN
		--Could not find a Thread 0 consumer-side wait that was valid? See if any tasks had NewRow (or producer-side PortOpen!) waits on it
		INSERT INTO #NodeConsumers (
			NodeID,
			task_address,
			HeuristicID
		)
		SELECT 
			ss.wait_special_number,
			@Thread0TaskAddress,
			3
		FROM (
			SELECT
				taw.wait_special_number,
				rn = ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC)
			FROM #tasks_and_waits taw
			WHERE taw.blocking_task_address = @Thread0TaskAddress
			AND taw.RowIsFragmented = 0
			AND taw.wait_special_tag IN ('NewRow', 'PortOpen')
			GROUP BY taw.wait_special_number
		) ss
		WHERE rn = 1;

		SET @err__RowCount = ROWCOUNT_BIG();
		IF @err__RowCount = 0
		BEGIN
			--TODO: For now, we warn. May need to adjust this to an error if it causes too many problems.
			SET @err__msg = 'Could not find the node Thread 0 consumes from.';
			RAISERROR(@err__msg, 10,1);
		END
	END --if we couldn't tie Thread 0 to a consumer node ID from consumer waits.


	--Now, let's do NewRow waits
	INSERT INTO #NodeProducers (
		NodeID,
		task_address,
		HeuristicID,
		NumDirect,
		IsSafe
	)
	SELECT
		taw.wait_special_number,
		taw.task_address,
		4,
		COUNT(*),
		0			--if the data somehow has multiple producer Node IDs for a task, we'll choose the most numerous one, and mark that safe.
	FROM #tasks_and_waits taw
	WHERE (
			taw.RowIsFragmented = 0
			OR (taw.RowIsFragmented = 1 AND taw.UseFragmentedRowAnyway = 1)
		)
	AND taw.wait_special_tag = 'NewRow'
	AND taw.wait_special_number IS NOT NULL
	GROUP BY taw.wait_special_number,
		taw.task_address;

	--NewRow waits can be used to tie the blocking_task_address to a NodeID as a consumer
	INSERT INTO #NodeConsumers (
		NodeID,
		task_address,
		HeuristicID,
		NumDirect
	)
	SELECT
		taw.wait_special_number,
		taw.blocking_task_address,
		5,
		COUNT(*)
	FROM #tasks_and_waits taw
	WHERE (
			taw.RowIsFragmented = 0
			OR (taw.RowIsFragmented = 1 AND taw.UseFragmentedRowAnyway = 1)
		)
	AND taw.wait_special_tag = 'NewRow'
	AND taw.wait_special_number IS NOT NULL
	AND taw.blocking_task_address IS NOT NULL
	AND taw.blocking_task_address <> @Thread0TaskAddress	--we already handled this special case further above
	GROUP BY taw.wait_special_number,
		taw.task_address;


	--Now, tie task_addresses to their Producer Node ID via "safe" consumer waits, i.e. waits that we KNOW are consumer-side,
	--and where we know that the blocking_task_address refers to a task on the other/producer side of the Node ID
	INSERT INTO #NodeProducers (
		NodeID,
		task_address,
		HeuristicID,
		NumDirect,
		IsSafe
	)
	SELECT 
		waiter.wait_special_number,
		waiter.blocking_task_address,
		6,
		NumRows = COUNT(*),
		0
	FROM #tasks_and_waits waiter
	WHERE waiter.wait_special_tag IN ('GetRow', 'PortClose', 'Range')
	AND waiter.blocking_session_id = @lv__nullsmallint
	AND waiter.blocking_task_address IS NOT NULL
	AND (
		waiter.RowIsFragmented = 0
		OR (waiter.RowIsFragmented = 1 AND waiter.UseFragmentedRowAnyway = 1)
	)
	GROUP BY waiter.blocking_task_address,
		waiter.wait_special_number;


	--Now, tie task_addresses to their Consumer Node ID(s) via "safe" consumer waits. Note that this includes SynchConsumer
	INSERT INTO #NodeConsumers (
		NodeID,
		task_address,
		HeuristicID,
		NumDirect
	)
	SELECT
		ss.wait_special_number,
		ss.TAddr,
		7,
		NumRows = COUNT(*)
	FROM (
		SELECT 
			taw.wait_special_number,
			[TAddr] = taw.task_address
		FROM #tasks_and_waits taw
		WHERE taw.wait_special_tag IN ('GetRow', 'PortClose', 'Range', 'SynchConsumer')
		--AND taw.blocking_session_id = @lv__nullsmallint
		--AND taw.blocking_task_address IS NOT NULL
		AND (
			taw.RowIsFragmented = 0
			OR (taw.RowIsFragmented = 1 AND taw.UseFragmentedRowAnyway = 1)
		)

		UNION ALL

		--The blocker of a SynchConsumer wait is always a task on the same side of the exchange.
		SELECT
			taw2.wait_special_number,
			[TAddr] = taw2.blocking_task_address
		FROM #tasks_and_waits taw2
		WHERE taw2.wait_special_tag = 'SynchConsumer'
		AND taw2.blocking_session_id = @lv__nullsmallint
		AND taw2.blocking_task_address IS NOT NULL
		AND (
			taw2.RowIsFragmented = 0
			OR (taw2.RowIsFragmented = 1 AND taw2.UseFragmentedRowAnyway = 1)
		)
	) ss
	GROUP BY ss.wait_special_number,
		ss.TAddr;


	/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

																*END*  Assign tasks to nodes based on direct relationships  *END*
																		NOTE: This section excludes most PortOpen logic

	~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

	/*
	AARON: LEFT OFF HERE
	Next, I should have a section that takes the above data from the direct relationships and recursively finds siblings.
	I think that may mean 1 outer loop and 2 inner loops. The idea is that each inner loop looks for sibling consumers (loop 1) or sibling producers (loop 2)
	As long as at least 1 row is found by a loop, the loop should keep going. If any rows are found by either inner loop, the outer loop should keep going.
	I need to code this out and think through it a bit more, but at least the general idea is on paper now.

	Once I have sibling logic resolved, then there is a higher chance for the PortOpen logic below to work correctly.

	*/






	/*********************************************************************************************************************************************************************************
	********************************************************************************************************************************************************************************

															*BEGIN*  Run rules to handle PortOpen waits  *BEGIN*

	********************************************************************************************************************************************************************************
	********************************************************************************************************************************************************************************/

	/*
		PortOpen waits are more complex and assuming that they are consumer-side and waiting on tasks in an adjacent branch on the other side of the exchange causes problems. 
		For example:

			1) Thread 0 often is PortOpen-waiting on tasks in a non-adjacent branch

			2) (for tasks other than Thread 0) PortOpen waits are often in a daisy-chain structure, with only the
				final task in the chain actually waiting on a blocking_task_address that is in a different branch.

			3) There *do* seem to be something like producer-side PortOpen waits; this is not common but it does
				seem to happen.

		Thus, our logic for handling these waits needs to be nuanced and tread carefully. Since this proc is built on the idea that a given parallel query execution
		will have been sampled by the collector a number of times (b/c shorter parallel queries aren't as interesting as longer ones), the multiplicity of samples
		of tasks and waits data means that the above logic on the simpler wait types will have probably identified which Node IDs each task is a producer and consumer for.
		There may be a few a tasks that we still don't have nodes identified for, and PortOpen can help us there.

	*/

	--First, let's try to identify PortOpen waits that are producer-side. For now, we are pretty conservative. A PortOpen wait is only declared producer-side if the Node ID
	--a task_address is PortOpen-waiting on is already known (by the above logic) to be its producer node ID from other samples taken. There are other tests we could do,
	--e.g. if the # of tasks in a branch would be > the statement's DOP if all PortOpen waits on a node were considered to be consumer-side. We may add more logic in later.
	;WITH cte1 AS (
		SELECT 
			taw.task_address,
			taw.UTCCaptureTime,
			taw.PortOpenIsProducerWait
		FROM #tasks_and_waits taw
		WHERE (
			taw.RowIsFragmented = 0
			OR (taw.RowIsFragmented = 1 AND taw.UseFragmentedRowAnyway = 1)
		)
		AND taw.wait_special_tag = 'PortOpen'
		AND (taw.parent_task_address IS NOT NULL AND ISNULL(taw.exec_context_id,255) <> 0)		--ignore Thread 0; its PortOpen waits are NEVER producer-side, of course
		AND taw.wait_special_number = COALESCE(taw.ProducerNodeID_FromNewRowOrThreadZero, taw.ProducerNodeID_FromSafeConsumerWaits)
	)
	UPDATE cte1
	SET PortOpenIsProducerWait = 1;

	SET @err__RowCount = ROWCOUNT_BIG();
	IF @err__RowCount > 0
	BEGIN
		SET @err__msg = 'NOTE: ' + CONVERT(VARCHAR(20),@err__RowCount) + ' rows identified as in a producer-side PortOpen wait';
		RAISERROR(@err__msg, 10,1);
	END


	--Ok, now that we have identified all producer-side PortOpen waits (fingers crossed), we can assume that all other
	--PortOpen waits are consumer-side. This lets us populate #NodeConsumers, which also has value for identifying further Producer Node IDs
	--because this gives us info about a task's siblings.
	INSERT INTO #NodeConsumers (
		NodeID,
		task_address,
		HeuristicID
	)
	SELECT DISTINCT
		taw.wait_special_number,
		taw.task_address,
		[HeuristicID] = 6
	FROM #tasks_and_waits taw
	WHERE (
			taw.RowIsFragmented = 0
			OR (taw.RowIsFragmented = 1 AND taw.UseFragmentedRowAnyway = 1)
		)
	AND taw.wait_special_tag = 'PortOpen'
	AND (taw.parent_task_address IS NOT NULL AND ISNULL(taw.exec_context_id,255) <> 0)		--ignore Thread 0; its PortOpen waits can sometimes be on tasks in a NON-adjacent branch
	AND taw.PortOpenIsProducerWait = 0;

	SET @err__RowCount = ROWCOUNT_BIG();
	SET @err__msg = CONVERT(VARCHAR(20),@err__RowCount) + ' rows connected to nodes as consumers via PortOpen waits';
	RAISERROR(@err__msg, 10,1);


	--Since a task_address can appear multiple times in the same UTCCaptureTime with the same wait, we need to unique-ify each row so that the below
	--UPDATE can work correctly.
	;WITH cte1 AS (
		SELECT 
			PortOpenRowNumber,
			rn = ROW_NUMBER() OVER (PARTITION BY taw.UTCCaptureTime, taw.task_address ORDER BY (SELECT NULL))	--currently no need to assign an order to them.
		FROM #tasks_and_waits taw
		WHERE taw.wait_special_tag = 'PortOpen'
	)
	UPDATE cte1
	SET PortOpenRowNumber = rn;

	/*
		Now, handle daisy-chains. A daisy-chain PortOpen wait occurs when a task_address (*besides Thread 0*) is in a PortOpen wait (and PortOpenIsProducerWait=0)
		and its blocking_task_address refers to a task on the same side (i.e. consumer side) of the exchange. (I use the term "daisy-chain" because typically there
		are a string of PortOpen waits all referring to the "next one in line").
		There are a variety of ways that we can determine whether the blocking_task_address is on the same side of the exchange:

			1. The blocking_task_address is also in a PortOpen wait on the same NodeID (and the blocking_task_address is NOT a known PortOpen producer wait)
			2. The blocking_task_address is in another CXPACKET wait that is known to be consumer side (GetRow, SynchConsumer, PortClose, Range), and on the same Node ID
			3. The blocking_task_address is a sibling of the task_address
	*/

	;WITH IdentifyDaisyChainPO AS (
		SELECT 
			waiter.task_address,
			waiter.UTCCaptureTime,
			waiter.PortOpenRowNumber
		FROM #tasks_and_waits waiter
			INNER JOIN #tasks_and_waits blocker
				ON waiter.blocking_task_address = blocker.task_address
				AND waiter.UTCCaptureTime = blocker.UTCCaptureTime
				AND waiter.blocking_session_id = @lv__nullsmallint
		WHERE (
				waiter.RowIsFragmented = 0
				OR (waiter.RowIsFragmented = 1 AND waiter.UseFragmentedRowAnyway = 1)
			)
		AND (
			blocker.RowIsFragmented = 0
			OR (blocker.RowIsFragmented = 1 AND blocker.UseFragmentedRowAnyway = 1)
		)
		AND (waiter.parent_task_address IS NULL OR ISNULL(waiter.exec_context_id,255) <> 0)		--NOT Thread 0
		AND waiter.wait_special_tag = 'PortOpen'		
		AND waiter.PortOpenIsProducerWait = 0

		--blocker is on the same side of the exchange
		AND (
			(
				blocker.wait_special_tag IN ('PortOpen', 'SynchConsumer','GetRow', 'SynchConsumer','Range')
				AND blocker.PortOpenIsProducerWait = 0
				AND waiter.wait_special_number = blocker.wait_special_number	--Node IDs match
			)
			OR
			--the waiter task consumes from the same NodeID as the blocker task, i.e. they are siblings
			EXISTS (
				SELECT * 
				FROM #NodeConsumers nc1
				WHERE nc1.task_address = waiter.task_address
				AND nc1.NodeID IN (SELECT nc2.task_address FROM #NodeConsumers nc2 WHERE nc2.task_address = blocker.task_address)
				)
			OR
			--the waiter task produces to the same NodeID as the blocker task, i.e. they are siblings
			EXISTS (
				SELECT *
				FROM #tasks_and_waits taw1
				WHERE taw1.task_address = waiter.task_address
				AND EXISTS (
					SELECT * 
					FROM #tasks_and_waits taw2
					WHERE taw2.task_address = blocker.task_address
					AND COALESCE(taw1.ProducerNodeID_FromNewRowOrThreadZero, taw1.ProducerNodeID_FromSafeConsumerWaits)
						= COALESCE(taw2.ProducerNodeID_FromNewRowOrThreadZero, taw2.ProducerNodeID_FromSafeConsumerWaits)
					)
			)
		) --end of complex "blocker is on the same side of the exchange" section
	)
	UPDATE targ 
	SET PortOpenIsDaisyChain = 1
	FROM #tasks_and_waits targ
		INNER JOIN IdentifyDaisyChainPO i
			ON targ.task_address = i.task_address
			AND targ.UTCCaptureTime = i.UTCCaptureTime
			AND targ.PortOpenRowNumber = i.PortOpenRowNumber
	WHERE (
		targ.RowIsFragmented = 0
		OR (targ.RowIsFragmented = 1 AND targ.UseFragmentedRowAnyway = 1)
	)
	AND (targ.parent_task_address IS NULL OR ISNULL(targ.exec_context_id,255) <> 0)		--NOT Thread 0
	AND targ.wait_special_tag = 'PortOpen'		
	AND targ.PortOpenIsProducerWait = 0;

	SET @err__RowCount = ROWCOUNT_BIG();
	SET @err__msg = CONVERT(VARCHAR(20),@err__RowCount) + ' rows found to be in a PortOpen daisy-chain';
	RAISERROR(@err__msg, 10,1);


	--Ok, any other consumer-side PortOpen waits provide additional information we can use to tie task_addresses to their producer nodes.
	UPDATE targ 
	SET ProducerNodeID_FromPortOpen = ss2.PossibleProducerNodeID
	FROM #tasks_and_waits targ
		INNER JOIN (
			SELECT 
				task_address,
				PossibleProducerNodeID,
				--This row-numbering logic is just in the (should be impossible!) event where the same task_address is somehow
				--causing consumer-side waits on multiple (i.e. different) node IDs. We assume that the most common one is the correct one.
				rn = ROW_NUMBER() OVER (PARTITION BY task_address ORDER BY NumRows DESC)
			FROM (
				SELECT 
					blocker.task_address,
					[PossibleProducerNodeID] = waiter.wait_special_number,
					NumRows = COUNT(*)
				FROM #tasks_and_waits blocker
					 INNER JOIN #tasks_and_waits waiter
						ON blocker.task_address = waiter.blocking_task_address
						AND blocker.UTCCaptureTime = waiter.UTCCaptureTime
						AND waiter.blocking_session_id = @lv__nullsmallint
						AND waiter.wait_special_tag = 'PortOpen'
						AND waiter.PortOpenIsDaisyChain = 0
						AND waiter.PortOpenIsProducerWait = 0
						AND (waiter.parent_task_address IS NOT NULL AND ISNULL(waiter.exec_context_id,255) <> 0)	--exclude Thread 0 here.
				WHERE (
					blocker.RowIsFragmented = 0
					OR (blocker.RowIsFragmented = 1 AND blocker.UseFragmentedRowAnyway = 1)
				)
				AND (
					blocker.RowIsFragmented = 0
					OR (blocker.RowIsFragmented = 1 AND blocker.UseFragmentedRowAnyway = 1)
				)
				GROUP BY blocker.task_address,
					waiter.wait_special_number
			) ss
		) ss2
			ON targ.task_address = ss2.task_address
			AND ss2.rn = 1;

	SET @err__RowCount = ROWCOUNT_BIG();
	SET @err__msg = CONVERT(VARCHAR(20),@err__RowCount) + ' rows connected to producer nodes via PortOpen waits that are clearly blocked on producer tasks';
	RAISERROR(@err__msg, 10,1);

	/*
		For Thread 0 Port Open waits, we can set the [ThreadZeroPortOpenIsAdjacent] bit flag to 1 if we have other info that indicates the blocking task is adjacent.
		Note that Thread 0's PortOpen waits are ALWAYS on its Gather Streams node, even if the wait is for a blocking task that is deep into the plan and not adjacent.
		
		Thus, we look for cases where 
			1. The blocking task has as its Producer Node the node that the Thread 0 PortOpen wait is on
			2. OR the blocking task has a sibling (another task with the same consumer node) whose producer node ID is Thread 0's PortOpen wait Node ID
	*/
	;WITH cte1 AS (
		SELECT 
			taw0.UTCCaptureTime,
			taw0.task_address,
			taw0.PortOpenRowNumber
		FROM #tasks_and_waits taw0
		WHERE (
			taw0.RowIsFragmented = 0
			OR (taw0.RowIsFragmented = 1 AND taw0.UseFragmentedRowAnyway = 1)
		)
		AND (taw0.parent_task_address IS NULL	--*IS* thread 0
			OR taw0.exec_context_id = 0
		)
		AND taw0.wait_special_tag = 'PortOpen'
		AND (
			--The blocking task has as its Producer Node the node that the Thread 0 PortOpen wait is on
			EXISTS (
				SELECT *
				FROM #tasks_and_waits taw2
				WHERE taw2.task_address = taw0.blocking_task_address
				AND taw0.wait_special_number = COALESCE(taw2.ProducerNodeID_FromNewRowOrThreadZero, taw2.ProducerNodeID_FromSafeConsumerWaits, taw2.ProducerNodeID_FromPortOpen)
			)

			OR --the blocking task has a sibling (another task with the same consumer node) whose producer node ID is Thread 0's PortOpen wait Node ID
			EXISTS (
				SELECT 
					ncOther.task_address
				FROM #NodeConsumers ncBlocker
					INNER JOIN #NodeConsumers ncOther
						ON ncBlocker.NodeID = ncOther.NodeID
						AND ncBlocker.task_address <> ncOther.task_address
				WHERE ncBlocker.task_address = taw0.blocking_task_address
				AND EXISTS (
					--At least one of the other tasks that are consumers of one of the blocking task's consumption nodes
					--has as its producer node the wait Node ID that the Thread 0
					SELECT *
					FROM #tasks_and_waits taw3
					WHERE taw3.task_address = ncOther.task_address
					AND taw0.wait_special_number = COALESCE(taw3.ProducerNodeID_FromNewRowOrThreadZero, taw3.ProducerNodeID_FromSafeConsumerWaits, taw3.ProducerNodeID_FromPortOpen)
				)
			)
		)
	) --end of CTE definition
	UPDATE targ 
	SET ThreadZeroPortOpenIsAdjacent = 1
	FROM #tasks_and_waits targ
		INNER JOIN cte1 c
			ON targ.UTCCaptureTime = c.UTCCaptureTime
			AND targ.task_address = c.task_address
			AND targ.PortOpenRowNumber = c.PortOpenRowNumber;

	SET @err__RowCount = ROWCOUNT_BIG();
	SET @err__msg = CONVERT(VARCHAR(20),@err__RowCount) + ' rows found where Thread 0 is PortOpen-waiting on an adjacent task';
	RAISERROR(@err__msg, 10,1);

	--For Thread 0 PortOpen waits that we've found to be blocked on a task that is truly in an adjacent branch,
	-- use that info to connect the blocking task_address(es) to the Gather Streams node as their producer node.
	UPDATE targ 
	SET ProducerNodeID_FromThreadZeroPortOpen = ss2.PossibleProducerNodeID
	FROM #tasks_and_waits targ
		INNER JOIN (
			SELECT 
				task_address,
				PossibleProducerNodeID,
				--This row-numbering logic is just in the (should be impossible!) event where the same task_address is somehow
				--causing consumer-side waits on multiple (i.e. different) node IDs. We assume that the most common one is the correct one.
				rn = ROW_NUMBER() OVER (PARTITION BY task_address ORDER BY NumRows DESC)
			FROM (
				SELECT 
					blocker.task_address,
					[PossibleProducerNodeID] = waiter.wait_special_number,
					NumRows = COUNT(*)
				FROM #tasks_and_waits blocker
					 INNER JOIN #tasks_and_waits waiter
						ON blocker.task_address = waiter.blocking_task_address
						AND blocker.UTCCaptureTime = waiter.UTCCaptureTime
						AND waiter.blocking_session_id = @lv__nullsmallint
						AND waiter.wait_special_tag = 'PortOpen'
						AND waiter.ThreadZeroPortOpenIsAdjacent = 1
						AND (waiter.parent_task_address IS NULL AND waiter.exec_context_id = 0)	--JUST Thread 0 here
				WHERE (
					blocker.RowIsFragmented = 0
					OR (blocker.RowIsFragmented = 1 AND blocker.UseFragmentedRowAnyway = 1)
				)
				AND (
					blocker.RowIsFragmented = 0
					OR (blocker.RowIsFragmented = 1 AND blocker.UseFragmentedRowAnyway = 1)
				)
				GROUP BY blocker.task_address,
					waiter.wait_special_number
			) ss
		) ss2
			ON targ.task_address = ss2.task_address
			AND ss2.rn = 1;

	SET @err__RowCount = ROWCOUNT_BIG();
	SET @err__msg = CONVERT(VARCHAR(20),@err__RowCount) + ' rows connected to a Producer node via a Thread 0 PortOpen wait on an adjacent task';
	RAISERROR(@err__msg, 10,1);

	/*********************************************************************************************************************************************************************************
	********************************************************************************************************************************************************************************

															*END*  Run rules to handle PortOpen waits  *END*

	********************************************************************************************************************************************************************************
	********************************************************************************************************************************************************************************/


	--Still need to rework this query after I get the PortOpen logic the way I want.
	UPDATE targ 
	SET ProducerNodeID = ss3.CalculatedProducerNodeID
	FROM #TaskStats targ
		INNER JOIN (
			SELECT 
				task_address,
				CalculatedProducerNodeID
			FROM (
				SELECT 
					task_address,
					CalculatedProducerNodeID,
					rn = ROW_NUMBER() OVER (PARTITION BY task_address ORDER BY NumRows DESC)
				FROM (
					SELECT
						taw.task_address,
						CalculatedProducerNodeID = COALESCE(taw.ProducerNodeID_FromNewRowOrThreadZero,
															taw.ProducerNodeID_FromSafeConsumerWaits, 
															taw.ProducerNodeID_FromPortOpen,
															taw.ProducerNodeID_FromThreadZeroPortOpen),
						NumRows = COUNT(*)
					FROM #tasks_and_waits taw
					WHERE taw.RowIsFragmented = 0
					GROUP BY taw.task_address,
						COALESCE(taw.ProducerNodeID_FromNewRowOrThreadZero,
									taw.ProducerNodeID_FromSafeConsumerWaits, 
									taw.ProducerNodeID_FromPortOpen,
									taw.ProducerNodeID_FromThreadZeroPortOpen)
				) ss
				WHERE CalculatedProducerNodeID IS NOT NULL
			) ss2
			WHERE rn = 1
		) ss3
			ON targ.task_address = ss3.task_address;


	--DEBUG queries --
	SELECT * 
	FROM #StmtStats

	SELECT * 
	FROM #TaskStats;

	SELECT * 
	FROM #NodeConsumers

	SELECT 
		taw.task_address,
		[ECID] = taw.exec_context_id,
		[SubWT] = taw.wait_special_tag,
		[Node] = taw.wait_special_number,
		WT = (SELECT dwt.wait_type_short FROM AutoWho.DimWaitType dwt WHERE dwt.DimWaitTypeID = taw.FKDimWaitType),
		[dur_ms] = taw.wait_duration_ms,
		[adj_ms] = taw.wait_duration_ms_adjusted,
		[Becid] = taw.blocking_exec_context_id,
		[RowFrag] = taw.RowIsFragmented,
		[UseFrag] = taw.UseFragmentedRowAnyway,
		[PID_NRT0] = taw.ProducerNodeID_FromNewRowOrThreadZero,
		[PID_SfCn] = taw.ProducerNodeID_FromSafeConsumerWaits,
		[PID_PO] = taw.ProducerNodeID_FromPortOpen,
		[PID_POT0] = taw.ProducerNodeID_FromThreadZeroPortOpen
	FROM #tasks_and_waits taw
	ORDER BY taw.UTCCaptureTime, taw.exec_context_id;


	
	--DEBUG queries --

	RETURN -1;
END 
