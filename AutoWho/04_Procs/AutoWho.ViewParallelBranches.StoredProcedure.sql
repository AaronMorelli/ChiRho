SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [AutoWho].[ViewParallelBranches] 
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
		@CXPWaitTypeID				SMALLINT;

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value

	--First, validate that the @spid/@rqst/@rqststart/@stmtid is a valid statement in AutoWho.StatementCaptureTimes
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
	SELECT TOP 1	--TODO: If a request has executed the same SQL statement multiple times, we always choose the first one. We'll have to fix this at a later time.
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
		[#Samples] = ss.NumRows,
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
			[NumRows] = COUNT(*),
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
		AND sar.UTCCaptureTime BETWEEN targ.FirstUTCCaptureTime AND targ.LastUTCCaptureTime
	) ss;

	SELECT 
		@CXPWaitTypeID = dwt.DimWaitTypeID
	FROM AutoWho.DimWaitType dwt
	WHERE dwt.wait_type = 'CXPACKET';
	

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
		[RowIsFragmented]			[bit]				NOT NULL,
		[ProducerNodeID_ImmediatelyIdentified]		[int]	NULL,
		[ProducerNodeID_FromGetRowWaits]			[int]	NULL,
		[ProducerNodeID_ThirdLevelIdentification]	[int]	NULL
	);

	CREATE TABLE #TaskStats (
		task_address	VARBINARY(8) NOT NULL,
		FirstSeenUTC		DATETIME NOT NULL,
		LastSeenUTC			DATETIME NOT NULL,
		MaxContextSwitches	BIGINT NOT NULL,
		ProducerNodeID		INT NOT NULL		--32767 means "not yet assigned to a node"
		
	);



	/*********************************************************************************************************************************************************************************
	 ********************************************************************************************************************************************************************************

																*BEGIN*  Assemble task and waits info and handle fragmented rows  *BEGIN*

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
		[RowIsFragmented]
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
		[RowIsFragmented] = CASE WHEN taw.FKDimWaitType = @CXPWaitTypeID
									AND (taw.wait_special_tag = '?' OR taw.resource_description IS NULL OR taw.wait_special_number = @lv__nullsmallint)
									THEN 1
									ELSE 0
									END
	FROM AutoWho.StatementCaptureTimes sct
		INNER JOIN AutoWho.TasksAndWaits taw
			ON taw.CollectionInitiatorID = @init
			AND taw.UTCCaptureTime = sct.UTCCaptureTime
			AND taw.session_id = sct.session_id
			AND taw.request_id = sct.request_id
	WHERE sct.session_id = @spid
	AND sct.request_id = @rqst
	AND sct.TimeIdentifier = @rqststart
	AND sct.PKSQLStmtStoreID = @stmtid
	AND sct.UTCCaptureTime BETWEEN @startUTC AND @endUTC;

	/*
		If a row is fragmented (CXP wait with bad data), we can still make use of it if 
			1. there is no other row with the same task_address for the same UTCCaptureTime.
				In that case, we adjust the row to "running" and assume that the task was transitioning to running 
			2. If multiple rows for a task_address/UTCCaptureTime exist and they are all fragmented, we select 1
				randomly and set it to running.
	*/
	UPDATE targ 
	SET tstate = 'R',
		FKDimWaitType = 1,
		wait_duration_ms = 0,
		wait_special_category = 0,
		wait_order_category = 250,
		wait_special_number = @lv__nullsmallint,
		wait_special_tag = '',
		blocking_task_address = NULL,
		blocking_session_id = @lv__nullsmallint,
		blocking_exec_context_id = @lv__nullsmallint,
		resource_description = NULL,
		resource_dbid = @lv__nullsmallint,
		resource_associatedobjid = @lv__nullsmallint
	FROM #tasks_and_waits targ
	WHERE targ.RowIsFragmented = 1
	AND 1 = (SELECT NumRows = COUNT(*) FROM #tasks_and_waits taw2
				WHERE taw2.UTCCaptureTime = targ.UTCCaptureTime
				AND taw2.task_address = targ.task_address);


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
	GROUP BY taw.task_address;


	/*********************************************************************************************************************************************************************************
	 ********************************************************************************************************************************************************************************

																*END*  Assemble task and waits info and handle fragmented rows  *END*

	 ********************************************************************************************************************************************************************************
	 ********************************************************************************************************************************************************************************/







	 /*********************************************************************************************************************************************************************************
	 ********************************************************************************************************************************************************************************

																*BEGIN*  Run rules to identify the producer node each task belongs to  *BEGIN*

	 ********************************************************************************************************************************************************************************
	 ********************************************************************************************************************************************************************************/


	--Now, can we associate rows with Producer Node IDs?
	--ECID 0 is always the "Thread 0". Apply that rule confidently
	--Also, NewRow waits ALWAYS indicate which exchange Node ID a task is supplying with rows, so those are safe too
	;WITH CTE AS (
		SELECT 
			taw.ProducerNodeID_ImmediatelyIdentified,
			CalcedProducer = CASE WHEN taw.exec_context_id = 0 THEN -1
								WHEN taw.wait_special_tag = 'NewRow' THEN taw.wait_special_number
								ELSE NULL
								END
		FROM #tasks_and_waits taw
		WHERE taw.RowIsFragmented = 0
	)
	UPDATE CTE 
	SET ProducerNodeID_ImmediatelyIdentified = CalcedProducer
	WHERE CalcedProducer IS NOT NULL;

	--Now, try to identify the producer node for rows whose task_address has been the blocker
	--for GetRow waits (the only consumer-side CXP sub-wait where we are 100% confident that the
	-- blocker is ALWAYS on the other side of the exchange)
	UPDATE targ 
	SET ProducerNodeID_FromGetRowWaits = ss2.PossibleProducerNodeID
	FROM #tasks_and_waits targ
		INNER JOIN (
			SELECT 
				task_address,
				PossibleProducerNodeID,
				rn = ROW_NUMBER() OVER (PARTITION BY task_address ORDER BY NumRows DESC)
			FROM (
				SELECT 
					blocker.task_address,
					[PossibleProducerNodeID] = waiter.wait_special_number,
					NumRows = COUNT(*)
				FROM #tasks_and_waits blocker
					 INNER JOIN #tasks_and_waits waiter
						ON blocker.task_address = waiter.blocking_task_address
						AND waiter.blocking_session_id = @spid
						AND waiter.wait_special_tag = 'GetRow'
				WHERE blocker.RowIsFragmented = 0
				GROUP BY blocker.task_address,
					waiter.wait_special_number
			) ss
		) ss2
			ON targ.task_address = ss2.task_address
			AND ss2.rn = 1
	WHERE targ.RowIsFragmented = 0
	;

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
					rn = ROW_NUMBER() OVER (PARTITION BY task_address, CalculatedProducerNodeID ORDER BY COUNT(*) DESC)
				FROM (
					SELECT 
						taw.task_address,
						CalculatedProducerNodeID = COALESCE(taw.ProducerNodeID_ImmediatelyIdentified, ProducerNodeID_FromGetRowWaits, ProducerNodeID_ThirdLevelIdentification)
					FROM #tasks_and_waits taw
					WHERE taw.RowIsFragmented = 0
				) ss
				WHERE CalculatedProducerNodeID IS NOT NULL
				GROUP BY task_address,
					CalculatedProducerNodeID
			) ss2
			WHERE rn = 1
		) ss3
			ON targ.task_address = ss3.task_address
	;

	SELECT * 
	FROM #StmtStats

	SELECT * 
	FROM #TaskStats;

	SELECT
		taw.task_address,
		taw.ProducerNodeID_ImmediatelyIdentified,
		taw.ProducerNodeID_FromGetRowWaits,
		taw.ProducerNodeID_ThirdLevelIdentification,
		NumRows = COUNT(*)
	FROM #tasks_and_waits taw
	WHERE taw.RowIsFragmented = 0
	GROUP BY taw.task_address,
		taw.ProducerNodeID_ImmediatelyIdentified,
		taw.ProducerNodeID_FromGetRowWaits,
		taw.ProducerNodeID_ThirdLevelIdentification;

	/*********************************************************************************************************************************************************************************
	 ********************************************************************************************************************************************************************************

																*END*  Run rules to identify the producer node each task belongs to  *END*

	 ********************************************************************************************************************************************************************************
	 ********************************************************************************************************************************************************************************/





	

	RETURN -1;
END 
GO
