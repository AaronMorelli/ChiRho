SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [AutoWho].[CalcBatchStmtStats] 
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

	FILE NAME: AutoWho.CalcBatchStmtStats.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.CalcBatchStmtStats

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Updates aggregation tables for tracking stmt and batch statistics from the AutoWho sar table

To Execute
------------------------
EXEC AutoWho.CalcBatchStmtStats @FirstCaptureTime='2017-07-24 04:00', @LastCaptureTime='2017-07-24 06:00'
*/
(
	@FirstCaptureTime		DATETIME,
	@LastCaptureTime		DATETIME
)
AS
BEGIN
	SET NOCOUNT ON;

	/* Scope is limited to 

		- Requests (no idle spids)
			(We may later decide to aggregate data for idle spids but there isn't as much need since it is so straightforward)

		- User SPIDs ([sess__is_user_process] = 1)

		- [calc__threshold_ignore] = 0

		- only SAR records from the background trace (initiator = 255)

	*/

	DECLARE @LastCaptureTimeMinus1	DATETIME,
			@LastCaptureTimeMinus2	DATETIME,
			@lv__nullsmallint		SMALLINT;

	SET @lv__nullsmallint = -929;

	--Construct a list of SPIDCaptureTimes that form our "unprocessed" set
	--TODO: currently I'm not really using this table. Do I need it?
	CREATE TABLE #UnprocessedCaptureTimes (
		SPIDCaptureTime DATETIME NOT NULL
	);

	CREATE TABLE #ClosingTimeFrame (
		[SPIDCaptureTime]		[datetime] NOT NULL,
		[session_id]			[smallint] NOT NULL,
		[request_id]			[smallint] NOT NULL,
		[TimeIdentifier]		[datetime] NOT NULL
	);

	CREATE TABLE #WorkingSet (
		[SPIDCaptureTime]		[datetime] NOT NULL,
		[session_id]			[smallint] NOT NULL,
		[request_id]			[smallint] NOT NULL,
		[TimeIdentifier]		[datetime] NOT NULL,

		[PKSQLStmtStoreID]		[bigint] NOT NULL,		--we set this to -1 if it is NULL in SAR. (This is typically TMR waits, I think)
													--Note that for TMR waits, for now we *always* assume it is a new statement even if
													--the calc__tmr_wait value matches between the most recent SPIDCaptureTime in this table
													--and the "current" statement.
		[rqst__query_hash]		[binary](8) NULL,

		--To properly set these fields, we need to examine both #ClosingTimeFrame and AutoWho.StmtCaptureTimes
		[IsStmtFirstCapture]	[bit] NOT NULL,		
		[IsStmtLastCapture]		[bit] NOT NULL,
		[IsBatchFirstCapture]	[bit] NOT NULL,		
		[IsBatchLastCapture]	[bit] NOT NULL,	

		[IsCurrentLastRowOfBatch]	[bit] NOT NULL,
		[IsFromPermTable]		[bit] NOT NULL,

		[StatementFirstCapture] [datetime] NULL,	--grouping column for grouping rows together into the same statement.
		[StatementSequenceNumber] [int] NOT NULL,		--Unlike the perm table, this is just the sequence of statements within the #WorkingSet data

		[ProcessingState]		[tinyint] NOT NULL	/*
														0 = completely unprocessed; 
														1 = Self-contained Single-stmt batch, completed
														2 = Self-contained multi-stmt batch, after first UPDATE
														3 = Self-contained multi-stmt batch, after second UPDATE
														4 = Self-contained multi-stmt batch, after final UPDATE (to set StatementSequenceNumber)

													*/
	);

	CREATE TABLE #WorkingSetBatches (
		[session_id]			[smallint] NOT NULL,
		[request_id]			[smallint] NOT NULL,
		[TimeIdentifier]		[datetime] NOT NULL,
		[NumCaptures]			[int] NOT NULL,
		[FirstCapture]			[datetime] NOT NULL,
		[LastCapture]			[datetime] NOT NULL,
		[IsInClosingSet]		[bit] NOT NULL,
		[IsInPermTable]			[bit] NOT NULL
	);

	INSERT INTO #UnprocessedCaptureTimes (
		SPIDCaptureTime
	)
	SELECT ct.SPIDCaptureTime
	FROM AutoWho.CaptureTimes ct
	WHERE ct.CollectionInitiatorID = 255		--background trace only
	AND ct.SPIDCaptureTime BETWEEN @FirstCaptureTime AND @LastCaptureTime;

	/* We don't need this logic because the caller [AutoWho.PostProcessor] handles this validation
	IF NOT EXISTS (SELECT * FROM #UnprocessedCaptureTimes)
	BEGIN
		RETURN 0;
	END
	*/

	/*
		We close a batch or statement when our permanent table shows it as open and it is not found in the SAR
		data for @LastCaptureTime, @LastCaptureTimeMinus1 (the SPIDCaptureTime immediately before @LastCaptureTime)
		or @LastCaptureTimeMinus2. So let's get those variable values.
	*/
	SELECT 
		@LastCaptureTimeMinus1 = ss.SPIDCaptureTime
	FROM (
		SELECT TOP 1
			ct.SPIDCaptureTime
		FROM AutoWho.CaptureTimes ct
		WHERE ct.CollectionInitiatorID = 255		--background trace only
		AND ct.SPIDCaptureTime < @LastCaptureTime
		ORDER BY ct.SPIDCaptureTime DESC
	) ss;

	SELECT 
		@LastCaptureTimeMinus2 = ss.SPIDCaptureTime
	FROM (
		SELECT TOP 1
			ct.SPIDCaptureTime
		FROM AutoWho.CaptureTimes ct
		WHERE ct.CollectionInitiatorID = 255		--background trace only
		AND ct.SPIDCaptureTime < @LastCaptureTimeMinus1
		ORDER BY ct.SPIDCaptureTime DESC
	) ss;

	INSERT INTO #ClosingTimeFrame (
		[SPIDCaptureTime],
		[session_id],
		[request_id],
		[TimeIdentifier]
	)
	SELECT 
		sar.SPIDCaptureTime,
		sar.session_id,
		sar.request_id,
		sar.TimeIdentifier
	FROM AutoWho.SessionsAndRequests sar
	WHERE sar.CollectionInitiatorID = 255
	AND sar.request_id <> @lv__nullsmallint
	AND sar.sess__is_user_process = 1
	AND sar.calc__threshold_ignore = 0
	AND sar.SPIDCaptureTime IN (
		@LastCaptureTime,
		@LastCaptureTimeMinus1,
		@LastCaptureTimeMinus2
	);

	--TODO: need to add logic to handle when PKSQLStmtStoreID is NULL, and for the TMR wait value.
	INSERT INTO #WorkingSet (
		SPIDCaptureTime,
		session_id,
		request_id,
		TimeIdentifier,
		PKSQLStmtStoreID,
		rqst__query_hash,
		
		IsCurrentLastRowOfBatch,
		IsFromPermTable,

		IsStmtFirstCapture,
		IsStmtLastCapture,
		IsBatchFirstCapture,
		IsBatchLastCapture,
		StatementSequenceNumber,
		ProcessingState
	)
	SELECT 
		sar.SPIDCaptureTime,
		sar.session_id,
		sar.request_id,
		sar.TimeIdentifier,
		sar.FKSQLStmtStoreID,
		sar.rqst__query_hash,

		[IsCurrentLastRowOfBatch] = 0,
		[IsFromPermTable] = 0,
		
		[IsStmtFirstCapture] = 0,
		[IsStmtLastCapture] = 0,
		[IsBatchFirstCapture] = 0,
		[IsBatchLastCapture] = 0,
		[StatementSequenceNumber] = 0,
		[ProcessingState] = 0
	FROM AutoWho.SessionsAndRequests sar
	WHERE sar.CollectionInitiatorID = 255
	AND sar.request_id <> @lv__nullsmallint
	AND sar.sess__is_user_process = 1
	AND sar.calc__threshold_ignore = 0
	AND sar.SPIDCaptureTime BETWEEN @FirstCaptureTime AND @LastCaptureTime;		--Note: we don't expect @FirstCaptureTime to ever be AFTER @LastCaptureMinus1 or 2.

	--Grab some basic batch stats
	INSERT INTO #WorkingSetBatches (
		[session_id],
		[request_id],
		[TimeIdentifier],
		[NumCaptures],
		[FirstCapture],
		[LastCapture],
		[IsInClosingSet],
		[IsInPermTable]
	)
	SELECT 
		ss.session_id,
		ss.request_id,
		ss.TimeIdentifier,
		ss.NumCaptures,
		ss.FirstCapture,
		ss.LastCapture,
		[IsInClosingSet] = CASE WHEN c.session_id IS NOT NULL THEN 1 ELSE 0 END,
		[IsInPermTable] = CASE WHEN p.session_id IS NOT NULL THEN 1 ELSE 0 END
	FROM (
		SELECT 
			ws.session_id,
			ws.request_id,
			ws.TimeIdentifier,
			NumCaptures = COUNT(*),
			FirstCapture = MIN(ws.SPIDCaptureTime),
			LastCapture = MAX(ws.SPIDCaptureTime)
		FROM #WorkingSet ws
		GROUP BY ws.session_id,
			ws.request_id,
			ws.TimeIdentifier
	) ss
		OUTER APPLY (
			SELECT TOP 1		--TODO: we only need the top 1 b/c the grain of #ClosingTimeFrame doesn't have a DISTINCT b/c I haven't decided yet whether to add more attributes.
				c.session_id		--If we add a DISTINCT to #ctf, then we can remove the top 1 here.
			FROM #ClosingTimeFrame c
			WHERE c.session_id = ss.session_id
			AND c.request_id = ss.request_id
			AND c.TimeIdentifier = ss.TimeIdentifier
		) c
		OUTER APPLY (
			SELECT TOP 1		--TODO: we only have a TOP 1 here b/c we are consulting a table whose grain is the statement/capture time rather than an overall list of
				p.session_id	--batches. If we ever do add a batch table, we can change this and remove the TOP 1.
			FROM AutoWho.StmtCaptureTimes p
			WHERE p.session_id = ss.session_id
			AND p.request_id = ss.request_id
			AND p.TimeIdentifier = ss.TimeIdentifier
		) p
	;

	--TODO: put a profiler SELECT here to store info about the types of batches we have into local variables,
	-- that I can then use in IF blocks below to control which statements are actually executed 
	-- (i.e. don't execute a statement unless there are actually batches that fit that bill).


	--Now, close batches in the perm table that aren't present at all in our working set. 
	UPDATE p
	SET IsCurrentLastRowOfBatch = 0,
		IsStmtLastCapture = 1,
		IsBatchLastCapture = 1
	FROM AutoWho.StmtCaptureTimes p
	WHERE p.IsCurrentLastRowOfBatch = 1
	AND NOT EXISTS (
		SELECT *
		FROM #WorkingSetBatches wsb
		WHERE wsb.session_id = p.session_id
		AND wsb.request_id = p.request_id
		AND wsb.TimeIdentifier = p.TimeIdentifier
	);

	/* For a typical OLTP system, there should be a number of batches in our working set that are short-lived enough
		that they don't exist in either the perm table or in the closing set. Thus, they are completely self-contained
		in #WorkingSet. Additionally, we expect single-statement batches to be fairly common (perhaps even VERY common),
		since most OLTP systems have them.
		
		We call these "self-contained" batches and they can be either single-statement or multi-statement.
		This statement handles single-statement self-contained batches.
	*/
	UPDATE ws 
	SET 
		IsStmtFirstCapture = 1,
		IsStmtLastCapture = 1,
		IsBatchFirstCapture = 1,
		IsBatchLastCapture = 1,
		StatementSequenceNumber = 1,
		ProcessingState = 1
	FROM #WorkingSetBatches wsb
		INNER JOIN #WorkingSet ws
			ON wsb.session_id = ws.session_id
			AND wsb.request_id = ws.request_id
			AND wsb.TimeIdentifier = ws.TimeIdentifier
	WHERE wsb.NumCaptures = 1
	AND wsb.IsInPermTable = 0
	AND wsb.IsInClosingSet = 0;


	--Now, insert the last row from the remaining "active" batches into our working set
	INSERT INTO #WorkingSet (
		SPIDCaptureTime,
		session_id,
		request_id,
		TimeIdentifier,
		PKSQLStmtStoreID,
		rqst__query_hash,
		
		IsCurrentLastRowOfBatch,
		IsFromPermTable,

		IsStmtFirstCapture,
		IsStmtLastCapture,
		IsBatchFirstCapture,
		IsBatchLastCapture,
		StatementSequenceNumber,
		ProcessingState
	)
	SELECT 
		p.SPIDCaptureTime,
		p.session_id,
		p.request_id,
		p.TimeIdentifier,
		p.PKSQLStmtStoreID,
		p.rqst__query_hash,
		
		IsCurrentLastRowOfBatch = 0,	--Note, b/c we closed out batches that were in the perm table but not in our working set (above)
										--we know that the row we just pulled that was previously the last-of-batch now has records in
										--the working set and thus cannot be the last-of-batch.
		--p.IsCurrentLastRowOfBatch,
		[IsFromPermTable] = 1,
		p.IsStmtFirstCapture,
		p.IsStmtLastCapture,
		p.IsBatchFirstCapture,
		p.IsBatchLastCapture,
		p.StatementSequenceNumber,
		ProcessingState = 0			--TODO: Figure out what state # I want here.
	FROM AutoWho.StmtCaptureTimes p
	WHERE p.IsCurrentLastRowOfBatch = 1;


	--Ok, now we look for "statement change" rows, i.e. when a row's PKSQLStmtStoreID is different than the prev cap time's PKSQLStmtStoreID
	--We also apply the info we have in #WSB re: batch first and last capture times to our working set
	UPDATE ws
	SET	
		IsCurrentLastRowOfBatch = CASE WHEN ws.IsFromPermTable = 1 THEN 0
										WHEN wsb.LastCapture = ws.SPIDCaptureTime THEN 1 ELSE 0 END,

		IsBatchFirstCapture = CASE WHEN ws.IsFromPermTable = 1 THEN 0
									WHEN wsb.IsInPermTable = 0		--If the batch already is in the perm table, we already have the Batch first capture there
									AND wsb.FirstCapture = ws.SPIDCaptureTime THEN 1 ELSE 0 END,

		IsBatchLastCapture = CASE WHEN ws.IsFromPermTable = 1 THEN 0
									WHEN wsb.IsInClosingSet = 0	--If the batch DOES have a row in our closing set, we don't consider closing the batch
									AND wsb.LastCapture = ws.SPIDCaptureTime THEN 1 ELSE 0 END,

		--TODO: need to add logic for when PKSQLStmtStoreID is null and there is a TMR wait.
		IsStmtFirstCapture = CASE WHEN ws.IsFromPermTable = 1 THEN ws.IsStmtFirstCapture		--retain whatever we had from the perm table
									WHEN prevCap.session_id IS NULL THEN 1						--if we hit this case, it means the batch had no recs in the perm table,
																								--and this row has no prev captures, so it is automatically the statement start
																								--It also in the batch start (which should be handled by the block a few lines above).
									WHEN ws.PKSQLStmtStoreID <> ISNULL(prevCap.PKSQLStmtStoreID,-99) THEN 1
									ELSE 0
									END,
		ProcessingState = 2		--TODO: revisit statuses
	FROM #WorkingSetBatches wsb
		INNER JOIN #WorkingSet ws
			ON wsb.session_id = ws.session_id
			AND wsb.request_id = ws.request_id
			AND wsb.TimeIdentifier = ws.TimeIdentifier
		OUTER APPLY (
			--Find the prev cap time, so we can compare SQL Stmt IDs
			SELECT TOP 1 
				prev.session_id,
				prev.PKSQLStmtStoreID
			FROM #WorkingSet prev
			WHERE prev.session_id = ws.session_id
			AND prev.request_id = ws.request_id
			AND prev.TimeIdentifier = ws.TimeIdentifier
			AND prev.SPIDCaptureTime < ws.SPIDCaptureTime
			ORDER BY prev.SPIDCaptureTime DESC
		) prevCap
	WHERE ws.ProcessingState = 0		--currently this should include IsFromPermTable=1 rows.
	--OR ws.IsFromPermTable = 1
	AND ws.IsFromPermTable <> 1
	;

	--I originally coded the above statement for "self-contained multi-stmt batches". I'm trying to make this logic more general.
	--AND wsb.IsInPermTable = 0
	--AND wsb.IsInClosingSet = 0
	;

	/*
			2. For each IsStmtFirstCapture = 1 (aka "this stmt start"), find the next IsStmtFirstCapture = 1 (aka "next stmt start"),
			then find the last capture before "next start", which should be the last capture/statement end ("last cap") for this statement.
	*/
	UPDATE ws
	SET StatementFirstCapture = ss.StatementFirstCapture,	--StatementFirstCapture acts as a grouping field. The logic in this statement
															--ensures that it is the same for every row between IsStmtFirstCapture=1 and IsStmtLastCapture=1
															--Having a grouping key that is also ascending lets us easily set the StatementSequenceNumber next.
		IsStmtLastCapture = ss.StatementLastCapture,
		ProcessingState = 3
	FROM #WorkingSet ws
		INNER JOIN (
			SELECT
				ws.session_id,
				ws.request_id,
				ws.TimeIdentifier,
				[StatementFirstCapture] = ws.SPIDCaptureTime,
				[StatementLastCapture] = CASE WHEN lastCap.session_id IS NULL --not able to find a "last cap time", so there are no intervening
																				--cap times for this statement. Thus, "last cap" is the same as first cap
												THEN ws.SPIDCaptureTime
											ELSE lastCap.SPIDCaptureTime
											END
			FROM #WorkingSet ws
				OUTER APPLY (
					--Get the next statement start
					SELECT TOP 1
						nxt.session_id,
						nxt.PKSQLStmtStoreID,
						nxt.SPIDCaptureTime
					FROM #WorkingSet nxt
					WHERE nxt.session_id = ws.session_id
					AND nxt.request_id = ws.request_id
					AND nxt.TimeIdentifier = ws.TimeIdentifier
					AND nxt.SPIDCaptureTime > ws.SPIDCaptureTime
					AND nxt.IsStmtFirstCapture = 1
					ORDER BY nxt.SPIDCaptureTime ASC
				) nextStmt
				OUTER APPLY (
					--once we have the next statement start, get the cap time immediately before that.
					--this should be the last cap for this statement.
					SELECT TOP 1
						l.session_id,
						l.SPIDCaptureTime
					FROM #WorkingSet l
					WHERE l.session_id = ws.session_id
					AND l.request_id = ws.request_id
					AND l.TimeIdentifier = ws.TimeIdentifier
					AND l.SPIDCaptureTime > ws.SPIDCaptureTime
					AND l.SPIDCaptureTime < ISNULL(nextStmt.SPIDCaptureTime,'3000-01-01')
					ORDER BY l.SPIDCaptureTime DESC
				) lastCap
			WHERE ws.IsStmtFirstCapture = 1
			AND ws.ProcessingState = 2
		) ss
			ON ws.session_id = ss.session_id
			AND ws.request_id = ss.request_id
			AND ws.TimeIdentifier = ss.TimeIdentifier
			AND ws.SPIDCaptureTime BETWEEN ss.StatementFirstCapture AND ss.StatementLastCapture;


	--Ok, now just update StatementSequenceNumber and we're done with this set of batches!
	UPDATE ws
	SET StatementSequenceNumber = ss.StatementSequenceNumber,
		ProcessingState = 4
	FROM #WorkingSet ws
		INNER JOIN (
			SELECT 
				session_id, 
				request_id,
				TimeIdentifier,
				SPIDCaptureTime,
				StatementSequenceNumber = DENSE_RANK() OVER (PARTITION BY session_id, request_id, TimeIdentifier ORDER BY StatementFirstCapture)
			FROM #WorkingSet ws
			WHERE ws.ProcessingState = 3
		) ss
			ON ws.session_id = ss.session_id
			AND ws.request_id = ss.request_id
			AND ws.TimeIdentifier = ss.TimeIdentifier
			AND ws.SPIDCaptureTime = ss.SPIDCaptureTime
	WHERE ws.ProcessingState = 3;


	/*
		Ok, now we're done with self-contained batches (the batch has no records either in the closing set or in the perm table.
		We have these situations left:

			1. Batch is in the perm table and has no records in #WorkingSet

				We need to close these

			2. Batch is not in perm table, only in #WorkingSet

				a. Batch is in closing set

				b. Batch is not in closing set (this is already handled by the above logic)

			3. Batch is in perm table and HAS records in #WorkingSet
			
				a. has records in closing set

				b. does NOT have records in closing set
	*/

	RETURN 0;
END
GO
