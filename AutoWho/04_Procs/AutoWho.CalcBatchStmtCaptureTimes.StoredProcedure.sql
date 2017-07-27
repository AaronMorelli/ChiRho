SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[CalcBatchStmtCaptureTimes] 
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

	FILE NAME: AutoWho.CalcBatchStmtCaptureTimes.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.CalcBatchStmtCaptureTimes

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Updates aggregation tables for tracking stmt and batch statistics from the AutoWho sar table

To Execute
------------------------
EXEC AutoWho.CalcBatchStmtCaptureTimes @FirstCaptureTime='2017-07-24 04:00', @LastCaptureTime='2017-07-24 06:00'
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


	--This is a list of rows from SAR between @FirstCaptureTime and @LastCaptureTime
	CREATE TABLE #WorkingSet (
		[session_id]			[smallint] NOT NULL,
		[request_id]			[smallint] NOT NULL,
		[TimeIdentifier]		[datetime] NOT NULL,
		[SPIDCaptureTime]		[datetime] NOT NULL,

		[StatementFirstCapture] [datetime] NULL,	--The first SPIDCaptureTime for the statement that this row belongs to. This acts as a grouping field (that is also ascending as statements 
													--run for the batch! a nice property)
		[PreviousCaptureTime]	[datetime] NULL,	--The cap time immediately previous to this row (for the same batch of course)
		[StatementSequenceNumber] [int] NOT NULL,	 --statement # within the batch. We use this instead of PKSQLStmtStoreID b/c that could be revisited

		[PKSQLStmtStoreID]		[bigint] NOT NULL,	--TODO: still need to implement TMR wait logic. Note that for TMR waits, the current plan is to *always* assume it is a new statement even if 
													--the calc__tmr_wait value matches between the most recent SPIDCaptureTime in this table and the "current" statement.

		[rqst__query_hash]		[binary](8) NULL,	--storing this makes some presentation procs more quickly able to find high-frequency queries.

		--These fields start at 0 and are only set to 1 when we KNOW that a row is the first and/or last of a statement or batch.
		--Thus, once set to 1 they should never change.
		[IsStmtFirstCapture]	[bit] NOT NULL,		
		[IsStmtLastCapture]		[bit] NOT NULL,
		[IsBatchFirstCapture]	[bit] NOT NULL,		
		[IsBatchLastCapture]	[bit] NOT NULL,	

		[IsCurrentLastRowOfBatch]	[bit] NOT NULL,
		[IsFromPermTable]		[bit] NOT NULL,
		[IsInLast3Captures]		[bit] NOT NULL,

		[ProcessingState]		[tinyint] NOT NULL	/*
														0 = completely unprocessed; 
														1 = Self-contained Single-stmt batch, completed
														2 = Normal processing Phase 1 complete: found rows where stmt <> prev.stmt, and applied Batch first/last info.
														3 = Normal processing Phase 2 complete: generate a range of stmt first/last captures and apply to #WorkingSet
													*/
	);
	CREATE UNIQUE CLUSTERED INDEX CL1 ON #WorkingSet (session_id, request_id, TimeIdentifier, SPIDCaptureTime);

	CREATE TABLE #WorkingSetBatches (
		[session_id]			[smallint] NOT NULL,
		[request_id]			[smallint] NOT NULL,
		[TimeIdentifier]		[datetime] NOT NULL,
		[NumCaptures]			[int] NOT NULL,
		[FirstCapture]			[datetime] NOT NULL,
		[LastCapture]			[datetime] NOT NULL,
		[IsInLast3Captures]		[bit] NOT NULL,
		[IsInPermTable]			[bit] NOT NULL
	);
	CREATE UNIQUE CLUSTERED INDEX CL1 ON #WorkingSetBatches (session_id, request_id, TimeIdentifier);

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


	--TODO: need to add logic to handle when PKSQLStmtStoreID is NULL, and for the TMR wait value.
	INSERT INTO #WorkingSet (
		session_id,
		request_id,
		TimeIdentifier,
		SPIDCaptureTime,

		StatementFirstCapture,
		PreviousCaptureTime,
		StatementSequenceNumber,
		PKSQLStmtStoreID,
		rqst__query_hash,
		IsStmtFirstCapture,
		IsStmtLastCapture,
		IsBatchFirstCapture,
		IsBatchLastCapture,
		IsCurrentLastRowOfBatch,
		IsFromPermTable,
		IsInLast3Captures,
		ProcessingState
	)
	SELECT 
		sar.session_id,
		sar.request_id,
		sar.TimeIdentifier,
		sar.SPIDCaptureTime,
		
		[StatementFirstCapture] = NULL,
		[PreviousCaptureTime] = NULL,
		[StatementSequenceNumber] = 0,
		sar.FKSQLStmtStoreID,
		sar.rqst__query_hash,
		[IsStmtFirstCapture] = 0,
		[IsStmtLastCapture] = 0,
		[IsBatchFirstCapture] = 0,
		[IsBatchLastCapture] = 0,
		[IsCurrentLastRowOfBatch] = 0,
		[IsFromPermTable] = 0,
		[IsInLast3Captures] = CASE WHEN sar.SPIDCaptureTime IN (@LastCaptureTime, @LastCaptureTimeMinus1, @LastCaptureTimeMinus2) 
								THEN 1 ELSE 0 END,
		[ProcessingState] = 0
	FROM AutoWho.SessionsAndRequests sar
	WHERE sar.CollectionInitiatorID = 255
	AND sar.request_id <> @lv__nullsmallint
	AND sar.sess__is_user_process = 1
	AND sar.calc__threshold_ignore = 0
	AND sar.SPIDCaptureTime BETWEEN @FirstCaptureTime AND @LastCaptureTime;		
		--Note: we don't expect @FirstCaptureTime to ever be AFTER @LastCaptureMinus1 or 2.
		--If that assumption could be false, we need to rethink our logic to see if there are bugs.

	--Grab some basic batch stats
	INSERT INTO #WorkingSetBatches (
		session_id,
		request_id,
		TimeIdentifier,
		NumCaptures,
		FirstCapture,
		LastCapture,
		IsInLast3Captures,
		IsInPermTable
	)
	SELECT 
		ss.session_id,
		ss.request_id,
		ss.TimeIdentifier,
		ss.NumCaptures,
		ss.FirstCapture,
		ss.LastCapture,
		[IsInLast3Captures] = ss.IsInLast3Captures,
		[IsInPermTable] = CASE WHEN p.session_id IS NOT NULL THEN 1 ELSE 0 END
	FROM (
		SELECT 
			ws.session_id,
			ws.request_id,
			ws.TimeIdentifier,
			[IsInLast3Captures] = CONVERT(BIT,MAX(CONVERT(INT,ws.IsInLast3Captures))),
			NumCaptures = COUNT(*),
			FirstCapture = MIN(ws.SPIDCaptureTime),
			LastCapture = MAX(ws.SPIDCaptureTime)
		FROM #WorkingSet ws
		GROUP BY ws.session_id,
			ws.request_id,
			ws.TimeIdentifier
	) ss
		OUTER APPLY (
			SELECT TOP 1		--TODO: we only have a TOP 1 here b/c we are consulting a table whose grain is the statement/capture time rather than an overall list of
				p.session_id	--batches. If we ever do add a batch table, we can change this and remove the TOP 1.
			FROM AutoWho.StatementCaptureTimes p
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
	FROM AutoWho.StatementCaptureTimes p
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
		in #WorkingSet. These "self-contained" batches can be either single-statement or multi-statement.
		Additionally, we expect single-statement batches to be fairly common (perhaps even VERY common),
		since most OLTP systems have them. Thus, the below statement represents a special-but-common case
		where we can set all our necessary flags in one statement with simple logic.
	*/
	UPDATE ws 
	SET 
		StatementFirstCapture = ws.SPIDCaptureTime,	--the grouping value is its own capture time, of course.
		--we leave this NULL, obviously: PreviousCaptureTime
		StatementSequenceNumber = 1,
		IsStmtFirstCapture = 1,
		IsStmtLastCapture = 1,
		IsBatchFirstCapture = 1,
		IsBatchLastCapture = 1,
		ProcessingState = 1
	FROM #WorkingSetBatches wsb
		INNER JOIN #WorkingSet ws
			ON wsb.session_id = ws.session_id
			AND wsb.request_id = ws.request_id
			AND wsb.TimeIdentifier = ws.TimeIdentifier
	WHERE wsb.NumCaptures = 1
	AND wsb.IsInPermTable = 0
	AND wsb.IsInLast3Captures = 0;


	--Now, insert the last row from the remaining "active" batches into our working set
	INSERT INTO #WorkingSet (
		session_id,
		request_id,
		TimeIdentifier,
		SPIDCaptureTime,

		StatementFirstCapture,
		PreviousCaptureTime,
		StatementSequenceNumber,
		PKSQLStmtStoreID,
		rqst__query_hash,
		IsStmtFirstCapture,
		IsStmtLastCapture,
		IsBatchFirstCapture,
		IsBatchLastCapture,
		IsCurrentLastRowOfBatch,
		IsFromPermTable,
		IsInLast3Captures,
		ProcessingState
	)
	SELECT 
		p.session_id,
		p.request_id,
		p.TimeIdentifier,
		p.SPIDCaptureTime,

		p.StatementFirstCapture,
		p.PreviousCaptureTime,
		p.StatementSequenceNumber,
		p.PKSQLStmtStoreID,
		p.rqst__query_hash,
		p.IsStmtFirstCapture,
		p.IsStmtLastCapture,
		p.IsBatchFirstCapture,
		p.IsBatchLastCapture,
		p.IsCurrentLastRowOfBatch,
		[IsFromPermTable] = 1,
		[IsInLast3Captures] = last3cap.IsInLast3Captures,
		ProcessingState = 0			--We give rows from the perm table the same status as the working set rows that they are joining.
	FROM AutoWho.StatementCaptureTimes p
		CROSS APPLY (
			SELECT wsb.IsInLast3Captures
			FROM #WorkingSetBatches wsb
			WHERE wsb.session_id = p.session_id
			AND wsb.request_id = p.request_id
			AND wsb.TimeIdentifier = p.TimeIdentifier
		) last3cap
	WHERE p.IsCurrentLastRowOfBatch = 1;


	--Ok, now we look for "statement change" rows, i.e. when a row's PKSQLStmtStoreID is different than the prev cap time's PKSQLStmtStoreID
	--We also apply the info we have in #WSB re: batch first and last capture times to our working set
	UPDATE ws
	SET	
		IsBatchFirstCapture = CASE WHEN ws.IsFromPermTable = 1 THEN ws.IsBatchFirstCapture	--For this field, we leave rows from the perm table alone
									WHEN wsb.IsInPermTable = 0		--If the batch already is in the perm table, we already have the Batch first capture there
										AND wsb.FirstCapture = ws.SPIDCaptureTime THEN 1   --otherwise, we know this is the first-ever row of our batch!
								ELSE 0 END,

		IsBatchLastCapture = CASE WHEN ws.IsFromPermTable = 1 THEN 0	--For perm rows, we DO update this field. If a perm row is in this table, we know that it has later WS rows.
																		--Of course, we shouldn't actually have to change this value b/c IsBatchLastCapture should never go to 1 unless
																		--we don't see it in the last 3 caps (and having 3 SPIDCaptureTimes in a row w/o a batch should mean the SPID/Request/TimeIdentifier
																		-- will never be seen again)
									WHEN wsb.IsInLast3Captures = 0	--If the batch has a row in our last 3 caps, we don't consider closing the batch
									AND wsb.LastCapture = ws.SPIDCaptureTime THEN 1 
								ELSE 0 END,

		--This field is mutually exclusive w/IsBatchLastCapture. They both can't be 1. So we only set this to 1 
		-- if this row is the last working set capture for the batch and the batch DOES exist in the last 3 caps
		IsCurrentLastRowOfBatch = CASE WHEN ws.IsFromPermTable = 1 THEN 0	-- a perm row in #WS must have later WS rows, so we know we can set it to 0.
										WHEN wsb.IsInLast3Captures = 1
										AND wsb.LastCapture = ws.SPIDCaptureTime THEN 1 
									ELSE 0 END,

		--TODO: need to add logic for when PKSQLStmtStoreID is null and there is a TMR wait.
		IsStmtFirstCapture = CASE WHEN ws.IsFromPermTable = 1 THEN ws.IsStmtFirstCapture		--retain whatever we had from the perm table
									WHEN prevCap.session_id IS NULL THEN 1						--if we hit this case, it means the batch had no recs in the perm table,
																								--and therefore this batch has no prev captures, so it is automatically the statement start
																								--It also in the batch start (which should be handled by the block a few lines above).
									WHEN ws.PKSQLStmtStoreID <> ISNULL(prevCap.PKSQLStmtStoreID,-99) THEN 1
									ELSE 0
									END,
		PreviousCaptureTime = CASE WHEN ws.IsFromPermTable = 1 THEN ws.PreviousCaptureTime
										ELSE prevCap.SPIDCaptureTime
								END,
		ProcessingState = 2
	FROM #WorkingSetBatches wsb
		INNER JOIN #WorkingSet ws
			ON wsb.session_id = ws.session_id
			AND wsb.request_id = ws.request_id
			AND wsb.TimeIdentifier = ws.TimeIdentifier
		OUTER APPLY (
			--Find the prev cap time, so we can compare SQL Stmt IDs
			SELECT TOP 1 
				prev.session_id,
				prev.SPIDCaptureTime,
				prev.IsFromPermTable,
				prev.PKSQLStmtStoreID
			FROM #WorkingSet prev
			WHERE prev.session_id = ws.session_id
			AND prev.request_id = ws.request_id
			AND prev.TimeIdentifier = ws.TimeIdentifier
			AND prev.SPIDCaptureTime < ws.SPIDCaptureTime
			ORDER BY prev.SPIDCaptureTime DESC
		) prevCap
	WHERE ws.ProcessingState = 0;		--This includes IsFromPermTable=1 rows.


	/*
			2. For each IsStmtFirstCapture = 1 (aka "this stmt start"), find the next IsStmtFirstCapture = 1 (aka "next stmt start"),
			then find the last capture before "next start", which should be the last capture/statement end ("last cap") for this statement.
	*/
	UPDATE ws
	SET StatementFirstCapture = ss.StatementFirstCapture,	--StatementFirstCapture acts as a grouping field. The logic in this statement
															--ensures that it is the same for every row between IsStmtFirstCapture=1 and IsStmtLastCapture=1
															--Having a grouping key that is also ascending lets us easily set the StatementSequenceNumber next.

		--If the last cap time for a statement is NOT the last row of a batch, then we can definitively say that it is the true last cap time for that statement.
		--Any further rows that come in will not add more capture times for this statement.
		--If the last cap time for a statement is also the last cap time for a batch with NO presence in our closing set, then similarly we know that no
		--new rows are going to come in for this statement.
		--But, if the last cap time for this statement is also the last cap time for a batch WITH a presence in our closing set, then we don't know whether
		--more rows will arrive, and whether they will be for this statement.
		IsStmtLastCapture = CASE WHEN ws.IsCurrentLastRowOfBatch = 1 THEN 0
								WHEN ws.IsBatchLastCapture = 1 THEN 1
								WHEN ws.SPIDCaptureTime = ss.StatementLastCapture THEN 1 
								ELSE 0
								END,
		ProcessingState = 3
	FROM #WorkingSet ws
		INNER JOIN (
			/*
				This sub-query gives us a list of capture ranges, for each batch, that define the start/stop of each statement within the batch
				Note the WHERE: ws.IsStmtFirstCapture = 1 OR ws.IsFromPermTable = 1
				The perm row isn't necessarily the start of a stmt, but it carries the info of the start of the statement (the SPIDCaptureTime)
				and thus effectively supplies the range. 
			*/
			SELECT DISTINCT
				ws.session_id,
				ws.request_id,
				ws.TimeIdentifier,

				[StatementFirstCapture] = CASE WHEN ws.IsFromPermTable = 1 THEN ws.StatementFirstCapture	--for perm rows, always keep the first cap that we calculated previously
											ELSE --based on the OR in the below WHERE clause, this must be ws.IsStmtFirstCapture = 1
												--therefore, this row's stmt first cap is its own SPIDCaptureTime
												ws.SPIDCaptureTime 
											END,

				[StatementLastCapture] = CASE WHEN lastCap.session_id IS NULL --not able to find a "last cap time", so there are no later
																				--cap times for this statement. Thus, "last cap" is this SPIDCaptureTime
												THEN ws.SPIDCaptureTime 
											ELSE lastCap.SPIDCaptureTime
											END
			FROM #WorkingSet ws
				OUTER APPLY (
					--Get the next statement start. Remember that it may not exist!
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
					--If there is no "next", we trust that our last cap will have occurred before the year 3000.
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
			WHERE ws.ProcessingState = 2
			AND (ws.IsStmtFirstCapture = 1
				OR ws.IsFromPermTable = 1		--If the first (non-perm) row in our working set is NOT a new statement, i.e. the same stmt ID as the preceding perm row
												--we want to associate the first row of our working set with the same stmt as its preceding perm row, whether or not
												--the perm row was the start of a new statement. So we need to include perm rows here.
				)
		) ss
			ON ws.session_id = ss.session_id
			AND ws.request_id = ss.request_id
			AND ws.TimeIdentifier = ss.TimeIdentifier
			AND ws.SPIDCaptureTime BETWEEN ss.StatementFirstCapture AND ss.StatementLastCapture;

	--Now, update the IsCurrentLastRowOfBatch values for the perm rows that we pulled into #WS
	UPDATE targ 
	SET IsCurrentLastRowOfBatch = 0
	FROM AutoWho.StatementCaptureTimes targ
		INNER JOIN #WorkingSet ws
			ON ws.session_id = targ.session_id
			AND ws.request_id = targ.request_id
			AND ws.TimeIdentifier = targ.TimeIdentifier
			AND ws.SPIDCaptureTime = targ.SPIDCaptureTime
	WHERE targ.IsCurrentLastRowOfBatch = 1
	AND ws.IsFromPermTable = 1;

	--Ok, we're ready to insert the working set into our perm table!
	INSERT INTO [AutoWho].[StatementCaptureTimes] (
		--Identifier columns
		[session_id],
		[request_id],
		[TimeIdentifier],
		[SPIDCaptureTime],

		--attribute cols
		[StatementFirstCapture],
		[PreviousCaptureTime],
		[StatementSequenceNumber],
		[PKSQLStmtStoreID],
		[rqst__query_hash],
		[IsStmtFirstCapture],
		[IsStmtLastCapture],
		[IsBatchFirstCapture],
		[IsBatchLastCapture],
		[IsCurrentLastRowOfBatch]
	)
	SELECT 
		ws.session_id,
		ws.request_id,
		ws.TimeIdentifier,
		ws.SPIDCaptureTime,

		ws.StatementFirstCapture,
		ws.PreviousCaptureTime,
		ws.StatementSequenceNumber,
		ws.PKSQLStmtStoreID,
		ws.rqst__query_hash,
		ws.IsStmtFirstCapture,
		ws.IsStmtLastCapture,
		ws.IsBatchFirstCapture,
		ws.IsBatchLastCapture,
		ws.IsCurrentLastRowOfBatch
	FROM #WorkingSet ws
	WHERE ws.IsFromPermTable = 0;





	/* For now, I'm not going to implement the StatementSequenceNumber logic. I currently don't see a need for that data.
		I may revisit this after writing presentation logic that relies on AutoWho.StatementCaptureTimes
	
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
	*/

	RETURN 0;
END
GO
