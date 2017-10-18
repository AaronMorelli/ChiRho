SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ApplyRetentionPolicies]
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

	FILE NAME: AutoWho.ApplyRetentionPolicies.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.ApplyRetentionPolicies

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Runs on the schedule defined via parameters to CoreXR.ChiRhoMaster, 
		and applies various retention policies defined in AutoWho.Options

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
EXEC AutoWho.ApplyRetentionPolicies
*/
AS
BEGIN

	SET NOCOUNT ON;

	DECLARE @lv__ErrorMessage NVARCHAR(4000),
			@lv__ErrorState INT,
			@lv__ErrorSeverity INT,
			@lv__ErrorLoc NVARCHAR(40);

	BEGIN TRY
		SET @lv__ErrorLoc = N'Variable declare';
		DECLARE 
			--from AutoWho.Options table
			@opt__HighTempDBThreshold						INT,
			@opt__TranDetailsThreshold						INT,
			@opt__MediumDurationThreshold					INT,
			@opt__HighDurationThreshold						INT,
			@opt__BatchDurationThreshold					INT,
			@opt__LongTransactionThreshold					INT,
			@opt__Retention_IdleSPIDs_NoTran				INT,
			@opt__Retention_IdleSPIDs_WithShortTran			INT,
			@opt__Retention_IdleSPIDs_WithLongTran			INT,
			@opt__Retention_IdleSPIDs_HighTempDB			INT,
			@opt__Retention_ActiveLow						INT,
			@opt__Retention_ActiveMedium					INT,
			@opt__Retention_ActiveHigh						INT,
			@opt__Retention_ActiveBatch						INT,
			@opt__Retention_CaptureTimes					INT,
			@opt__PurgeUnextractedData						NCHAR(1),
			@max__RetentionHours							INT,

			@retainAfter__IdleSPIDs_NoTran					DATETIME,
			@retainAfter__IdleSPIDs_WithShortTran			DATETIME,
			@retainAfter__IdleSPIDs_WithLongTran			DATETIME,
			@retainAfter__IdleSPIDs_HighTempDB				DATETIME,
			@retainAfter__ActiveLow							DATETIME,
			@retainAfter__ActiveMedium						DATETIME,
			@retainAfter__ActiveHigh						DATETIME,
			@retainAfter__ActiveBatch						DATETIME,

			--misc general purpose
			@lv__ProcRC										INT,
			@lv__tmpBigInt									BIGINT,
			@lv__tmpStr										NVARCHAR(4000),
			@lv__tmpMinID									BIGINT, 
			@lv__tmpMaxID									BIGINT,
			@lv__nullstring									NVARCHAR(8),
			@lv__nullint									INT,
			@lv__nullsmallint								SMALLINT,

			--derived or intermediate values
			@lv__MaxSPIDCaptureTime							DATETIME,
			@lv__MinPurge_SPIDCaptureTime					DATETIME,
			@lv__MaxPurge_SPIDCaptureTime					DATETIME,
			@lv__TableSize_ReservedPages					BIGINT,
			@lv__HardDeleteCaptureTime						DATETIME,
			@lv__NextDWExtractionCaptureTime				DATETIME
			;

		SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
		SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
		SET @lv__nullsmallint = -929;			-- overlapping with some special system value

		SET @lv__ErrorLoc = N'Temp table creation';
		CREATE TABLE #AutoWhoDistinctStoreKeys (
			[FKSQLStmtStoreID] BIGINT,
			[FKSQLBatchStoreID] BIGINT,
			[FKInputBufferStoreID] BIGINT,
			[FKQueryPlanBatchStoreID] BIGINT,
			[FKQueryPlanStmtStoreID] BIGINT
		);

		CREATE TABLE #StoreTableIDsToPurge (
			ID BIGINT NOT NULL PRIMARY KEY CLUSTERED
		);

		CREATE TABLE #RecordsToPurge (
			CollectionInitiatorID			TINYINT NOT NULL,
			SPIDCaptureTime					DATETIME NOT NULL,
			session_id						SMALLINT NOT NULL,
			request_id						INT NOT NULL,
			TimeIdentifier					DATETIME NOT NULL,
			Retain_IdleSPID_HighTempDB		INT NOT NULL,
			Retain_IdleSPID_WithLongTran	INT NOT NULL,
			Retain_IdleSPID_WithShortTran	INT NOT NULL,
			Retain_IdleSPID_WithNoTran		INT NOT NULL,
			Retain_ActiveLow				INT NOT NULL,
			Retain_ActiveMedium				INT NOT NULL,
			Retain_ActiveHigh				INT NOT NULL,
			Retain_ActiveBatch				INT NOT NULL,
			Retain_SpecialRows				INT NOT NULL
		);

		SET @lv__ErrorLoc = N'Option obtain';
		SELECT 
			@opt__HighTempDBThreshold				= [HighTempDBThreshold],
			@opt__MediumDurationThreshold			= [MediumDurationThreshold],
			@opt__HighDurationThreshold				= [HighDurationThreshold],
			@opt__BatchDurationThreshold			= [BatchDurationThreshold],
			@opt__LongTransactionThreshold			= [LongTransactionThreshold],

			@opt__Retention_IdleSPIDs_NoTran		= [Retention_IdleSPIDs_NoTran],
			@opt__Retention_IdleSPIDs_WithShortTran = [Retention_IdleSPIDs_WithShortTran],
			@opt__Retention_IdleSPIDs_WithLongTran  = [Retention_IdleSPIDs_WithLongTran],
			@opt__Retention_IdleSPIDs_HighTempDB	= [Retention_IdleSPIDs_HighTempDB],
			@opt__Retention_ActiveLow				= [Retention_ActiveLow],
			@opt__Retention_ActiveMedium			= [Retention_ActiveMedium],
			@opt__Retention_ActiveHigh				= [Retention_ActiveHigh],
			@opt__Retention_ActiveBatch				= [Retention_ActiveBatch],
			@opt__Retention_CaptureTimes			= [Retention_CaptureTimes],
			@opt__PurgeUnextractedData				= [PurgeUnextractedData]
		FROM AutoWho.Options;

		--Calculate the datetime boundary for each of these retention policies.
		SET @retainAfter__IdleSPIDs_NoTran = DATEADD(HOUR, 0 - @opt__Retention_IdleSPIDs_NoTran, GETDATE());
		SET @retainAfter__IdleSPIDs_WithShortTran = DATEADD(HOUR, 0 - @opt__Retention_IdleSPIDs_WithShortTran, GETDATE());
		SET @retainAfter__IdleSPIDs_WithLongTran = DATEADD(HOUR, 0 - @opt__Retention_IdleSPIDs_WithLongTran, GETDATE());
		SET @retainAfter__IdleSPIDs_HighTempDB = DATEADD(HOUR, 0 - @opt__Retention_IdleSPIDs_HighTempDB, GETDATE());
		SET @retainAfter__ActiveLow = DATEADD(HOUR, 0 - @opt__Retention_ActiveLow, GETDATE());
		SET @retainAfter__ActiveMedium = DATEADD(HOUR, 0 - @opt__Retention_ActiveMedium, GETDATE());
		SET @retainAfter__ActiveHigh = DATEADD(HOUR, 0 - @opt__Retention_ActiveHigh, GETDATE());
		SET @retainAfter__ActiveBatch = DATEADD(HOUR, 0 - @opt__Retention_ActiveBatch, GETDATE());

		SET @lv__ErrorLoc = N'Next extract & Hard Delete';
		SELECT 
			@lv__NextDWExtractionCaptureTime = MIN(ct.SPIDCaptureTime)
		FROM AutoWho.CaptureTimes ct
		WHERE ct.ExtractedForDW = 0
		AND ct.CollectionInitiatorID = 255;	--DW extraction only occurs for captures by the background collector.

		IF @lv__NextDWExtractionCaptureTime IS NULL
		BEGIN
			SET @lv__NextDWExtractionCaptureTime = GETDATE();
		END

		/* Calculate our "Hard-delete" policy. Anything older than this *WILL* be deleted by this purge run. 
			It is based on @opt__Retention_CaptureTimes, but if the administrator has configured this install
			to prevent the purging of unextracted-to-DW capture times, then the hard delete cannot be more
			recent than our @lv__NextDWExtractionCaptureTime.
		*/
		SELECT 
			@lv__HardDeleteCaptureTime = ss.SPIDCaptureTime
		FROM (
			SELECT TOP 1 ct.SPIDCaptureTime
			FROM AutoWho.CaptureTimes ct
			WHERE ct.CollectionInitiatorID = 255
			AND ct.RunWasSuccessful = 1
			AND ct.SPIDCaptureTime < DATEADD(DAY, 0 - @opt__Retention_CaptureTimes, GETDATE())
			ORDER BY ct.SPIDCaptureTime DESC
		) ss;

		IF @lv__HardDeleteCaptureTime IS NULL
		BEGIN
			SET @lv__HardDeleteCaptureTime = DATEADD(DAY, 0 - @opt__Retention_CaptureTimes, GETDATE());
		END

		IF @opt__PurgeUnextractedData = N'N'
			AND @lv__HardDeleteCaptureTime >= @lv__NextDWExtractionCaptureTime
		BEGIN
			--We raise a warning to the log b/c our hard-delete timeframe was affected by rows that probably
			--should have been extracted by now but haven't yet.
			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 1, N'Hard-delete warning', 
				'Original hard-delete boundary of "' + ISNULL(CONVERT(VARCHAR(20),@lv__HardDeleteCaptureTime),'<null>') + 
					'" has been changed to "' + ISNULL(CONVERT(VARCHAR(20),DATEADD(second, -10, @lv__NextDWExtractionCaptureTime)),'<null>') + '" 
					because of captures not yet extracted to the DW.';

			SET @lv__HardDeleteCaptureTime = DATEADD(second, -10, @lv__NextDWExtractionCaptureTime);
		END
	
		--Now, we scan the Sessions and Requests table, applying the above retention policies to each record to determine which ones
		-- are safe to purge (i.e. don't meet ANY of the retention policies). Each row in SessionsAndRequests is compared with every 
		-- retention policy we have, and if it meets ANY of those retention policies, the row is kept. Only if the SAR entry = "0"
		-- for every policy do we delete it.

		--To avoid contention with the AutoWho collector proc itself, we first obtain a "max committed SPIDCaptureTime" value that
		-- we will use with our queries to ensure that the records we're looking at are not close to the ones being inserted.
		-- Since none of the retention policies can be < 1 hour, we choose a time that is at least an hour back
		SELECT @lv__MaxSPIDCaptureTime = SPIDCaptureTime
		FROM (
			SELECT TOP 1 SPIDCaptureTime
			FROM AutoWho.SessionsAndRequests sar WITH (READPAST, ROWLOCK, READCOMMITTED)
			WHERE SPIDCaptureTime < DATEADD(HOUR, -1, GETDATE())
			AND CollectionInitiatorID = 255
			ORDER BY SPIDCaptureTime DESC
		) ss;


		/* NOTE: we don't filter by CollectionInitiatorID in the below query. At this point in time, we use the same
			purge policies for all data, regardless of how it was collected.
		*/

		SET @lv__ErrorLoc = N'#RecordsToPurge pop';
		INSERT INTO #RecordsToPurge (
			CollectionInitiatorID,
			SPIDCaptureTime,
			session_id,
			request_id,
			TimeIdentifier,
			Retain_IdleSPID_HighTempDB,
			Retain_IdleSPID_WithLongTran,
			Retain_IdleSPID_WithShortTran,
			Retain_IdleSPID_WithNoTran,
			Retain_ActiveLow,
			Retain_ActiveMedium,
			Retain_ActiveHigh,
			Retain_ActiveBatch,
			Retain_SpecialRows
		)
		SELECT DISTINCT 
			CollectionInitiatorID,
			SPIDCaptureTime, 
			session_id, 
			request_id, 
			TimeIdentifier,

			Retain_IdleSPID_HighTempDB,
			Retain_IdleSPID_WithLongTran,
			Retain_IdleSPID_WithShortTran,
			Retain_IdleSPID_WithNoTran,
			Retain_ActiveLow,
			Retain_ActiveMedium,
			Retain_ActiveHigh,
			Retain_ActiveBatch,
			Retain_SpecialRows
		FROM (
			SELECT 
				sar.CollectionInitiatorID,
				sar.SPIDCaptureTime, 
				sar.session_id, 
				sar.request_id, 
				sar.TimeIdentifier, 

				/* Retain_IdleSPID_HighTempDB is a reason to keep the row if:
					- idle spid; "High TempDB" retention only applies to idle spids b/c the goal of the retention (and the scoping inclusion that correlates to the retention policy)
						is for spids that were idle w/o a tran, but had a high enough tempdb usage that we want to capture them
					- TempDB usage is >= our "High TempDB" threshold
					- SPIDCaptureTime is more recent than @retainAfter__IdleSPIDs_HighTempDB
				*/
				[Retain_IdleSPID_HighTempDB] = CASE 
					WHEN sar.session_id > 0 
						AND sar.request_id = @lv__nullsmallint		
						AND (@opt__HighTempDBThreshold <=
								(
								CASE WHEN ISNULL([tempdb__sess_user_objects_alloc_page_count],0) - 
										ISNULL([tempdb__sess_user_objects_dealloc_page_count],0) < 0 THEN 0 
									ELSE ISNULL([tempdb__sess_user_objects_alloc_page_count],0) - 
										ISNULL([tempdb__sess_user_objects_dealloc_page_count],0)
									END + 

								CASE WHEN ISNULL([tempdb__sess_internal_objects_alloc_page_count],0) - 
										ISNULL([tempdb__sess_internal_objects_dealloc_page_count],0) < 0 THEN 0 
									ELSE ISNULL([tempdb__sess_internal_objects_alloc_page_count],0) - 
										ISNULL([tempdb__sess_internal_objects_dealloc_page_count],0)
									END + 

								CASE WHEN ISNULL([tempdb__task_user_objects_alloc_page_count],0) - 
										ISNULL([tempdb__task_user_objects_dealloc_page_count],0) < 0 THEN 0 
									ELSE ISNULL([tempdb__task_user_objects_alloc_page_count],0) - 
										ISNULL([tempdb__task_user_objects_dealloc_page_count],0)
									END + 

								CASE WHEN ISNULL([tempdb__task_internal_objects_alloc_page_count],0) - 
										ISNULL([tempdb__task_internal_objects_dealloc_page_count],0) < 0 THEN 0 
									ELSE ISNULL([tempdb__task_internal_objects_alloc_page_count],0) - 
										ISNULL([tempdb__task_internal_objects_dealloc_page_count],0)
									END
								)
							)

						AND @retainAfter__IdleSPIDs_HighTempDB < sar.SPIDCaptureTime
					THEN 1
					ELSE 0
					END,

				/* Retain_IdleSPID_WithLongTran is a reason to keep the row if:
					- idle spid
					- has an open transaction
					- has a tran length and it is >= our "long transaction" threshold
					- SPIDCaptureTime is > @retainAfter__IdleSPIDs_WithLongTran
				*/
				[Retain_IdleSPID_WithLongTran] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id = @lv__nullsmallint
						AND ISNULL(sar.sess__open_transaction_count,0) > 0
						AND (td.TranLength_sec IS NOT NULL	--"NOT NULL" unnecessary b/c of the AND'd >= clause, but just want to be explicit
															-- here that NULL tran lengths are handled by the "Short Tran" policy.
							AND td.TranLength_sec >= @opt__LongTransactionThreshold)
						AND @retainAfter__IdleSPIDs_WithLongTran < sar.SPIDCaptureTime
					THEN 1
					ELSE 0
					END,

				/* Retain_IdleSPID_WithShortTran is a reason to keep the row if:
					- idle spid
					- has an open transaction
					- the tran has no duration or the length is < our "Long Transaction" threshold
					- the SPIDCaptureTime is > @retainAfter__IdleSPIDs_WithShortTran
				*/
				[Retain_IdleSPID_WithShortTran] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id = @lv__nullsmallint
						AND ISNULL(sar.sess__open_transaction_count,0) > 0
						AND (td.TranLength_sec IS NULL 
							OR td.TranLength_sec < @opt__LongTransactionThreshold)

						AND @retainAfter__IdleSPIDs_WithShortTran < sar.SPIDCaptureTime
					THEN 1
					ELSE 0
					END,

				/* Retain_IdleSPID_WithNoTran is a reason to keep the row if:
					- idle spid
					- no transactions open
					- if blocker, SPIDCaptureTime is > @lv__HardDeleteCaptureTime
					- if not blocker, SPIDCaptureTime is > @retainAfter__IdleSPIDs_NoTran
				*/
				[Retain_IdleSPID_WithNoTran] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id = @lv__nullsmallint
						AND ISNULL(sar.sess__open_transaction_count,0) = 0
						AND (
							(sar.calc__is_blocker = 1
							AND @lv__HardDeleteCaptureTime < sar.SPIDCaptureTime
							)
							OR 
							(sar.calc__is_blocker = 0
							AND @retainAfter__IdleSPIDs_NoTran < sar.SPIDCaptureTime
							)
						)
					THEN 1
					ELSE 0
					END,
				
				/* Retain_ActiveLow is a reason to keep the row if:
					- active request
					- duration is < our medium threshold
					- SPIDCaptureTime is more recent than @retainAfter__ActiveLow
				*/
				[Retain_ActiveLow] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id <> @lv__nullsmallint
						AND sar.calc__duration_ms < @opt__MediumDurationThreshold*1000
						AND @retainAfter__ActiveLow < sar.SPIDCaptureTime
					THEN 1
					ELSE 0
					END,

				/* Retain_ActiveMedium is a reason to keep the row if:
					- active request
					- duration between our medium and high thresholds
					- SPIDCaptureTime is more recent than @retainAfter__ActiveMedium
				*/
				[Retain_ActiveMedium] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id <> @lv__nullsmallint
						AND (sar.calc__duration_ms >= @opt__MediumDurationThreshold*1000
							AND sar.calc__duration_ms < @opt__HighDurationThreshold*1000)
						AND @retainAfter__ActiveMedium < sar.SPIDCaptureTime
					THEN 1
					ELSE 0
					END,

				/* Retain_ActiveHigh is a reason to keep the row if:
					-active request
					-duration between our High and Batch thresholds
					-SPIDCaptureTime is more recent than @retainAfter__ActiveHigh
				*/
				[Retain_ActiveHigh] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id <> @lv__nullsmallint
						AND (sar.calc__duration_ms >= @opt__HighDurationThreshold*1000
							AND sar.calc__duration_ms < @opt__BatchDurationThreshold*1000)
						AND @retainAfter__ActiveHigh < sar.SPIDCaptureTime
					THEN 1
					ELSE 0
					END,

				/* Retain_ActiveBatch is a reason to keep the row if:
					- active request
					- duration >= our "batch duration threshold"
					- SPIDCaptureTime is more recent than @retainAfter__ActiveBatch
				*/
				[Retain_ActiveBatch] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id <> @lv__nullsmallint
						AND sar.calc__duration_ms >= @opt__BatchDurationThreshold*1000
						AND @retainAfter__ActiveBatch < sar.SPIDCaptureTime
					THEN 1
					ELSE 0
					END,

				[Retain_SpecialRows] = CASE 
					WHEN sar.session_id <= 0 AND @lv__HardDeleteCaptureTime < sar.SPIDCaptureTime
					THEN 1
					ELSE 0
					END
			FROM AutoWho.SessionsAndRequests sar WITH (READUNCOMMITTED)
				--THIS JOIN COPIED (WITH A FEW CHANGES) FROM AutoWho.PopulateCaptureSummary
				-- If that other logic changes, we should change it here as well. 
				LEFT OUTER JOIN (
					SELECT 
						td.CollectionInitiatorID,
						td.SPIDCaptureTime, 
						td.session_id,
						td.TimeIdentifier, 
							--since a spid could have transactions that span databases 
							-- (from the DMV's point of view, "multiple transactions"), we take the duration
							-- of the longest one.
						[TranLength_sec] = MAX(DATEDIFF(SECOND, td.dtat_transaction_begin_time,td.SPIDCaptureTime))
					FROM AutoWho.TransactionDetails td WITH (READUNCOMMITTED)
					WHERE ISNULL(td.dtdt_database_id,99999) <> 32767

					--This doesn't seem to mean what I think it means:
					--AND td.dtst_is_user_transaction = 1

					/* not sure whether I actually do want the below criteria
					--dtat transaction_type
					--		1 = Read/write transaction
					--		2 = Read-only transaction
					--		3 = System transaction
					--		4 = Distributed transaction
					AND td.dtat_transaction_type NOT IN (2, 3)		--we don't want trans that are read-only or system trans
					--dtdt database_transaction_type
					--		1 = Read/write transaction
					--		2 = Read-only transaction
					--		3 = System transaction
					AND td.dtdt_database_transaction_type NOT IN (2,3) --we don't want DB trans that are read-only or system trans
					*/
					GROUP BY td.CollectionInitiatorID, td.SPIDCaptureTime, td.session_id, td.TimeIdentifier
				) td
					ON sar.CollectionInitiatorID = td.CollectionInitiatorID
					AND sar.SPIDCaptureTime = td.SPIDCaptureTime
					AND sar.session_id = td.session_id
					AND sar.TimeIdentifier = td.TimeIdentifier
			WHERE sar.SPIDCaptureTime <= @lv__MaxSPIDCaptureTime
		) ss
		WHERE 
		--If any of the "Retain_" columns is 1, then we have at least 1 retention policy that gives us a reason
		--to keep the row, so it should not pass the below WHERE clauses.
			Retain_IdleSPID_HighTempDB = 0
		AND Retain_IdleSPID_WithLongTran = 0
		AND Retain_IdleSPID_WithShortTran = 0
		AND Retain_IdleSPID_WithNoTran = 0
		AND Retain_ActiveLow = 0
		AND Retain_ActiveMedium = 0
		AND Retain_ActiveHigh = 0
		AND Retain_ActiveBatch = 0
		AND Retain_SpecialRows = 0

		/* We have 1 final reason we might need to keep the row: the administrator has configured this install
			such that purge has to wait to remove data until it has been extracted to a DW. 

			So if @opt__PurgeUnextractedData = N'N', and SPIDCaptureTime >= @lv__NextDWExtractionCaptureTime, we must keep it.
			This only applies to data collected by AutoWho's background collector.
		*/
		AND 0 = (CASE WHEN @opt__PurgeUnextractedData = N'N' 
						AND ss.SPIDCaptureTime >= @lv__NextDWExtractionCaptureTime
						AND ss.CollectionInitiatorID = 255
						THEN 1
					ELSE 0
				END)
		OPTION(RECOMPILE, MAXDOP 4);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		IF @lv__tmpBigInt <= 0
		BEGIN
			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge', 'No rows found to be purged. Exiting...';

			DELETE FROM AutoWho.[Log] WHERE LogDT <= @lv__HardDeleteCaptureTime;
			RETURN 0;
		END

		SET @lv__ErrorLoc = N'#RecordsToPurge index';
		CREATE UNIQUE CLUSTERED INDEX CL1 ON #RecordsToPurge (
			CollectionInitiatorID,
			SPIDCaptureTime,
			session_id,
			request_id,
			TimeIdentifier
		);

		SET @lv__ErrorLoc = N'Final prep';
		SELECT 
			@lv__MinPurge_SPIDCaptureTime = ss.minnie,
			@lv__MaxPurge_SPIDCaptureTime = ss.maxie
		FROM (
			SELECT 
				MIN(SPIDCaptureTime)as minnie,
				MAX(SPIDCaptureTime) maxie
			FROM #RecordsToPurge 
		) ss;


		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows identified that have no reason to be retained, ranging from ' + 
			ISNULL(CONVERT(NVARCHAR(20), @lv__MinPurge_SPIDCaptureTime),N'<null>') + ' to ' + 
			ISNULL(CONVERT(NVARCHAR(20),@lv__MaxPurge_SPIDCaptureTime),N'<null>') + '.';

		SET @lv__ErrorLoc = N'Lock delete';
		DELETE targ 
		FROM AutoWho.LockDetails targ
			INNER JOIN #RecordsToPurge r
				ON targ.CollectionInitiatorID = r.CollectionInitiatorID
				AND targ.SPIDCaptureTime = r.SPIDCaptureTime
				AND targ.request_session_id = r.session_id
				AND targ.request_request_id = r.request_id
				AND targ.TimeIdentifier = r.TimeIdentifier
		WHERE targ.SPIDCaptureTime >= @lv__MinPurge_SPIDCaptureTime
		AND targ.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime
		OPTION(RECOMPILE);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.LockDetails.';

		--If rows somehow slip by our above criteria, we delete anything older than our hard-delete boundary
		DELETE targ 
		FROM AutoWho.LockDetails targ 
		WHERE targ.SPIDCaptureTime <= @lv__HardDeleteCaptureTime; 

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows hard-deleted from AutoWho.LockDetails.';

		SET @lv__ErrorLoc = N'tran delete';
		DELETE targ 
		FROM AutoWho.TransactionDetails targ
			INNER JOIN #RecordsToPurge r
				ON targ.CollectionInitiatorID = r.CollectionInitiatorID
				AND targ.SPIDCaptureTime = r.SPIDCaptureTime
				AND targ.session_id = r.session_id
				AND targ.TimeIdentifier = r.TimeIdentifier
		WHERE targ.SPIDCaptureTime >= @lv__MinPurge_SPIDCaptureTime
		AND targ.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime
		OPTION(RECOMPILE);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.TransactionDetails.';

		DELETE targ 
		FROM AutoWho.TransactionDetails targ
		WHERE targ.SPIDCaptureTime < @lv__HardDeleteCaptureTime;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows hard-deleted from AutoWho.TransactionDetails.';

		SET @lv__ErrorLoc = N'TAW delete';
		DELETE targ 
		FROM AutoWho.TasksAndWaits targ 
			INNER JOIN #RecordsToPurge r
				ON targ.CollectionInitiatorID = r.CollectionInitiatorID
				AND targ.SPIDCaptureTime = r.SPIDCaptureTime
				AND targ.session_id = r.session_id
				AND targ.request_id = r.request_id
		WHERE targ.SPIDCaptureTime >= @lv__MinPurge_SPIDCaptureTime
		AND targ.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime
		OPTION(RECOMPILE);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.TasksAndWaits.';

		DELETE targ 
		FROM AutoWho.TasksAndWaits targ 
		WHERE targ.SPIDCaptureTime < @lv__HardDeleteCaptureTime;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows hard-deleted from AutoWho.TasksAndWaits.';

		SET @lv__ErrorLoc = N'SAR delete';
		DELETE targ 
		FROM AutoWho.SessionsAndRequests targ 
			INNER JOIN #RecordsToPurge r
				ON targ.CollectionInitiatorID = r.CollectionInitiatorID
				AND targ.SPIDCaptureTime = r.SPIDCaptureTime
				AND targ.session_id = r.session_id
				AND targ.request_id = r.request_id
				AND targ.TimeIdentifier = r.TimeIdentifier
		WHERE targ.SPIDCaptureTime >= @lv__MinPurge_SPIDCaptureTime
		AND targ.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime
		;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.SessionsAndRequests.';

		DELETE targ 
		FROM AutoWho.SessionsAndRequests targ
		WHERE targ.SPIDCaptureTime < @lv__HardDeleteCaptureTime;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows hard-deleted from AutoWho.SessionsAndRequests.';

		--With the BlockingGraphs table, we only delete records for capture times
		-- where there are NO remaining spids in SAR for that capture time
		SET @lv__ErrorLoc = N'BG delete';
		DELETE targ 
		FROM AutoWho.BlockingGraphs targ
		WHERE targ.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime
		AND NOT EXISTS (
			SELECT *
			FROM AutoWho.SessionsAndRequests sar
			WHERE sar.CollectionInitiatorID = targ.CollectionInitiatorID
			AND sar.SPIDCaptureTime = targ.SPIDCaptureTime
			AND sar.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime		--avoid conflicts with AutoWho
			AND sar.session_id > 0		--don't let a special row keep us from deleting a blocking graph
		)
		OPTION(RECOMPILE);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.BlockingGraphs.';

		/* For the "Store" tables, which aren't tied to any single SPIDCaptureTime, there are 3 main criteria:

			- Store table must be a non-trivial size
			- Store entry is not referenced anymore
			- Store entry's last-touched datetime must be older than our longest retention period (except for the hard-delete retention)
		*/

		SET @lv__ErrorLoc = N'Store prep';
		SELECT @max__RetentionHours = ss1.col1
		FROM (
			SELECT TOP 1 col1 
			FROM (
				SELECT @opt__Retention_IdleSPIDs_NoTran as col1	UNION
				SELECT @opt__Retention_IdleSPIDs_WithShortTran UNION
				SELECT @opt__Retention_IdleSPIDs_WithLongTran UNION
				SELECT @opt__Retention_IdleSPIDs_HighTempDB UNION
				SELECT @opt__Retention_ActiveLow UNION
				SELECT @opt__Retention_ActiveMedium	UNION
				SELECT @opt__Retention_ActiveHigh UNION
				SELECT @opt__Retention_ActiveBatch
			) ss0
			ORDER BY col1 DESC
		) ss1;
		--if NULL somehow (this shouldn't happen), default to a week.
		SET @max__RetentionHours = ISNULL(@max__RetentionHours,168); 

		--One scan through the SAR table to construct a distinct-keys list is much
		-- more efficient than the previous code, which joined SAR in every DELETE
		--Note that we totally ignore CollectionInitiatorID here.
		INSERT INTO #AutoWhoDistinctStoreKeys (
			[FKSQLStmtStoreID],
			[FKSQLBatchStoreID],
			[FKInputBufferStoreID],
			[FKQueryPlanBatchStoreID],
			[FKQueryPlanStmtStoreID]
		)
		SELECT DISTINCT 
			sar.FKSQLStmtStoreID,
			sar.FKSQLBatchStoreID,
			sar.FKInputBufferStoreID,
			sar.FKQueryPlanBatchStoreID,
			sar.FKQueryPlanStmtStoreID
		FROM AutoWho.SessionsAndRequests sar WITH (NOLOCK)
		;

		SET @lv__ErrorLoc = N'IB delete';
		SELECT @lv__TableSize_ReservedPages = ss.rsvdpgs
		FROM (
			SELECT SUM(ps.reserved_page_count) as rsvdpgs
			FROM sys.dm_db_partition_stats ps
				INNER JOIN sys.objects o
					ON ps.object_id = o.object_id
			WHERE o.name = N'InputBufferStore'
			AND o.type = 'U'
		) ss;

		IF @lv__TableSize_ReservedPages > 250*1024/8		--250 MB
		BEGIN
			TRUNCATE TABLE #StoreTableIDsToPurge;

			INSERT INTO #StoreTableIDsToPurge (
				ID 
			)
			SELECT targ.PKInputBufferStoreID
			FROM (SELECT DISTINCT sar.FKInputBufferStoreID 
					FROM #AutoWhoDistinctStoreKeys sar WITH (NOLOCK)
					WHERE sar.FKInputBufferStoreID IS NOT NULL) sar
				RIGHT OUTER hash JOIN CoreXR.InputBufferStore targ 
					ON targ.PKInputBufferStoreID = sar.FKInputBufferStoreID
			WHERE targ.LastTouchedBy_SPIDCaptureTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
			AND sar.FKInputBufferStoreID IS NULL 
			OPTION(RECOMPILE, FORCE ORDER);

			SELECT @lv__tmpMinID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID ASC) xapp1;

			SELECT @lv__tmpMaxID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID DESC) xapp1;

			DELETE targ 
			FROM #StoreTableIDsToPurge t
				INNER hash JOIN CoreXR.InputBufferStore targ
					ON targ.PKInputBufferStoreID = t.ID
			WHERE targ.PKInputBufferStoreID BETWEEN @lv__tmpMinID AND @lv__tmpMaxID
			OPTION(FORCE ORDER, RECOMPILE);

			SET @lv__tmpBigInt = ROWCOUNT_BIG();

			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge',
				ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CoreXR.InputBufferStore.';
		END

		SET @lv__ErrorLoc = N'QPBS delete';
		SELECT @lv__TableSize_ReservedPages = ss.rsvdpgs
		FROM (
			SELECT SUM(ps.reserved_page_count) as rsvdpgs
			FROM sys.dm_db_partition_stats ps
				INNER JOIN sys.objects o
					ON ps.object_id = o.object_id
			WHERE o.name = N'QueryPlanBatchStore'
			AND o.type = 'U'
		) ss;

		IF @lv__TableSize_ReservedPages > 500*1024/8
		BEGIN
			TRUNCATE TABLE #StoreTableIDsToPurge;

			INSERT INTO #StoreTableIDsToPurge (
				ID 
			)
			SELECT targ.PKQueryPlanBatchStoreID
			FROM (SELECT DISTINCT sar.FKQueryPlanBatchStoreID 
					FROM #AutoWhoDistinctStoreKeys sar WITH (NOLOCK)
					WHERE sar.FKQueryPlanBatchStoreID IS NOT NULL) sar
				RIGHT OUTER hash JOIN CoreXR.QueryPlanBatchStore targ
					ON targ.PKQueryPlanBatchStoreID = sar.FKQueryPlanBatchStoreID
			WHERE targ.LastTouchedBy_SPIDCaptureTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
			AND sar.FKQueryPlanBatchStoreID IS NULL
			OPTION(RECOMPILE, FORCE ORDER);

			SELECT @lv__tmpMinID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID ASC) xapp1;

			SELECT @lv__tmpMaxID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID DESC) xapp1;

			DELETE targ 
			FROM #StoreTableIDsToPurge t
				INNER hash JOIN CoreXR.QueryPlanBatchStore targ
					ON targ.PKQueryPlanBatchStoreID = t.ID
			WHERE targ.PKQueryPlanBatchStoreID BETWEEN @lv__tmpMinID AND @lv__tmpMaxID
			OPTION(FORCE ORDER, RECOMPILE);

			SET @lv__tmpBigInt = ROWCOUNT_BIG();

			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge', 
				ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CoreXR.QueryPlanBatchStore.';
		END

		SET @lv__ErrorLoc = N'QPSS delete';
		SELECT @lv__TableSize_ReservedPages = ss.rsvdpgs
		FROM (
			SELECT SUM(ps.reserved_page_count) as rsvdpgs
			FROM sys.dm_db_partition_stats ps
				INNER JOIN sys.objects o
					ON ps.object_id = o.object_id
			WHERE o.name = N'QueryPlanStmtStore'
			AND o.type = 'U'
		) ss;

		IF @lv__TableSize_ReservedPages > 500*1024/8
		BEGIN
			TRUNCATE TABLE #StoreTableIDsToPurge;

			INSERT INTO #StoreTableIDsToPurge (
				ID 
			)
			SELECT targ.PKQueryPlanStmtStoreID
			FROM (SELECT DISTINCT sar.FKQueryPlanStmtStoreID 
					FROM #AutoWhoDistinctStoreKeys sar WITH (NOLOCK)
					WHERE sar.FKQueryPlanStmtStoreID IS NOT NULL) sar
				RIGHT OUTER hash JOIN CoreXR.QueryPlanStmtStore targ
					ON targ.PKQueryPlanStmtStoreID = sar.FKQueryPlanStmtStoreID
			WHERE targ.LastTouchedBy_SPIDCaptureTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
			AND sar.FKQueryPlanStmtStoreID IS NULL
			OPTION(RECOMPILE, FORCE ORDER);

			SELECT @lv__tmpMinID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID ASC) xapp1;

			SELECT @lv__tmpMaxID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID DESC) xapp1;

			DELETE targ 
			FROM #StoreTableIDsToPurge t
				INNER hash JOIN CoreXR.QueryPlanStmtStore targ
					ON targ.PKQueryPlanStmtStoreID = t.ID
			WHERE targ.PKQueryPlanStmtStoreID BETWEEN @lv__tmpMinID AND @lv__tmpMaxID
			OPTION(FORCE ORDER, RECOMPILE);

			SET @lv__tmpBigInt = ROWCOUNT_BIG();

			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge', 
				ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CoreXR.QueryPlanStmtStore.';
		END

		SET @lv__ErrorLoc = N'SBS delete';
		SELECT @lv__TableSize_ReservedPages = ss.rsvdpgs
		FROM (
			SELECT SUM(ps.reserved_page_count) as rsvdpgs
			FROM sys.dm_db_partition_stats ps
				INNER JOIN sys.objects o
					ON ps.object_id = o.object_id
			WHERE o.name = N'SQLBatchStore'
			AND o.type = 'U'
		) ss;

		IF @lv__TableSize_ReservedPages > 500*1024/8
		BEGIN
			TRUNCATE TABLE #StoreTableIDsToPurge;

			INSERT INTO #StoreTableIDsToPurge (
				ID 
			)
			SELECT targ.PKSQLBatchStoreID
			FROM (SELECT DISTINCT sar.FKSQLBatchStoreID 
					FROM #AutoWhoDistinctStoreKeys sar WITH (NOLOCK)
					WHERE sar.FKSQLBatchStoreID IS NOT NULL) sar
				RIGHT OUTER hash JOIN CoreXR.SQLBatchStore targ
					ON targ.PKSQLBatchStoreID = sar.FKSQLBatchStoreID
			WHERE targ.LastTouchedBy_SPIDCaptureTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
			AND sar.FKSQLBatchStoreID IS NULL
			OPTION(RECOMPILE, FORCE ORDER);

			SELECT @lv__tmpMinID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID ASC) xapp1;

			SELECT @lv__tmpMaxID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID DESC) xapp1;

			DELETE targ 
			FROM #StoreTableIDsToPurge t
				INNER hash JOIN CoreXR.SQLBatchStore targ
					ON targ.PKSQLBatchStoreID = t.ID
			WHERE targ.PKSQLBatchStoreID BETWEEN @lv__tmpMinID AND @lv__tmpMaxID
			OPTION(FORCE ORDER, RECOMPILE);

			SET @lv__tmpBigInt = ROWCOUNT_BIG();
			SET @lv__tmpStr = ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CoreXR.SQLBatchStore.';

			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge', @lv__tmpStr;
		END

		SET @lv__ErrorLoc = N'SSS delete';
		SELECT @lv__TableSize_ReservedPages = ss.rsvdpgs
		FROM (
			SELECT SUM(ps.reserved_page_count) as rsvdpgs
			FROM sys.dm_db_partition_stats ps
				INNER JOIN sys.objects o
					ON ps.object_id = o.object_id
			WHERE o.name = N'SQLStmtStore'
			AND o.type = 'U'
		) ss;

		IF @lv__TableSize_ReservedPages > 500*1024/8
		BEGIN
			TRUNCATE TABLE #StoreTableIDsToPurge;

			INSERT INTO #StoreTableIDsToPurge (ID)
			SELECT targ.PKSQLStmtStoreID
			FROM (SELECT DISTINCT sar.FKSQLStmtStoreID 
					FROM #AutoWhoDistinctStoreKeys sar WITH (NOLOCK)
					WHERE sar.FKSQLStmtStoreID IS NOT NULL) sar
				RIGHT OUTER hash JOIN CoreXR.SQLStmtStore targ
					ON targ.PKSQLStmtStoreID = sar.FKSQLStmtStoreID
			WHERE targ.LastTouchedBy_SPIDCaptureTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
			AND sar.FKSQLStmtStoreID IS NULL
			OPTION(RECOMPILE, FORCE ORDER);

			SELECT @lv__tmpMinID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID ASC) xapp1;

			SELECT @lv__tmpMaxID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID DESC) xapp1;

			DELETE targ 
			FROM #StoreTableIDsToPurge t
				INNER hash JOIN CoreXR.SQLStmtStore targ
					ON targ.PKSQLStmtStoreID = t.ID
			WHERE targ.PKSQLStmtStoreID BETWEEN @lv__tmpMinID AND @lv__tmpMaxID
			OPTION(FORCE ORDER, RECOMPILE);

			SET @lv__tmpBigInt = ROWCOUNT_BIG();

			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge', 
				ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CoreXR.SQLStmtStore.';
		END

		DELETE targ 
		FROM AutoWho.StatementCaptureTimes targ
		WHERE targ.SPIDCaptureTime <= @lv__HardDeleteCaptureTime;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.StatementCaptureTimes.';

		DELETE targ 
		FROM AutoWho.UserCollectionTimes targ
		WHERE targ.SPIDCaptureTime <= @lv__HardDeleteCaptureTime;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.UserCollectionTimes.';

		--LightweightSessions, LightweightTasks, LightweightTrans, SARException, TAWException
		-- since these are heaps, we use tablock to allow the pages to be deallocated
		SET @lv__ErrorLoc = N'Lightweight deletes';
		DELETE FROM AutoWho.LightweightSessions WITH (TABLOCK)
		WHERE SPIDCaptureTime < @lv__HardDeleteCaptureTime;

		DELETE FROM AutoWho.LightweightTasks WITH (TABLOCK)
		WHERE SPIDCaptureTime < @lv__HardDeleteCaptureTime;

		DELETE FROM AutoWho.LightweightTrans WITH (TABLOCK)
		WHERE SPIDCaptureTime < @lv__HardDeleteCaptureTime;

		DELETE FROM AutoWho.SARException WITH (TABLOCK)
		WHERE SPIDCaptureTime < @lv__HardDeleteCaptureTime;

		DELETE FROM AutoWho.TAWException WITH (TABLOCK)
		WHERE SPIDCaptureTime < @lv__HardDeleteCaptureTime;

		--Get rid of metadata
		SET @lv__ErrorLoc = N'Metadata Deletes';
		DELETE AutoWho.CaptureTimes
		WHERE SPIDCaptureTime <= @lv__MaxSPIDCaptureTime
		AND SPIDCaptureTime <= @lv__HardDeleteCaptureTime;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.CaptureTimes.';

		DELETE targ 
		FROM AutoWho.CaptureSummary targ 
		WHERE targ.SPIDCaptureTime <= @lv__MaxSPIDCaptureTime
		AND NOT EXISTS (
			SELECT *
			FROM AutoWho.CaptureTimes ct
			WHERE ct.SPIDCaptureTime = targ.SPIDCaptureTime
		);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.CaptureSummary.';

		--We just (potentially) deleted rows from AutoWho.CaptureTimes. Any ordinal caches that contain capture times
		--that were just removed are no longer useful. Delete these, and the position markers that depend on them
		IF OBJECT_ID('tempdb..#invalidOrdCache') IS NOT NULL DROP TABLE #invalidOrdCache;
		CREATE TABLE #invalidOrdCache (
			Utility					NVARCHAR(30) NOT NULL,
			CollectionInitiatorID	TINYINT NOT NULL,
			StartTime				DATETIME NOT NULL,
			EndTime					DATETIME NOT NULL
		);

		INSERT INTO #invalidOrdCache (
			Utility,
			CollectionInitiatorID,
			StartTime,
			EndTime
		)
		SELECT DISTINCT 
			ord.Utility,
			ord.CollectionInitiatorID,
			ord.StartTime, 
			ord.EndTime
		FROM CoreXR.CaptureOrdinalCache ord
		WHERE ord.Utility IN (N'AutoWho',N'SessionViewer',N'QueryProgress')		--AutoWho-related utilities only
		AND NOT EXISTS (
			SELECT *
			FROM AutoWho.CaptureTimes ct
			WHERE ct.SPIDCaptureTime = ord.CaptureTime
		);

		DELETE p
		FROM CoreXR.OrdinalCachePosition p
		WHERE EXISTS (
			SELECT * 
			FROM #invalidOrdCache t
			WHERE t.Utility = p.Utility
			AND t.CollectionInitiatorID = p.CollectionInitiatorID
			AND t.StartTime = p.StartTime
			AND t.EndTime = p.EndTime
		);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.CaptureOrdinalCache.';

		DELETE c
		FROM CoreXR.CaptureOrdinalCache c
		WHERE EXISTS (
			SELECT * 
			FROM #invalidOrdCache t
			WHERE t.Utility = c.Utility
			AND t.CollectionInitiatorID = c.CollectionInitiatorID
			AND t.StartTime = c.StartTime
			AND t.EndTime = c.EndTime
		);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.CaptureOrdinalCache.';

		DELETE FROM CoreXR.[Traces]
		WHERE Utility = N'AutoWho'
		AND CreateTime <= @lv__HardDeleteCaptureTime;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CoreXR.Traces.';

		DELETE FROM AutoWho.[Log] WHERE LogDT <= @lv__HardDeleteCaptureTime;
		RETURN 0;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK;

		SET @lv__ErrorState = ERROR_STATE();
		SET @lv__ErrorSeverity = ERROR_SEVERITY();

		SET @lv__ErrorMessage = N'Exception occurred in procedure: ' + OBJECT_NAME(@@PROCID) + N' at location ("' + ISNULL(@lv__ErrorLoc,N'<null>') + '"). 
		Error #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + '; Message: ' + ISNULL(ERROR_MESSAGE(), N'<null>');

		RAISERROR(@lv__ErrorMessage, @lv__ErrorSeverity, @lv__ErrorState);
		RETURN -999;
	END CATCH
END
GO
