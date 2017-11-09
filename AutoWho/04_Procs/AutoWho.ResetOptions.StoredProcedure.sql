SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ResetOptions] 
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

	FILE NAME: AutoWho.ResetOptions.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.ResetOptions

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Deletes the row in AutoWho.Options and re-inserts a row based on default values

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
EXEC AutoWho.ResetOptions

SELECT * FROM AutoWho.Options
*/
AS
BEGIN
	SET NOCOUNT ON;

	INSERT INTO AutoWho.Options_History
	(
	RowID, AutoWhoEnabled, BeginTime, EndTime, BeginEndIsUTC, IntervalLength, IncludeIdleWithTran, IncludeIdleWithoutTran, DurationFilter, IncludeDBs, ExcludeDBs, HighTempDBThreshold, CollectSystemSpids, HideSelf, ObtainBatchText, ObtainQueryPlanForStatement, ObtainQueryPlanForBatch, ObtainLocksForBlockRelevantThreshold, InputBufferThreshold, ParallelWaitsThreshold, QueryPlanThreshold, QueryPlanThresholdBlockRel, BlockingChainThreshold, BlockingChainDepth, TranDetailsThreshold, MediumDurationThreshold, HighDurationThreshold, BatchDurationThreshold, LongTransactionThreshold, Retention_IdleSPIDs_NoTran, Retention_IdleSPIDs_WithShortTran, Retention_IdleSPIDs_WithLongTran, Retention_IdleSPIDs_HighTempDB, Retention_ActiveLow, Retention_ActiveMedium, Retention_ActiveHigh, Retention_ActiveBatch, Retention_CaptureTimes, DebugSpeed, ThresholdFilterRefresh, SaveBadDims, Enable8666, ResolvePageLatches, ResolveLockWaits, PurgeUnextractedData,
	HistoryInsertDate,
	HistoryInsertDateUTC,
	TriggerAction,
	LastModifiedUser)
	SELECT 
	RowID, AutoWhoEnabled, BeginTime, EndTime, BeginEndIsUTC, IntervalLength, IncludeIdleWithTran, IncludeIdleWithoutTran, DurationFilter, IncludeDBs, ExcludeDBs, HighTempDBThreshold, CollectSystemSpids, HideSelf, ObtainBatchText, ObtainQueryPlanForStatement, ObtainQueryPlanForBatch, ObtainLocksForBlockRelevantThreshold, InputBufferThreshold, ParallelWaitsThreshold, QueryPlanThreshold, QueryPlanThresholdBlockRel, BlockingChainThreshold, BlockingChainDepth, TranDetailsThreshold, MediumDurationThreshold, HighDurationThreshold, BatchDurationThreshold, LongTransactionThreshold, Retention_IdleSPIDs_NoTran, Retention_IdleSPIDs_WithShortTran, Retention_IdleSPIDs_WithLongTran, Retention_IdleSPIDs_HighTempDB, Retention_ActiveLow, Retention_ActiveMedium, Retention_ActiveHigh, Retention_ActiveBatch, Retention_CaptureTimes, DebugSpeed, ThresholdFilterRefresh, SaveBadDims, Enable8666, ResolvePageLatches, ResolveLockWaits, PurgeUnextractedData,
	GETDATE(),
	GETUTCDATE(),
	'Reset',
	SUSER_NAME()
	FROM AutoWho.Options;

	DISABLE TRIGGER AutoWho.trgDEL_AutoWhoOptions ON AutoWho.Options;

	DELETE FROM AutoWho.Options;

	ENABLE TRIGGER AutoWho.trgDEL_AutoWhoOptions ON AutoWho.Options;

	INSERT INTO AutoWho.Options 
		DEFAULT VALUES;

	RETURN 0;
END



GO
