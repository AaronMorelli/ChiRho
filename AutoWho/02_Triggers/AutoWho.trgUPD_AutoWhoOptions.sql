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
*/
/*
	FILE NAME: AutoWho.trgUPD_AutoWhoOptions.sql

	TRIGGER NAME: AutoWho.trgUPD_AutoWhoOptions

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: Copies data updated in the Options table to the history table.
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [AutoWho].[trgUPD_AutoWhoOptions] ON [AutoWho].[Options]

FOR UPDATE
AS 	BEGIN

INSERT INTO AutoWho.Options_History 
(RowID, AutoWhoEnabled, BeginTime, EndTime, IntervalLength, IncludeIdleWithTran, IncludeIdleWithoutTran, DurationFilter, IncludeDBs, ExcludeDBs, HighTempDBThreshold, CollectSystemSpids, HideSelf, ObtainBatchText, ObtainQueryPlanForStatement, ObtainQueryPlanForBatch, ObtainLocksForBlockRelevantThreshold, InputBufferThreshold, ParallelWaitsThreshold, QueryPlanThreshold, QueryPlanThresholdBlockRel, BlockingChainThreshold, BlockingChainDepth, TranDetailsThreshold, MediumDurationThreshold, HighDurationThreshold, BatchDurationThreshold, LongTransactionThreshold, Retention_IdleSPIDs_NoTran, Retention_IdleSPIDs_WithShortTran, Retention_IdleSPIDs_WithLongTran, Retention_IdleSPIDs_HighTempDB, Retention_ActiveLow, Retention_ActiveMedium, Retention_ActiveHigh, Retention_ActiveBatch, Retention_CaptureTimes, DebugSpeed, ThresholdFilterRefresh, SaveBadDims, Enable8666, ResolvePageLatches, ResolveLockWaits, 
HistoryInsertDate,
TriggerAction,
LastModifiedUser)
SELECT 
RowID, AutoWhoEnabled, BeginTime, EndTime, IntervalLength, IncludeIdleWithTran, IncludeIdleWithoutTran, DurationFilter, IncludeDBs, ExcludeDBs, HighTempDBThreshold, CollectSystemSpids, HideSelf, ObtainBatchText, ObtainQueryPlanForStatement, ObtainQueryPlanForBatch, ObtainLocksForBlockRelevantThreshold, InputBufferThreshold, ParallelWaitsThreshold, QueryPlanThreshold, QueryPlanThresholdBlockRel, BlockingChainThreshold, BlockingChainDepth, TranDetailsThreshold, MediumDurationThreshold, HighDurationThreshold, BatchDurationThreshold, LongTransactionThreshold, Retention_IdleSPIDs_NoTran, Retention_IdleSPIDs_WithShortTran, Retention_IdleSPIDs_WithLongTran, Retention_IdleSPIDs_HighTempDB, Retention_ActiveLow, Retention_ActiveMedium, Retention_ActiveHigh, Retention_ActiveBatch, Retention_CaptureTimes, DebugSpeed, ThresholdFilterRefresh, SaveBadDims, Enable8666, ResolvePageLatches, ResolveLockWaits, 
getdate(),
'Update',
SUSER_SNAME()
FROM inserted

END
GO


