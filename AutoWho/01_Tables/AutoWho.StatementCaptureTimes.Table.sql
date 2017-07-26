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

	FILE NAME: AutoWho.StatementCaptureTimes.Table.sql

	TABLE NAME: AutoWho.StatementCaptureTimes

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Marks start and end times for user statements and batches observed by AutoWho.
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [AutoWho].[StatementCaptureTimes] (
	--Identifier columns
	[session_id]			[smallint] NOT NULL,
	[request_id]			[smallint] NOT NULL,
	[TimeIdentifier]		[datetime] NOT NULL,
	[SPIDCaptureTime]		[datetime] NOT NULL,

	--attribute cols
	[StatementFirstCapture] [datetime] NOT NULL,	--The first SPIDCaptureTime for the statement that this row belongs to. This acts as a grouping
													--field (that is also ascending as statements run for the batch! a nice property)
	[StatementSequenceNumber] [int] NOT NULL,		--statement # within the batch. We use this instead of PKSQLStmtStoreID b/c that could be revisited

	[PKSQLStmtStoreID]		[bigint] NOT NULL,		--TODO: still need to implement TMR wait logic. Note that for TMR waits, the current plan is to 
													--*always* assume it is a new statement even if the calc__tmr_wait value matches between the 
													--most recent SPIDCaptureTime in this table and the "current" statement.

	[rqst__query_hash]		[binary](8) NULL,		--storing this makes some presentation procs more quickly able to find high-frequency queries.

	--These fields start at 0 and are only set to 1 when we KNOW that a row is the first and/or last of a statement or batch.
	--Thus, once set to 1 they should never change.
	[IsStmtFirstCapture]	[bit] NOT NULL,
	[IsStmtLastCapture]		[bit] NOT NULL,
	[IsBatchFirstCapture]	[bit] NOT NULL,
	[IsBatchLastCapture]	[bit] NOT NULL,

	--This is set to 1 when we consider a batch still be active (i.e. a run of our post-processing proc finds it still in closing set).
	--This allows us to find this row quickly on a future post-processing run, and then include it in the "working set".
	[IsCurrentLastRowOfBatch]	[bit] NOT NULL,
 CONSTRAINT [PKAutoWhoStatementCaptureTimes] PRIMARY KEY CLUSTERED 
(
	[session_id] ASC,
	[request_id] ASC,
	[TimeIdentifier] ASC,
	[SPIDCaptureTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
--We keep the PK cols in the NCL key to support merge joins on the commonly-joined cols (after a seek on IsCurrentLastRowOfBatch=1)
CREATE NONCLUSTERED INDEX [NCL_ActiveBatchFinalRow] ON [AutoWho].[StatementCaptureTimes]
(
	[IsCurrentLastRowOfBatch] ASC,
	[session_id] ASC,
	[request_id] ASC,
	[TimeIdentifier] ASC,
	[SPIDCaptureTime] ASC
)
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
