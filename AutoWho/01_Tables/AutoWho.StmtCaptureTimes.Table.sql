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

	FILE NAME: AutoWho.StmtCaptureTimes.Table.sql

	TABLE NAME: AutoWho.StmtCaptureTimes

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Holds aggregated statistics about user statements that have been observed by AutoWho
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [AutoWho].[StmtCaptureTimes] (
	--Identifier columns
	[session_id]			[smallint] NOT NULL,
	[request_id]			[smallint] NOT NULL,
	[TimeIdentifier]		[datetime] NOT NULL,
	[StatementSequenceNumber] [int] NOT NULL,		--statement # within the batch. We use this instead of PKSQLStmtStoreID b/c that could be revisited
	[SPIDCaptureTime]		[datetime] NOT NULL,

	--attribute cols
	[PKSQLStmtStoreID]		[bigint] NOT NULL,		--we set this to -1 if it is NULL in SAR. (This is typically TMR waits, I think)
													--Note that for TMR waits, for now we *always* assume it is a new statement even if
													--the calc__tmr_wait value matches between the most recent SPIDCaptureTime in this table
													--and the "current" statement.
	[rqst__query_hash]		[binary](8) NULL,

	--These fields are only set to 1 when we KNOW that a row is the first and/or last of a statement or batch
	[IsStmtFirstCapture]	[bit] NOT NULL,
	[IsStmtLastCapture]		[bit] NOT NULL,
	[IsBatchFirstCapture]	[bit] NOT NULL,
	[IsBatchLastCapture]	[bit] NOT NULL,

	--These fields are set to 1 when we consider a batch/stmt still be active (i.e. is in closing set) so that we can
	--find it quickly on the next run.
	[IsCurrentLastRowOfBatch]	[bit] NOT NULL,
	[IsCurrentLastRowOfStmt]	[bit] NOT NULL,		--TODO: don't know if I actually need this? curlastrowofbatch may be enough
 CONSTRAINT [PKAutoWhoStmtCaptureTimes] PRIMARY KEY CLUSTERED 
(
	[session_id] ASC,
	[request_id] ASC,
	[TimeIdentifier] ASC,
	[StatementSequenceNumber] ASC,
	[SPIDCaptureTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
