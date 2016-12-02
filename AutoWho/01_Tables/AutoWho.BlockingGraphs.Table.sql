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
	FILE NAME: AutoWho.BlockingGraphs.Table.sql

	TABLE NAME: AutoWho.BlockingGraphs

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: Holds a tabular, intermediate representation of the Blocking Graph functionality
	displayed by sp_XR_SessionViewer.
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [AutoWho].[BlockingGraphs](
	[SPIDCaptureTime] [datetime] NOT NULL,
	[session_id] [smallint] NOT NULL,
	[request_id] [smallint] NOT NULL,
	[exec_context_id] [smallint] NULL,
	[calc__blocking_session_Id] [smallint] NULL,
	[wait_type] [nvarchar](60) NULL,
	[wait_duration_ms] [bigint] NULL,
	[resource_description] [nvarchar](500) NULL,
	[FKInputBufferStoreID] [bigint] NULL,
	[FKSQLStmtStoreID] [bigint] NULL,
	[sort_value] [nvarchar](400) NULL,
	[block_group] [smallint] NULL,
	[levelindc] [smallint] NOT NULL,
	[rn] [smallint] NOT NULL
) ON [PRIMARY]

GO
CREATE CLUSTERED INDEX [CL_SPIDCaptureTime] ON [AutoWho].[BlockingGraphs]
(
	[SPIDCaptureTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
