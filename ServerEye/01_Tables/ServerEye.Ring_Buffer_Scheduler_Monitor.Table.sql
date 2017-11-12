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

	FILE NAME: ServerEye.Ring_Buffer_Scheduler_Monitor.Table.sql

	TABLE NAME: ServerEye.Ring_Buffer_Scheduler_Monitor

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Snapshots contents from the SCHEDULER_MONITOR ring buffer (in Low-frequency metrics)
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [ServerEye].[Ring_Buffer_Scheduler_Monitor](
	[SQLServerStartTime] [datetime] NOT NULL,
	[RecordID] [bigint] NOT NULL,
	[timestamp] [bigint] NOT NULL,
	[ExceptionTime] [datetime] NOT NULL,
	[UTCCaptureTime] [datetime] NOT NULL,
	[LocalCaptureTime] [datetime] NOT NULL,
	[ProcessUtilization] [int] NULL,
	[SystemIdle] [int] NULL,
	[UserModeTime] [int] NULL,
	[KernelModeTime] [int] NULL,
	[PageFaults] [int] NULL,
	[WorkingSetDelta] [int] NULL,
	[MemoryUtilization] [int] NULL,
 CONSTRAINT [PK_Ring_Buffer_Scheduler_Monitor] PRIMARY KEY NONCLUSTERED 
(
	[SQLServerStartTime] ASC,
	[RecordID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
CREATE CLUSTERED INDEX CL1 ON [ServerEye].[Ring_Buffer_Scheduler_Monitor](UTCCaptureTime);
GO