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

	FILE NAME: ServerEye.dm_os_workers.Table.sql

	TABLE NAME: ServerEye.dm_os_workers

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Snapshots sys.dm_os_workers (in High-frequency metrics)
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [ServerEye].[dm_os_workers](
	[UTCCaptureTime]		[datetime] NOT NULL,
	[LocalCaptureTime]		[datetime] NOT NULL,
	[worker_address]		[varbinary](8) NOT NULL,
	[is_preemptive]			[bit] NULL,
	[is_sick]				[bit] NULL,
	[is_in_cc_exception]	[bit] NULL,
	[is_fatal_exception]	[bit] NULL,
	[is_inside_catch]		[bit] NULL,
	[is_in_polling_io_completion_routine] [bit] NULL,
	[context_switch_count]	[int] NOT NULL,
	[pending_io_count]		[int] NOT NULL,
	[pending_io_byte_count] [bigint] NOT NULL,
	[tasks_processed_count] [int] NOT NULL,
CONSTRAINT [PKdm_os_workers] PRIMARY KEY CLUSTERED 
(
	[UTCCaptureTime] ASC,
	[worker_address] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
