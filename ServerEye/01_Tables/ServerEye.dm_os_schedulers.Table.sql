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

	FILE NAME: ServerEye.dm_os_schedulers.Table.sql

	TABLE NAME: ServerEye.dm_os_schedulers

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Snapshots sys.dm_os_schedulers (in High-frequency metrics)
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [ServerEye].[dm_os_schedulers](
	[UTCCaptureTime]			[datetime] NOT NULL,
	[LocalCaptureTime]			[datetime] NOT NULL,
	[parent_node_id]			[int] NOT NULL,
	[scheduler_id]				[int] NOT NULL,
	[cpu_id]					[int] NOT NULL,
	[status]					[nvarchar](60) NOT NULL,
	[is_online]					[bit] NOT NULL,
	[is_idle]					[bit] NOT NULL,
	[preemptive_switches_count] [int] NOT NULL,
	[context_switches_count]	[int] NOT NULL,
	[idle_switches_count]		[int] NOT NULL,
	[current_tasks_count]		[int] NOT NULL,
	[runnable_tasks_count]		[int] NOT NULL,
	[current_workers_count]		[int] NOT NULL,
	[active_workers_count]		[int] NOT NULL,
	[work_queue_count]			[bigint] NOT NULL,
	[pending_disk_io_count]		[int] NOT NULL,
	[load_factor]				[int] NOT NULL,
	[yield_count]				[int] NOT NULL,
	[last_timer_activity]		[bigint] NOT NULL,
CONSTRAINT [PKdm_os_schedulers] PRIMARY KEY CLUSTERED 
(
	[UTCCaptureTime] ASC,
	[scheduler_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
