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

	FILE NAME: ServerEye.dm_os_sys_info_volatile.Table.sql

	TABLE NAME: ServerEye.dm_os_sys_info_volatile

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Holds data from dm_os_sys_info that is likely to change quite often.
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [ServerEye].[dm_os_sys_info_volatile](
	[UTCCaptureTime] [datetime] NOT NULL,
	[LocalCaptureTime] [datetime] NOT NULL,
	[StableOSIID] [int] NOT NULL,
	[cpu_ticks] [bigint] NOT NULL,
	[ms_ticks] [bigint] NOT NULL,
	[committed_kb] [int] NULL,
	[bpool_committed] [int] NULL,
	[committed_target_kb] [int] NULL,
	[bpool_commit_target] [int] NULL,
	[visible_target_kb] [int] NULL,
	[bpool_visible] [int] NULL,
	[process_kernel_time_ms] [bigint] NULL,
	[process_user_time_ms] [bigint] NULL,
 CONSTRAINT [PK_os_sys_info_volatile] PRIMARY KEY CLUSTERED 
(
	[UTCCaptureTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


