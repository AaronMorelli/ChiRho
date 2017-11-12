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

	FILE NAME: ServerEye.dm_db_file_space_usage.Table.sql

	TABLE NAME: ServerEye.dm_db_file_space_usage

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Snapshots dm_db_file_space_usage (in Low-frequency metrics)
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [ServerEye].[dm_db_file_space_usage](
	[UTCCaptureTime] [datetime] NOT NULL,
	[LocalCaptureTime] [datetime] NOT NULL,
	[database_id] [int] NOT NULL,
	[file_id] [smallint] NOT NULL,
	[filegroup_id] [smallint] NULL,
	[total_page_count] [bigint] NULL,
	[allocated_extent_page_count] [bigint] NULL,
	[unallocated_extent_page_count] [bigint] NULL,
	[version_store_reserved_page_count] [bigint] NULL,
	[user_object_reserved_page_count] [bigint] NULL,
	[internal_object_reserved_page_count] [bigint] NULL,
	[mixed_extent_page_count] [bigint] NULL,
 CONSTRAINT [PK_dm_db_file_space_usage] PRIMARY KEY CLUSTERED 
(
	[UTCCaptureTime] ASC,
	[database_id] ASC,
	[file_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO