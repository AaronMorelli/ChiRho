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

	FILE NAME: ServerEye.dm_os_memory_cache_hash_tables.Table.sql

	TABLE NAME: ServerEye.dm_os_memory_cache_hash_tables

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Snapshots sys.dm_os_memory_cache_hash_tables (in Med-frequency metrics)
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [ServerEye].[dm_os_memory_cache_hash_tables](
	[UTCCaptureTime] [datetime] NOT NULL,
	[LocalCaptureTime] [datetime] NOT NULL,
	[DimMemoryTrackerID] [smallint] NOT NULL,
	[memory_node_id] [smallint] NOT NULL,
	[table_level] [int] NOT NULL,
	[sum_buckets_count] [int] NULL,
	[sum_buckets_in_use_count] [int] NULL,
	[min_buckets_min_length] [int] NULL,
	[max_buckets_max_length] [int] NULL,
	[avg_buckets_avg_length] [decimal](11, 2) NULL,
	[max_buckets_max_length_ever] [int] NULL,
	[sum_hits_count] [bigint] NULL,
	[sum_misses_count] [bigint] NULL,
	[avg_buckets_avg_scan_hit_length] [decimal](11, 2) NULL,
	[avg_buckets_avg_scan_miss_length] [decimal](11, 2) NULL,
 CONSTRAINT [PKdm_os_memory_cache_hash_tables] PRIMARY KEY CLUSTERED 
(
	[UTCCaptureTime] ASC,
	[DimMemoryTrackerID] ASC,
	[memory_node_id] ASC,
	[table_level] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


