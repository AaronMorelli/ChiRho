/****** Object:  Table [ServerEye].[dm_exec_query_stats__A]    Script Date: 11/22/2017 1:01:31 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING OFF
GO

CREATE TABLE [ServerEye].[dm_exec_query_stats__A](
	[sql_handle] [varbinary](64) NOT NULL,
	[statement_start_offset] [int] NOT NULL,
	[statement_end_offset] [int] NOT NULL,
	[plan_generation_num] [bigint] NULL
) ON [PRIMARY]
SET ANSI_PADDING ON
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [plan_handle] [varbinary](64) NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [creation_time] [datetime] NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [last_execution_time] [datetime] NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [execution_count] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [total_worker_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [last_worker_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [min_worker_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [max_worker_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [total_physical_reads] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [last_physical_reads] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [min_physical_reads] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [max_physical_reads] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [total_logical_writes] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [last_logical_writes] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [min_logical_writes] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [max_logical_writes] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [total_logical_reads] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [last_logical_reads] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [min_logical_reads] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [max_logical_reads] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [total_clr_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [last_clr_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [min_clr_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [max_clr_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [total_elapsed_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [last_elapsed_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [min_elapsed_time] [bigint] NOT NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [max_elapsed_time] [bigint] NOT NULL
SET ANSI_PADDING OFF
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [query_hash] [binary](8) NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [query_plan_hash] [binary](8) NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [total_rows] [bigint] NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [last_rows] [bigint] NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [min_rows] [bigint] NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [max_rows] [bigint] NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [bucketid] [int] NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [refcounts] [int] NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [usecounts] [int] NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [size_in_bytes] [int] NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [cacheobjtype] [nvarchar](50) NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [objtype] [nvarchar](20) NULL
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [pool_id] [int] NULL
SET ANSI_PADDING ON
ALTER TABLE [ServerEye].[dm_exec_query_stats__A] ADD [parent_plan_handle] [varbinary](64) NULL

GO

SET ANSI_PADDING OFF
GO


