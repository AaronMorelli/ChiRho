/****** Object:  Table [ServerEye].[QueryPatterns_RepresentativeStmts]    Script Date: 11/22/2017 1:02:19 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [ServerEye].[QueryPatterns_RepresentativeStmts](
	[CollectionTime] [datetime] NOT NULL,
	[cacheobjtype] [nvarchar](50) NOT NULL,
	[objtype] [nvarchar](20) NOT NULL,
	[query_hash] [binary](8) NOT NULL,
	[sql_handle] [varbinary](64) NULL,
	[statement_start_offset] [int] NOT NULL,
	[statement_end_offset] [int] NOT NULL,
	[plan_handle] [varbinary](64) NULL,
	[plan_generation_num] [bigint] NULL,
	[plan_generation_num_delta] [bigint] NULL,
	[creation_time] [datetime] NULL,
	[last_execution_time] [datetime] NULL,
	[execution_count] [bigint] NOT NULL,
	[execution_count_delta] [bigint] NOT NULL,
	[total_worker_time] [bigint] NOT NULL,
	[total_worker_time_delta] [bigint] NOT NULL,
	[last_worker_time] [bigint] NOT NULL,
	[min_worker_time] [bigint] NOT NULL,
	[max_worker_time] [bigint] NOT NULL,
	[total_physical_reads] [bigint] NOT NULL,
	[total_physical_reads_delta] [bigint] NOT NULL,
	[last_physical_reads] [bigint] NOT NULL,
	[min_physical_reads] [bigint] NOT NULL,
	[max_physical_reads] [bigint] NOT NULL,
	[total_logical_writes] [bigint] NOT NULL,
	[total_logical_writes_delta] [bigint] NOT NULL,
	[last_logical_writes] [bigint] NOT NULL,
	[min_logical_writes] [bigint] NOT NULL,
	[max_logical_writes] [bigint] NOT NULL,
	[total_logical_reads] [bigint] NOT NULL,
	[total_logical_reads_delta] [bigint] NOT NULL,
	[last_logical_reads] [bigint] NOT NULL,
	[min_logical_reads] [bigint] NOT NULL,
	[max_logical_reads] [bigint] NOT NULL,
	[total_clr_time] [bigint] NOT NULL,
	[total_clr_time_delta] [bigint] NOT NULL,
	[last_clr_time] [bigint] NOT NULL,
	[min_clr_time] [bigint] NOT NULL,
	[max_clr_time] [bigint] NOT NULL,
	[total_elapsed_time] [bigint] NOT NULL,
	[total_elapsed_time_delta] [bigint] NOT NULL,
	[last_elapsed_time] [bigint] NOT NULL,
	[min_elapsed_time] [bigint] NOT NULL,
	[max_elapsed_time] [bigint] NOT NULL,
	[query_plan_hash] [binary](8) NULL,
	[total_rows] [bigint] NULL,
	[total_rows_delta] [bigint] NULL,
	[last_rows] [bigint] NULL,
	[min_rows] [bigint] NULL,
	[max_rows] [bigint] NULL,
	[refcounts] [int] NULL,
	[usecounts] [int] NULL,
	[size_in_bytes] [int] NULL,
	[pool_id] [int] NULL,
	[parent_plan_handle] [varbinary](64) NULL,
	[FKSQLStmtStoreID] [bigint] NULL,
	[FKQueryPlanStmtStoreID] [bigint] NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

SET ANSI_PADDING ON

GO

/****** Object:  Index [CL_CollectionTime_cacheobjtype_objtype_hash]    Script Date: 11/22/2017 1:02:19 PM ******/
CREATE CLUSTERED INDEX [CL_CollectionTime_cacheobjtype_objtype_hash] ON [ServerEye].[QueryPatterns_RepresentativeStmts]
(
	[CollectionTime] ASC,
	[cacheobjtype] ASC,
	[objtype] ASC,
	[query_hash] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO


