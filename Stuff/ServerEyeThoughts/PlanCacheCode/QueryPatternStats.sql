/****** Object:  Table [ServerEye].[QueryPatternStats]    Script Date: 11/22/2017 1:02:23 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [ServerEye].[QueryPatternStats](
	[CollectionTime] [datetime] NOT NULL,
	[cacheobjtype] [nvarchar](50) NOT NULL,
	[objtype] [nvarchar](20) NOT NULL,
	[query_hash] [binary](8) NOT NULL,
	[NumEntries] [int] NOT NULL,
	[NumEntries_delta] [int] NOT NULL,
	[size_in_bytes] [bigint] NOT NULL,
	[size_in_bytes_delta] [bigint] NOT NULL,
	[total_rows] [bigint] NULL,
	[total_rows_delta] [bigint] NULL,
	[plan_generation_num] [bigint] NULL,
	[plan_generation_num_delta] [bigint] NULL,
	[refcounts] [bigint] NULL,
	[usecounts] [bigint] NULL,
	[usecounts_delta] [bigint] NULL,
	[execution_count] [bigint] NOT NULL,
	[execution_count_delta] [bigint] NOT NULL,
	[total_worker_time] [bigint] NOT NULL,
	[total_worker_time_delta] [bigint] NOT NULL,
	[total_physical_reads] [bigint] NOT NULL,
	[total_physical_reads_delta] [bigint] NOT NULL,
	[total_logical_writes] [bigint] NOT NULL,
	[total_logical_writes_delta] [bigint] NOT NULL,
	[total_logical_reads] [bigint] NOT NULL,
	[total_logical_reads_delta] [bigint] NOT NULL,
	[total_clr_time] [bigint] NOT NULL,
	[total_clr_time_delta] [bigint] NOT NULL,
	[total_elapsed_time] [bigint] NOT NULL,
	[total_elapsed_time_delta] [bigint] NOT NULL,
	[rank_total_rows] [int] NULL,
	[rank_worker_time] [int] NULL,
	[pctall_worker_time] [decimal](5, 2) NULL,
	[rank_physical_reads] [int] NULL,
	[pctall_physical_reads] [decimal](5, 2) NULL,
	[rank_logical_writes] [int] NULL,
	[pctall_logical_writes] [decimal](5, 2) NULL,
	[rank_logical_reads] [int] NULL,
	[pctall_logical_reads] [decimal](5, 2) NULL,
	[rank_elapsed_time] [int] NULL,
	[pctall_elapsed_time] [decimal](5, 2) NULL,
	[rank_NumEntries] [int] NULL,
	[pctall_NumEntries] [decimal](5, 2) NULL,
 CONSTRAINT [PK_QueryPatternStats] PRIMARY KEY CLUSTERED 
(
	[CollectionTime] ASC,
	[cacheobjtype] ASC,
	[objtype] ASC,
	[query_hash] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


