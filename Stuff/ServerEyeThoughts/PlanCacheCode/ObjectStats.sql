/****** Object:  Table [ServerEye].[ObjectStats]    Script Date: 11/22/2017 1:02:06 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [ServerEye].[ObjectStats](
	[CollectionTime] [datetime] NOT NULL,
	[database_id] [int] NOT NULL,
	[object_id] [int] NOT NULL,
	[type] [char](2) NULL,
	[sql_handle] [varbinary](64) NULL,
	[plan_handle] [varbinary](64) NULL,
	[cached_time] [datetime] NULL,
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
	[total_elapsed_time] [bigint] NOT NULL,
	[total_elapsed_time_delta] [bigint] NOT NULL,
	[last_elapsed_time] [bigint] NOT NULL,
	[min_elapsed_time] [bigint] NOT NULL,
	[max_elapsed_time] [bigint] NOT NULL,
	[rank_worker_time] [int] NULL,
	[pctallplans_worker_time] [decimal](5, 2) NULL,
	[rank_physical_reads] [int] NULL,
	[pctallplans_physical_reads] [decimal](5, 2) NULL,
	[rank_logical_writes] [int] NULL,
	[pctallplans_logical_writes] [decimal](5, 2) NULL,
	[rank_logical_reads] [int] NULL,
	[pctallplans_logical_reads] [decimal](5, 2) NULL,
	[rank_elapsed_time] [int] NULL,
	[pctallplans_elapsed_time] [decimal](5, 2) NULL,
 CONSTRAINT [PK_ObjectStats] PRIMARY KEY CLUSTERED 
(
	[CollectionTime] ASC,
	[database_id] ASC,
	[object_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


