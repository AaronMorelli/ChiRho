/****** Object:  Table [ServerEye].[QueryPatternStats__B]    Script Date: 11/22/2017 1:02:31 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [ServerEye].[QueryPatternStats__B](
	[cacheobjtype] [nvarchar](50) NOT NULL,
	[objtype] [nvarchar](20) NOT NULL,
	[query_hash] [binary](8) NOT NULL,
	[NumEntries] [int] NOT NULL,
	[size_in_bytes] [int] NOT NULL,
	[total_rows] [bigint] NULL,
	[plan_generation_num] [bigint] NULL,
	[refcounts] [bigint] NULL,
	[usecounts] [bigint] NULL,
	[execution_count] [bigint] NOT NULL,
	[total_worker_time] [bigint] NOT NULL,
	[total_physical_reads] [bigint] NOT NULL,
	[total_logical_writes] [bigint] NOT NULL,
	[total_logical_reads] [bigint] NOT NULL,
	[total_clr_time] [bigint] NOT NULL,
	[total_elapsed_time] [bigint] NOT NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


