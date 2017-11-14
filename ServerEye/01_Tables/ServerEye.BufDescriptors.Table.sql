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

	FILE NAME: ServerEye.BufDescriptors.Table.sql

	TABLE NAME: ServerEye.BufDescriptors

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Aggregates data from sys.dm_os_buffer_descriptors to track memory usage. Called in Batch Freq collector
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [ServerEye].[BufDescriptors](
	[UTCCaptureTime]		[datetime] NOT NULL,
	[database_id]			[int] NOT NULL,
	[file_id]				[int] NOT NULL,
	[allocation_unit_id]	[bigint] NOT NULL,
	[page_type]				[nvarchar](60) NOT NULL,
	[numa_node]				[int] NOT NULL,
	[NumModified]			[int] NULL,
	[SumRowCount]			[bigint] NULL,
	[SumFreeSpaceInBytes]	[bigint] NULL,
	[NumRows]				[int] NULL,
CONSTRAINT [PKBufDescriptors] PRIMARY KEY CLUSTERED 
(
	[UTCCaptureTime] ASC,
	[database_id] ASC,
	[file_id] ASC,
	[allocation_unit_id] ASC,
	[page_type] ASC,
	[numa_node] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


