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

	FILE NAME: ServerEye.DimWaitType.Table.sql

	TABLE NAME: ServerEye.DimWaitType

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Holds a complete list of wait types from sys.dm_os_waiting_tasks.
	This is different from AutoWho.DimWaitType which only holds waits actually
	observed by the AutoWho.Collector proc. The focus of the AutoWho table is on
	staying as small as possible for speed. ServerEye runs less frequently and
	so a larger table is fine.
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ServerEye].[DimWaitType](
	[DimWaitTypeID] [smallint] IDENTITY(30,1) NOT NULL,
	[wait_type] [nvarchar](60) NOT NULL,
	[isBenign] [bit] NOT NULL,			--we start with a certain set of waits, but user can update to change viewer output
	[TimeAdded] [datetime] NOT NULL,
	[TimeAddedUTC] [datetime] NOT NULL,
 CONSTRAINT [PK_ServerEye_DimWaitType] PRIMARY KEY CLUSTERED 
(
	[DimWaitTypeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
CREATE UNIQUE NONCLUSTERED INDEX [AK_ServerEye_DimWaitType] ON [ServerEye].[DimWaitType]
(
	[wait_type] ASC
)
INCLUDE ( 	
	[DimWaitTypeID],
	[isBenign],
	[TimeAdded],
	[TimeAddedUTC]
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [ServerEye].[DimWaitType] ADD  CONSTRAINT [DF_ServerEye_DimWaitType_TimeAdded]  DEFAULT (GETDATE()) FOR [TimeAdded]
GO
ALTER TABLE [ServerEye].[DimWaitType] ADD  CONSTRAINT [DF_ServerEye_DimWaitType_TimeAddedUTC]  DEFAULT (GETUTCDATE()) FOR [TimeAddedUTC]
GO