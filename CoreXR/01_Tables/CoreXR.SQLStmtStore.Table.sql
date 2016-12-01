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
*/
/*
	FILE NAME: CoreXR.SQLStmtStore.Table.sql

	TABLE NAME: CoreXR.SQLStmtStore

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: A centralized store for all statement-level SQL text captured
	by the various components of the ChiRho system.
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [CoreXR].[SQLStmtStore](
	[PKSQLStmtStoreID] [bigint] IDENTITY(1,1) NOT NULL,
	[sql_handle] [varbinary](64) NOT NULL,
	[statement_start_offset] [int] NOT NULL,
	[statement_end_offset] [int] NOT NULL,
	[dbid] [smallint] NOT NULL,
	[objectid] [int] NOT NULL,
	[fail_to_obtain] [bit] NOT NULL,
	[datalen_batch] [int] NOT NULL,
	[stmt_text] [nvarchar](max) NOT NULL,
	[Insertedby_SPIDCaptureTime] [datetime] NOT NULL,
	[LastTouchedBy_SPIDCaptureTime] [datetime] NOT NULL,
 CONSTRAINT [PKSQLStmtStore] PRIMARY KEY CLUSTERED 
(
	[PKSQLStmtStoreID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
SET ANSI_PADDING ON

GO
CREATE NONCLUSTERED INDEX [NCL_LastTouched] ON [CoreXR].[SQLStmtStore]
(
	[LastTouchedBy_SPIDCaptureTime] ASC
)
INCLUDE ( 	[statement_start_offset],
	[statement_end_offset],
	[PKSQLStmtStoreID],
	[sql_handle],
	[dbid],
	[objectid]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_PADDING ON

GO
CREATE UNIQUE NONCLUSTERED INDEX [AKSQLStmtStore] ON [CoreXR].[SQLStmtStore]
(
	[sql_handle] ASC,
	[statement_start_offset] ASC,
	[statement_end_offset] ASC
)
INCLUDE ( 	[PKSQLStmtStoreID],
	[fail_to_obtain],
	[dbid],
	[objectid]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
