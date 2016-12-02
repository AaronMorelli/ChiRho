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
	FILE NAME: AutoWho.Log.Table.sql

	TABLE NAME: AutoWho.Log

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: Holds error and informational log messages from various AutoWho
	procedures
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [AutoWho].[Log](
	[LogDT] [datetime2](7) NOT NULL,
	[TraceID] [int] NULL,
	[ErrorCode] [int] NOT NULL,
	[LocationTag] [nvarchar](50) NOT NULL,
	[LogMessage] [nvarchar](max) NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
CREATE CLUSTERED INDEX [CL_LogDT] ON [AutoWho].[Log]
(
	[LogDT] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
