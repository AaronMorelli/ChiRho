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

	FILE NAME: AutoWho.DimLoginName.Table.sql

	TABLE NAME: AutoWho.DimLoginName

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Holds a distinct list of login names observed by AutoWho.Collector
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [AutoWho].[DimLoginName](
	[DimLoginNameID] [smallint] IDENTITY(30,1) NOT NULL,
	[login_name] [nvarchar](128) NOT NULL,
	[original_login_name] [nvarchar](128) NOT NULL,
	[TimeAdded] [datetime] NOT NULL,
	[TimeAddedUTC] [datetime] NOT NULL,
 CONSTRAINT [PK_AutoWho_DimLoginName] PRIMARY KEY CLUSTERED 
(
	[DimLoginNameID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
CREATE UNIQUE NONCLUSTERED INDEX [AK_AutoWho_DimLoginName] ON [AutoWho].[DimLoginName]
(
	[login_name] ASC,
	[original_login_name] ASC
)
INCLUDE ( 	[DimLoginNameID],
	[TimeAdded],
	[TimeAddedUTC]
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [AutoWho].[DimLoginName] ADD  CONSTRAINT [DF_DimLoginName_TimeAdded]  DEFAULT (GETDATE()) FOR [TimeAdded]
GO
ALTER TABLE [AutoWho].[DimLoginName] ADD  CONSTRAINT [DF_DimLoginName_TimeAddedUTC]  DEFAULT (GETUTCDATE()) FOR [TimeAddedUTC]
GO