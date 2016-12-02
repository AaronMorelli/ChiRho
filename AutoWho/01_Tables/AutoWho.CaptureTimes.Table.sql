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

	FILE NAME: AutoWho.CaptureTimes.Table.sql

	TABLE NAME: AutoWho.CaptureTimes

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Holds 1 row for each successful run of the AutoWho.Collector
	procedure, identifying the time and basic stats of the run.
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [AutoWho].[CaptureTimes](
	[SPIDCaptureTime] [datetime] NOT NULL,
	[UTCCaptureTime] [datetime] NOT NULL,
	[RunWasSuccessful] [tinyint] NOT NULL,
	[CaptureSummaryPopulated] [tinyint] NOT NULL,
	[AutoWhoDuration_ms] [int] NOT NULL,
	[SpidsCaptured] [int] NULL,
	[DurationBreakdown] [varchar](1000) NULL,
 CONSTRAINT [PK_AutoWho_CaptureTimes] PRIMARY KEY CLUSTERED 
(
	[SPIDCaptureTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
CREATE UNIQUE NONCLUSTERED INDEX [NCL_SearchFields] ON [AutoWho].[CaptureTimes]
(
	[RunWasSuccessful] ASC,
	[CaptureSummaryPopulated] ASC,
	[SPIDCaptureTime] ASC
)
INCLUDE ( 	[AutoWhoDuration_ms]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
