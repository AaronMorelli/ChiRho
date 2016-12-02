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

	FILE NAME: CoreXR.CaptureOrdinalCache.Table.sql

	TABLE NAME: CoreXR.CaptureOrdinalCache

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Holds a list of capture times (e.g. for captures by the AutoWho or ServerEye components)
	and each capture time's order number (both ascending and descending) within the overall range. The
	front-end UI procs populate this table when requested by a user call, for a given start/end range,
	and then refer to it as they iterate over the capture times.
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [CoreXR].[CaptureOrdinalCache](
	[Utility] [nvarchar](30) NOT NULL,
	[CollectionInitiatorID] [tinyint] NOT NULL,
	[StartTime] [datetime] NOT NULL,
	[EndTime] [datetime] NOT NULL,
	[Ordinal] [int] NOT NULL,
	[OrdinalNegative] [int] NOT NULL,
	[CaptureTime] [datetime] NOT NULL,
	[TimePopulated] [datetime] NOT NULL,
 CONSTRAINT [PKCaptureOrdinalCache] PRIMARY KEY CLUSTERED 
(
	[Utility] ASC,
	[CollectionInitiatorID] ASC,
	[StartTime] ASC,
	[EndTime] ASC,
	[Ordinal] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE UNIQUE NONCLUSTERED INDEX [UNCL_OrdinalNegative] ON [CoreXR].[CaptureOrdinalCache]
(
	--TODO: doesn't this need both the Utility and initiator ID fields?
	[StartTime] ASC,
	[EndTime] ASC,
	[OrdinalNegative] ASC
)
INCLUDE ( 	[Ordinal],
	[CaptureTime],
	[TimePopulated]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, 
		IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [CoreXR].[CaptureOrdinalCache] ADD  CONSTRAINT [DF_CoreXR_CaptureOrdinalCache_TimePopulated]  DEFAULT (getdate()) FOR [TimePopulated]
GO
