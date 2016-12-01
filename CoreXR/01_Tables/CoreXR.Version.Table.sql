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
	FILE NAME: CoreXR.Version.Table.sql

	TABLE NAME: CoreXR.Version

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: One-row table with the current version of the ChiRho system
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [CoreXR].[Version](
	[Version] [nvarchar](30) NOT NULL,
	[EffectiveDate] [datetime] NOT NULL
) ON [PRIMARY]

GO
ALTER TABLE [CoreXR].[Version] ADD  CONSTRAINT [DF_Version_InsertedDate]  DEFAULT (getdate()) FOR [EffectiveDate]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
