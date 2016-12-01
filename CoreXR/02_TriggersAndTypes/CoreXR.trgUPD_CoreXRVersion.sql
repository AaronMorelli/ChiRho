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
	FILE NAME: CoreXR.trgUPD_CoreXRVersion.sql

	TABLE NAME: CoreXR.trgUPD_CoreXRVersion

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: Maintains the CoreXR.Version_History table
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [CoreXR].[trgUPD_CoreXRVersion] ON [CoreXR].[Version]

FOR UPDATE
AS 	BEGIN

INSERT INTO CoreXR.Version_History 
([Version], 
EffectiveDate, 
HistoryInsertDate,
TriggerAction)
SELECT 
[Version], 
EffectiveDate, 
getdate(),
'Update'
FROM inserted
END
GO
