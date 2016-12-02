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

	FILE NAME: AutoWho.trgDEL_AutoWhoOptions.sql

	TRIGGER NAME: AutoWho.trgDEL_AutoWhoOptions

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: redirects attempts to delete data from the Options table into a reset
	of the values. (An accidental delete can be recovered using the history table).
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [AutoWho].[trgDEL_AutoWhoOptions] ON [AutoWho].[Options]

FOR DELETE
AS 	BEGIN

--We don't actually allow deletes. So call the reset procedure
RAISERROR('Attempts to Delete data in the Options table instead cause the Options table to be reset.',10,1);
ROLLBACK TRANSACTION;

EXEC AutoWho.ResetOptions;

RETURN;

END
GO
