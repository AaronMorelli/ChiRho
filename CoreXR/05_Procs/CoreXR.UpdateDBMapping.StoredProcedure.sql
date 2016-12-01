SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [CoreXR].[UpdateDBMapping] 
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

	FILE NAME: CoreXR.UpdateDBMapping.StoredProcedure.sql

	PROCEDURE NAME: CoreXR.UpdateDBMapping

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: Since our app stored historical data, and DBs are sometimes detached/re-attached, etc,
	 we want to keep a mapping between DBID and DBName. (Much of our storage just keeps DBID rather than DBName).
	 We make the (usually-safe, but not always) assumption that 2 DBs with the same name are really the same database.

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
EXEC CoreXR.UpdateDBMapping
*/
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @EffectiveTime DATETIME = GETDATE();

	CREATE TABLE #CurrentDBIDNameMapping (
		DBID int not null, 
		DBName nvarchar(256) not null
	);

	INSERT INTO #CurrentDBIDNameMapping (
		DBID, DBName
	)
	SELECT d.database_id, d.name
	FROM sys.databases d
	;

	-- In the below joins, we typically connect the contents of #DBIDchanges to the "current" set of rows in 
	-- CoreXR.DBIDNameMapping, i.e. where EffectiveEndTime is null

	-- First, find matches on DBName, where the DBID is different.
	--		a. first, close out the our row (EffectiveEndTime = GETDATE()
	--		b. second, insert the new pair in. Note that this also takes care of completely new DBName values.

	UPDATE targ 
	SET EffectiveEndTime = @EffectiveTime
	FROM CoreXR.DBIDNameMapping targ 
		INNER JOIN #CurrentDBIDNameMapping t
			ON t.DBName = targ.DBName
			AND t.DBID <> targ.DBID
	WHERE targ.EffectiveEndTime IS NULL
	;

	INSERT INTO CoreXR.DBIDNameMapping
	(DBID, DBName, EffectiveStartTime, EffectiveEndTime)
	SELECT t.DBID, t.DBName, @EffectiveTime, NULL 
	FROM #CurrentDBIDNameMapping t
	WHERE NOT EXISTS (
		SELECT * 
		FROM CoreXR.DBIDNameMapping m
		WHERE m.DBName = t.DBName
		AND m.EffectiveEndTime IS NULL 
	);

	UPDATE targ 
	SET EffectiveEndTime = @EffectiveTime
	FROM CoreXR.DBIDNameMapping targ 
	WHERE targ.EffectiveEndTime IS NULL
	AND NOT EXISTS (
		SELECT * FROM #CurrentDBIDNameMapping t
		WHERE t.DBName = targ.DBName
	)
	;

	RETURN 0;
END
GO
