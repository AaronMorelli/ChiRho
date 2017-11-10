SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [ServerEye].[PrePopulateDimensions]
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

	FILE NAME: ServerEye.PrePopulateDimensions.StoredProcedure.sql

	PROCEDURE NAME: ServerEye.PrePopulateDimensions

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Adds new rows (if any are found) to various dimension tables so that those values are present when
		the various ServerEye collection queries run.

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
EXEC ServerEye.PrePopulateDimensions

*/
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE
		@lv__nullstring						NVARCHAR(8),
		@lv__nullint						INT,
		@lv__nullsmallint					SMALLINT;

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" 
											-- would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value

	IF OBJECT_ID('tempdb..#DistinctWaitTypes1') IS NOT NULL
	BEGIN
		DROP TABLE #DistinctWaitTypes1;
	END
	SELECT w.wait_type
	INTO #DistinctWaitTypes1
	FROM sys.dm_os_wait_stats w;

	INSERT INTO ServerEye.DimWaitType
		(wait_type, TimeAdded, TimeAddedUTC)
	SELECT DISTINCT w.wait_type, GETDATE(), GETUTCDATE()
	FROM #DistinctWaitTypes1 w
	WHERE NOT EXISTS (
		SELECT * FROM ServerEye.DimWaitType dwt
		WHERE dwt.wait_type = w.wait_type
	);

	RETURN 0;
END
GO
