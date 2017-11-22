SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [ServerEye].[CollectorBatchFreqPlanCache]
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

	FILE NAME: ServerEye.CollectorBatchFreqPlanCache.StoredProcedure.sql

	PROCEDURE NAME: CollectorBatchFreqPlanCache.Collector

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Called by the ServerEye.CollectorBatchFreq procedure. Collects statistics about objects (procs and triggers)
	and statements (both ad-hoc and in objects) from the plan cache and, for the most resource-intensive, captures 
	sql text and query plans.

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------

*/
(
	@init				TINYINT,
	@LocalCaptureTime	DATETIME, 
	@UTCCaptureTime		DATETIME,
	@SQLServerStartTime	DATETIME
	--TODO: add @PrevUTCCaptureTime, which holds the previous *successful* batch-freq capture time. This will
	--identify the time when the previous stats were captured.
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	/* Hi-level logic flow

		1. Pull the object (proc/trigger) stats into the raw table. (The contents for the previous pull should exist under the previous @UTCCaptureTime)

			a. I think I'm going to go with the design that the delta metrics are stored in the same table as the raw table.
				Maybe we just call the table by the name of the DMV (or obj for proc/trig)
			
			b. If @PrevUTCCaptureTime is not null, update the delta metrics by comparing the @UTCCaptureTime set with the @PrevUTCCaptureTime set

		2. Pull the query stats into the raw table. (for both obj and ad-hoc entries)

			do the same delta stuff as #1

		3. Pull from the raw query stats table and aggregate by query_hash/cacheobjtype/etc and populate a QueryPatterns table.
			(This is the ServerEye.QueryPatternStats__A table in the POC set). Currently the POC logic excludes objtype IN ('Proc','Trigger')
			and I think that's right, because we are mainly looking for ad-hoc patterns here. We will eventually deal with individual statements
			in the objects, but only after we've identified the "top objects".
			We should rename this table to AdHocQueryPatterns or something.

			a. calc the deltas here like in #1 and #2

		4. Do a pass over the query pattern table to get the total amount of worker time, reads, etc. Having these #'s allows us to calculate
			the % of our "top patterns"

		5. Identify the top patterns in the query patterns table (again, these are all ad-hoc statements) and calculate the % that they count
			toward a given metric (e.g. % total worker time)

		6. Persist the final results of our QueryPattern stats to our actual permanent table

		7. Do a pass over our object table to get the total amount of worker time, reads, etc just as in #4

		8. Rank the objects by their stats, and calculate their % contribution toward the totals calc'd in #7

		9. Persist our final object stats to the perm table

		10. For our top objects, grab the statements from the query stats table (#2)

		11. For each of these statements, calc its % contribution towards the overall object's stats, and grab the SQL text from the stmt store if it exists

		12. For any significant statements (#11) where we weren't able to get its text, grab the text from the real plan cache.

		13. Grab the query plans for all significant statements and resolve them, and compare to the query plan store
			TODO: need to make sure that AutoWho isn't going to overwrite the query plan statement store with a bad/unresolved plan
			when the current plan in the QPSS is good, and vice versa

		14. Finally, persist our object-statement stats and their text/plan pointers to the perm table

		15. For each of our significant ad-hoc query patterns, randomly select X number of statements from the query stats table (#2) 
			to serve as representative statements

		16. Resolve their SQL text and query plan statement

		17. Persist the ad-hoc pattern representative statements into a permanent table.


		NOT YET SURE ABOUT THIS STEP:  x (final) delete everything in the raw tables older than @UTCCaptureTime (which will become the prev capture time)
			May move this out to a PostProcessor-type step in the ChiRho master, and have it delete X thousand at a time in a loop to avoid lock escalation.


		CONSIDER: creating Dim tables for DimPlanObject and DimPlanStatement (or whatever) to reduce the # of columns necessary for these joins,
		and to save some space.
	*/



	RETURN 0;
END
GO