USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_XR_LongRequests] 
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

	FILE NAME: sp_XR_LongRequests__TEMPLATE.sql

	PROCEDURE NAME: sp_XR_LongRequests

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Returns a listing of requests (unique session_id/request_id/request_start_time) whose duration
	(as observed at a given point in time by AutoWho collections) is > than a certain threshold. This can be
	used to find long-running requests in a longer time window, or to compare a given batch process between
	a "good" run and a "bad" run.

To Execute
------------------------
exec sp_XR_LongRequests @start='2016-05-17 04:00', @end='2016-05-17 06:00', @savespace=N'N'

*/
(
	@start			DATETIME=NULL,			--the start of the time window. If NULL, defaults to 4 hours ago.
	@end			DATETIME=NULL,			-- the end of the time window. If NULL, defaults to 1 second ago.
	@source			NVARCHAR(20)=N'trace',		--'trace' = standard AutoWho.Executor background trace; 
												-- 'pastSV' reviews data from past sp_XR_SessionViewer calls done in "current" mode
												-- 'pastQP' reviews data from past sp_XR_QueryProgress calls done in "current" or "time series" mode.
												-- This param is ignored if this invocation is "current" mode (i.e. start/end are null)
	@mindur			INT=120,				-- in seconds. Only batch requests with at least one entry in SAR that is >= this val will be included
	@dbs			NVARCHAR(512)=N'',		--list of DB names to include
	@xdbs			NVARCHAR(512)=N'',		--list of DB names to exclude
	@spids			NVARCHAR(128)=N'',		--comma-separated list of session_ids to include
	@xspids			NVARCHAR(128)=N'',		--comma-separated list of session_ids to exclude
	@attr			NCHAR(1)=N'N',			--Whether to include the session/connection attributes for the request's first entry in sar (in the time range)
	@plan			NVARCHAR(20)=N'none',		--none / statement		whether to include the query plan for each statement
	@help			NVARCHAR(10)=N'N'		-- "params", "columns", or "all" (anything else <> "N" maps to "all")
)
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SET ANSI_PADDING ON;

	DECLARE @scratch__int				INT,
			@helpexec					NVARCHAR(4000),
			@err__msg					NVARCHAR(MAX),
			@DynSQL						NVARCHAR(MAX),
			@helpstr					NVARCHAR(MAX),
			@lv__CollectionInitiatorID	TINYINT,
			@lv__qplan					NCHAR(1)
			;

	--We always print out the exec syntax (whether help was requested or not) so that the user can switch over to the Messages
	-- tab and see what their options are.
	SET @helpexec = N'
exec sp_XR_LongRequests @start=''<start datetime>'', @end=''<end datetime>'', @mindur=120, 
	@source=N''trace'',		-- trace, pastSV, pastQP
	@dbs=N'''', @xdbs=N'''', @spids=N'''', @xspids=N'''', 
	@attr=N''N'', @plan=N''none'',			--none, statement
	@help = N''N''							--params, columns, all
	';

	IF @help IS NULL
	BEGIN
		SET @help = N'ALL';
	END
	ELSE
	BEGIN
		SET @help = UPPER(@help);
	END

	IF @help <> N'N'
	BEGIN
		GOTO helpbasic
	END

	IF @start IS NULL
	BEGIN
		SET @start = DATEADD(HOUR, -4, GETDATE());
		RAISERROR('Parameter @start set to 4 hours ago because a NULL value was supplied.', 10, 1) WITH NOWAIT;
	END

	SET @helpexec = REPLACE(@helpexec,'<start datetime>', REPLACE(CONVERT(NVARCHAR(20), @start, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @start, 108) + '.' + 
													RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @start)),3)
					);

	IF @end IS NULL
	BEGIN
		SET @end = DATEADD(SECOND,-1, GETDATE());
		RAISERROR('Parameter @end set to 1 second ago because a NULL value was supplied.',10,1) WITH NOWAIT;
	END

	SET @helpexec = REPLACE(@helpexec,'<end datetime>', REPLACE(CONVERT(NVARCHAR(20), @end, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @end, 108) + '.' + 
														RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @end)),3)
						);

	IF @start > GETDATE() OR @end > GETDATE()
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Neither of the parameters @start or @end can be in the future.',16,1);
		RETURN -1;
	END
	
	IF @end <= @start
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @end cannot be <= to parameter @start', 16, 1);
		RETURN -1;
	END

	IF LOWER(ISNULL(@source,N'z')) NOT IN (N'trace', N'pastsv', N'pastqp')
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @source must be either "trace" (historical data from standard AutoWho trace), "pastSV" (data from past sp_XR_SessionViewer executions), or "pastQP" (data from past sp_XR_QueryProgress executions).',16,1);
		RETURN -1;
	END
	ELSE
	BEGIN
		IF @source = N'trace'
		BEGIN
			SET @lv__CollectionInitiatorID = 255;	--use the standard historical data collected by AutoWho.Executor
		END
		ELSE IF @source = N'pastSV'
		BEGIN
			SET @lv__CollectionInitiatorID = 1;		--use the data collected by past calls to sp_XR_SessionViewer
		END
		ELSE IF @source = N'pastQP'
		BEGIN
			SET @lv__CollectionInitiatorID = 2;		--use the data collected by past calls to sp_XR_QueryProgress
		END
	END

	IF ISNULL(@mindur, -1) < 0
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @mindur must be an integer >= 0.', 16, 1);
		RETURN -1;
	END

	IF UPPER(ISNULL(@attr,N'z')) NOT IN (N'N', N'Y')
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @attr must be either N''N'' or N''Y''.', 16, 1);
		RETURN -1;
	END

	IF LOWER(ISNULL(@plan,N'z')) NOT IN (N'none', N'statement')
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @plan must be either N''none'' or N''statement''.', 16, 1);
		RETURN -1;
	END
	ELSE
	BEGIN
		IF LOWER(@plan) = N'none'
		BEGIN
			SET @lv__qplan = N'N';
		END
		ELSE
		BEGIN
			SET @lv__qplan = N'Y';
		END
	END

	--We don't validate the 4 CSV filtering variables, outside of checking for NULL. We leave that to the sub-procs
	IF @dbs IS NULL
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @dbs cannot be NULL; it should either be an empty string or a comma-delimited string of database names.',16,1);
		RETURN -1;
	END

	IF @xdbs IS NULL
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @xdbs cannot be NULL; it should either be an empty string or a comma-delimited string of database names.',16,1);
		RETURN -1;
	END

	IF @spids IS NULL
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @spids cannot be NULL; it should either be an empty string or a comma-delimited string of session IDs.',16,1);
		RETURN -1;
	END

	IF @xspids IS NULL
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @xspids cannot be NULL; it should either be an empty string or a comma-delimited string of session IDs.',16,1);
		RETURN -1;
	END

	EXEC @@XRDATABASENAME@@.AutoWho.ViewLongRequests @init = @lv__CollectionInitiatorID,
			@start = @start, 
			@end = @end, 
			@mindur = @mindur,
			@spids = @spids, 
			@xspids = @xspids,
			@dbs = @dbs, 
			@xdbs = @xdbs,
			@attr = @attr,
			@plan = @lv__qplan
	;

	--we always print out at least the EXEC command
	GOTO helpbasic



helpbasic:

	IF @help <> N'N'
	BEGIN
		IF @help LIKE N'P%'
		BEGIN
			SET @help = N'PARAMS'
		END

		IF @help LIKE N'C%'
		BEGIN
			SET @help = N'COLUMNS'
		END

		IF @help NOT IN (N'PARAMS', N'COLUMNS', N'ALL')
		BEGIN
			--user may have typed gibberish... which is ok, give him/her all the help
			SET @Help = N'ALL'
		END
	END

	SET @helpstr = @helpexec;
	RAISERROR(@helpstr,10,1) WITH NOWAIT;
	
	IF @Help = N'N'
	BEGIN
		--because the user may want to use sp_XR_SessionViewer and/or sp_XR_QueryProgress next, if they haven't asked for help explicitly, we print out the syntax for 
		--the Session Viewer and Query Progress procedures
		SET @helpstr = N'
EXEC dbo.sp_XR_SessionViewer @start=''<start datetime>'',@end=''<end datetime>'', --@offset=99999,	--99999
	@source=N''trace'',		-- trace, pastSV, pastQP
	@camrate=0, @camstop=60,
	@activity=1,@dur=0, @dbs=N'''',@xdbs=N'''', @spids=N'''',@xspids=N'''', @blockonly=N''N'',
	@attr=N''N'',@resources=N''N'',@batch=N''N'',@plan=N''none'',	--none, statement, full
	@ibuf=N''N'',@bchain=0,@tran=N''N'',@waits=0,		--bchain 0-10, waits 0-3
	@savespace=N''N'',@directives=N'''', @help=N''N''		--"query(ies)"
	';

		RAISERROR(@helpstr,10,1);

--		SET @helpstr = '
--EXEC dbo.sp_XR_QueryProgress @start=''<start datetime>'',@end=''<end datetime>'', --@offset=99999,	--99999
--						@spid=<int>, @request=0, @nodeassociate=N''N'',
--						@help=N''N''		--"query(ies)"
--		';
		--TODO: once QP is ready, include it in the output here

		RETURN 0;
	END

	SET @helpstr = N'
sp_XR_LongRequests version 2008R2.1

Key Concepts and Terminology
-------------------------------------------------------------------------------------------------------------------------------------------
sp_XR_LongRequests displays from AutoWho, a subcomponent of the ChiRho toolkit that collects data from the session-centric 
DMVs and stores that data in AutoWho tables. sp_XR_LongRequests focuses on displaying longer-running requests with information
aggregated to the statement level within each request. This proc has cousins: sp_XR_SessionSummary, which shows just 1 row per 
AutoWho capture and aggregated info about the DMV data at that capture time, and sp_XR_SessionViewer, all actively-running queries 
and idle SPIDs (depending on filtering criteria) for a given moment in time. Forthcoming is sp_XR_QueryProgress, which allows the 
tracking of an individual query including detailed info about parallel waits. 

The term "AutoWho" needs clarification: it is the subcomponent of the ChiRho toolkit focused on session-based DMV data,
and is a set of procs and tables that collect and store this data. On a typical install of ChiRho, the AutoWho code typically
executes in the context of a background trace, polling every 15 seconds. However, the same AutoWho collection code can be run by
sp_XR_SessionViewer whenever it is running in "current mode", and likewise for sp_XR_QueryProgress in its own current mode.
Regardless of which method is used to collect AutoWho data, it is always stored in AutoWho tables. Thus, even a "current mode"
run of sp_XR_SessionViewer or sp_XR_QueryProgress stores data from the DMVs into AutoWho tables before displaying to the user.
A tag in the AutoWho tables is used to differentiate which method of collection was used for each capture, essentially partitioning
(logically) the data into different "sets". The @source parameter allows the user to target these different sets.

sp_XR_LongRequests examines a time window (often measured in hours) and returns all requests that have been observed to
run longer than @mindur seconds between the @start and @end times. This proc can be used to compare between a good
and a bad execution of a longer batch process, or to identify changes in resource utilization between runs, or determine
the wait types common to a given longer-running statement.

Each row in the result set can either be a header row, representing a request, or a detail row representing a statement.
(The same statement can span rows in some cases, see notes under the @plan attribute). A request in SQL Server is the
unique combination of dm_exec_requests.session_id, dm_exec_requests.request_id, and dm_exec_requests.start_time. A request
(also known as a "batch" in some contexts) can involve one or more statements. 

More info on the result set structure is available in the "columns" help section.
	';
	RAISERROR(@helpstr,10,1);

	IF @Help NOT IN (N'params',N'all')
	BEGIN
		GOTO helpcolumns
	END

helpparams:
	SET @helpstr = N'
Parameters
-------------------------------------------------------------------------------------------------------------------------------------------
@start			Valid Values: NULL, any datetime value in the past

				Defines the start time of the time window/range used to pull & display request-related data from the ChiRho database. The 
				time cannot be in the future, and must be < @end. If NULL is passed, the time defaults to 4 hours before the current time 
				[ DATEADD(hour, -4, GETDATE()) ]. If a given request started executing before @start, only its data from @start onward 
				will be included.
	
@end			Valid Values: NULL, any datetime in the past

				Defines the end time of the time window/range used. The time cannot be in the future, and must be > @start. If NULL is 
				passed, the time defaults to 1 second before the current time [ DATEADD(second, -1, GETDATE()) ]. If a given request 
				continued executing after @end, only its data until @end will be included.

@source			Valid Values: "trace" (default), "pastSV", "pastQP" (all case-insensitive)

				Specifies which subset of data in AutoWho tables to review. AutoWho data is collected through one of three ways: 
				the standard background trace ("trace") which usually executes every 15 seconds all day long; through the 
				sp_XR_SessionViewer procedure ("pastSV") when run with null @start and @end parameters; through the sp_XR_QueryProgress 
				procedure ("pastQP") also when run with null @start/@end. Internally, this data is partitioned based on how the 
				collection in initiated, and this @source parameter allows the user to direct which collection type is reviewed. 
				Most of the time, the background trace data is desired and thus "trace" is appropriate.';
	RAISERROR(@helpstr,10,1);


	SET @helpstr = N'
@mindur			Valid Values: Zero or any positive integer, in seconds (defaults to 120)

				This procedure presents data on requests (a unique session_id, request_id, and dm_exec_requests.start_time). This 
				parameter sets the threshold at which a given request is considered to have executed for a "long" time. Thus, by 
				default, if AutoWho has observed a given request execution duration to be >= 120 seconds for AutoWho collections 
				occurring between @start and @end, that request is considered "long" and all data collected by AutoWho between @start 
				and @end will be taken into account by this procedure.

@dbs			Valid Values: comma-separated list of database names to be included. Defaults to empty string.

				Filters the output to include only requests whose context database ID (from dbo.sysprocesses) is equal to the DBIDs 
				that correspond to the DB names in this list. If a request has multiple context DBIDs, its first one (in the @start/@end 
				range) is the defining context DBID for the complete request.

@xdbs			Valid Values: comma-separated list of database names to be excluded. Defaults to empty string.

				Filters the output to exclude requests whose initial context DBID (see above note in" @dbs" for how this is defined) 
				matches one of the DBs specified.

@spids			Valid Values: comma-separated list of session IDs to be included. Defaults to empty string.

				Filters the output to include only requests whose SPIDs are in this list. Note that there is a bit of a mismatch, in 
				that the output data is at the request level, but filtering is at the session level. It is currently considered unlikely 
				that a user would want to include requests from one session and exclude other requests from that same session.

@xspids			Valid values: comma-separated list of session IDs to be excluded. Defaults to empty string.

				Filters the output to remove requests whose SPIDs are in this list. See above note (in "@spids") about requests
				versus sessions.';
	RAISERROR(@helpstr,10,1);

	SET @helpstr = N'
@attr			Valid values: Y or N (default)

				If Y, a new column named "Plan&Info" is added to the result set. (This column is also added to the output when the 
				@plan parameter is set to "statement".) For header-level rows, this column contains various attributes from the 
				dm_exec_sessions, dm_exec_connections, and dm_exec_requests views.

@plan			Valid values: "none" or "statement"

				If Y, a new column named "Plan&Info" is added to the result set. (This column is also added to the output when the 
				@attr parameter is set to Y.) For detail-level rows, this column contains the query plan XML for the detail-level row''s 
				statement. 

				NOTE: Because the initial AutoWho capture(s) of a running request can omit the collection of the query plan (depending 
				on the values of the "QueryPlanThreshold" and "QueryPlanThresholdBlockRel" AutoWho options), adding in the plan information 
				can change the granularity of the output. For example, the first observation of a request, when its duration is 2.5 
				seconds, may omit capturing the query plan for that request, while the next observation 15 seconds later will capture it
				(under default config values). Because the output is grouped by both statement and plan identifiers, there will appear to 
				be 2 separate statements (output rows) with the same statement text. This situation does not occur when @plan="N" b/c the 
				plan IDs are set to an irrelevant constant.';
	RAISERROR(@helpstr,10,1);

	SET @helpstr = N'
@help			Valid Values: N, params, columns, all, or even gibberish

				If @Help=N''N'', then no help is printed. If =''params'', this section of Help is printed. If =''columns'', the section 
				on result columns is prented. If @Help is passed anything else (even gibberish), it is set to ''all'', and all help 
				content is printed.
	';

	RAISERROR(@helpstr,10,1);

	IF @Help = N'params'
	BEGIN
		GOTO exitloc
	END
	ELSE
	BEGIN
		SET @helpstr = N'
		';
		RAISERROR(@helpstr,10,1);
	END


helpcolumns:

	SET @helpstr = N'
Columns
-------------------------------------------------------------------------------------------------------------------------------------------
SPID			The session ID of the request. Rows for a given request are grouped together, but the overall list of requests is 
				ordered by request start time, then by session id, then by request_id. To simplify the output and aid the user visually, 
				SPID values are only displayed for header-level (request-level) rows, and this column is blank for detail-level 
				(statement-level) rows.

FirstSeen		For header-level rows, the first time (after @start time) the request was seen by the AutoWho collection code. For 
				detail-level rows, the first time (after @start time) the statement was seen by AutoWho. If a request was already executing 
				before @start, any previous collections by AutoWho will not be reflected in this output.

LastSeen		For header-level rows, the last time (before @end time) the request was seen by the AutoWho collection code. For 
				detail-level rows, the last time (after @end time) the statement was seen by AutoWho. If a request was already executing after 
				@end, any later collections by AutoWho will not be reflected in this output.

Extent(sec)		The time difference, in seconds, between FirstSeen and LastSeen. Note that this is NOT necessarily the duration of the 
				statement for several reasons:
					1) AutoWho polls on intervals (default=15 seconds for the background trace). A query is unlikely to end right after 
						an AutoWho collection and so its duration is almost always longer than seen by AutoWho.
					2) A request executing before @start or after @end will have a duration longer than Last minus First.
					3) The same statement text can be visited multiple times, e.g. inside of a loop or in a sub-proc that is called 
						multiple times.
				Thus, the word choice of "extent" is intentional: the proc merely notes the time gap between the first time the statement 
				was seen and the last. Future versions of this proc may offer the ability to display statement "run-lengths", highlighting 
				when a given statement was seen, then not seen for the same request, then seen again.
	';
	RAISERROR(@helpstr,10,1);

		SET @helpstr = N'
#Seen			The number of times a given statement was seen within @start and @end for the request. This can assist the user in 
				determining whether a high "Extent(sec)" value represents one instance of a given statement or many return visits to 
				the same statement. A large value in "Extent(sec)" but a lower value in #Seen indicates that the same statement was 
				re-visited a number of times. The collection interval of AutoWho can be of assistance. It is 15 seconds by default, 
				so an "Extent(sec)" of 300 and a "#Seen" value of 20 would indicate that AutoWho had seen this statement every time 
				over a 300 second time interval. (15 seconds multiplied by 4 times-per-minute multiplied by 5 minutes).

DB&Object		For header-level (request-level) rows, indicates the context database for the request. If a request''s context database 
				changes the first-observed context database is presented. For detail-level rows, represents the T-SQL object name (if any) 
				that the statement resides in. Ad-hoc SQL is currently just represented as a ".". (This will change in future versions).

Cmd				For header-level (request-level) rows, the input buffer of the request. For detail-level rows, the statement text.
	';
	RAISERROR(@helpstr,10,1);

		SET @helpstr = N'
Statuses		As a request (actually, a task) executes, it alternates between several states: running, runnable, and suspended. This
				field aggregates the various statuses seen for a given statement (no data is presented for header-level rows). The info
				can be used to determine what % of the time a given request is actually executing versus waiting for CPU (runnable) or
				waiting on other SPIDs or environmental factors (suspended).

				Note that suspended task states are divided between suspended-waiting-for-CXPACKET and all other suspended states. This
				enables the user to separate waiting on other sessions/environmental conditions from inter-thread waiting that is somewhat
				inevitable. 

NonCXWaits		The various wait types observed for a given statement are aggregated, along with the observed wait times. All non-CXPACKET
				waits are aggregated under this column. Note that because the underlying data is polling-based, the actual wait-times
				encountered by a given statement may vary. This column is blank for header-level (request-level) rows.

CXWaits			All CXPACKET waits and the sub-waits (e.g. PortOpen on Node 4) are aggregated under this column. This column allows the
				user to see which nodes the CXPACKET waits are occurring on, and to some extent to see whether those CXPACKET waits were
				consumer-side or producer-side. 

Max#Tasks		The MAX of the number of tasks seen for a given statement between @start and @end. Note that the DOP of a query is different
				than the # of tasks that can be allocated for the query. This column can be useful to see differences in task allocation
				between good and bad executions of a longer query or batch. Blank for header-level (request-level) rows.

HiDOP			The MAX degree of parallelism (from dm_exec_memory_grants) observed for this statement. This column can be useful to see
				differences in runtime DOP chosen for a given statement. Blank for header-level (request-level) rows.
			';
	RAISERROR(@helpstr,10,1);

	SET @helpstr = N'
MaxTdb			The MAX amount of TempDB (allocated minus deallocated for both system and user objects) observed for a given statement. 
				Blank for header-level (request-level) rows.

MaxQMem			The MAX amount of granted query memory (from dm_exec_memory_grants) observed for this statement. Blank for header-level
				(request-level) rows.

MaxUsedMem		The MAX amount of used query memory (from dm_exec_memory_grants) observed for this statement. Blank for header-level
				(request-level) rows.

Tlog			The MAX amount of transaction log used (from dm_tran_database_transactions) observed while this statement was active. 
				Transaction log is not really a statement-level resource. However, showing the max amount of T-log in use for a given
				statement text can help highlight which statements are using exceptionally-large amounts of TempDB. Blank for 
				header-level (request-level) rows.

CPU				The MAX amount of CPU (from dm_exec_requests.cpu) observed while this statement was active. CPU usage is a request-level
				metric not a statement-level metric. However, showing the max amount of CPU usage observed for a given statement
				text can help highlight which statements are using large amounts of CPU. Also, comparing CPU usage for the same statement
				between a good run of a longer request/batch and a bad run can help highlight other problems.
			';
	RAISERROR(@helpstr,10,1);

	SET @helpstr = N'
MemReads		The MAX amount of "logical" reads (from dm_exec_requests.logical_reads) observed while this statement was active. 
				Logical reads are a request-level metric rather than a statement-level metric. However, showing the max number of 
				logical reads observed for a given statement text can help highlight which statements are reading large amounts of 
				data in RAM. Also, comparing logical reads for the same statement between a good run of a longer request/batch and a 
				bad run can help highlight other problems.

PhysReads		The MAX amount of physical reads (from dm_exec_requests.reads) observed while this statement was active. Physical reads 
				are a request-level metric rather than a statement-level metric. However, showing the max number of disk reads observed 
				for a given statement text can help highlight which statements are causing heavy disk usage. Also, comparing physical 
				reads for the same statement between a good run of a longer request/batch and a bad run can help highlight variance due 
				to a warm/versus cold buffer pool when other factors are the same.

Writes			The MAX amount of page writes (from dm_exec_requests.writes) observed while this statement was active. Writes are a
				request-level metric rather than a statement-level metric. However, showing the max number of page writes observed for a given
				statement text can help highlight which statements are doing large updates to the database.

Plan&Info		For header-level (request-level) rows, provides a clickable-XML value that contains various attributes from dm_exec_sessions,
				dm_exec_connections, and dm_exec_requests. For detail-level (statement-level) rows, provides the query plan in XML form.
				This column is only present when either @attr="Y" or @plan="statement"
			';
	RAISERROR(@helpstr,10,1);

exitloc:

	RETURN 0;
END

GO
