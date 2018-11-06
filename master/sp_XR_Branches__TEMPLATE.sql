USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_XR_Branches]
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

	FILE NAME: sp_XR_Branches__TEMPLATE.sql

	PROCEDURE NAME: sp_XR_Branches

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					

	PURPOSE: 

		Detailed help documentation is available by running
			EXEC sp_XR_Branches @help=N'Y'


	FUTURE ENHANCEMENTS: 


To Execute
------------------------


*/
(
	--navigation & execution modes
	@start			DATETIME,			--Currently required, may change this later
	@end			DATETIME,			--ditto
	@source			NVARCHAR(20)=N'trace',		--'trace' = standard AutoWho.Executor background trace; 
												-- 'pastSV' reviews data from past sp_XR_SessionViewer calls done in "current" mode
												-- 'pastQP' reviews data from past sp_XR_QueryProgress calls done in "current" or "time series" mode.
	@spid			INT=NULL,				--If not specified, this proc will return a list of SPIDs in the time range specified by @start and @end
	@rqst			INT=0,
	@rqststart		DATETIME=NULL,			--If not specified, this proc will return a list of batch start times (sys.dm_exec_requests.rqst_start_time) for the SPID specified in the @start/@end time window
	@stmtid			BIGINT=NULL,			--If not specified, this proc will return a list of PKSQLStmtStoreIDs that were executed (and observed) by the @spid/@rqst/@rqststart combo
	@help			NVARCHAR(10)=N'N'		--params, columns, all
)
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SET ANSI_PADDING ON;

	DECLARE @lv__StartUTC							DATETIME,
			@lv__EndUTC								DATETIME,
			@helpstr								NVARCHAR(MAX),
			@helpexec								NVARCHAR(4000);


	SET @helpexec = N'
EXEC dbo.sp_XR_Branches @start=''<start datetime>'',@end=''<end datetime>'',
	@source=N''trace'',		-- trace, pastSV, pastQP
	@spid=NULL,@rqst=NULL,@rqststart=''<request start datetime>'',
	@stmtid=NULL,
	@help=N''N''										-- N, All, Params, Columns
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

	DECLARE @lv__SQLVersion NVARCHAR(10);
	SELECT @lv__SQLVersion = (
	SELECT CASE
			WHEN t.col1 LIKE N'8%' THEN N'2000'
			WHEN t.col1 LIKE N'9%' THEN N'2005'
			WHEN t.col1 LIKE N'10.5%' THEN N'2008R2'
			WHEN t.col1 LIKE N'10%' THEN N'2008'
			WHEN t.col1 LIKE N'11%' THEN N'2012'
			WHEN t.col1 LIKE N'12%' THEN N'2014'
			WHEN t.col1 LIKE N'13%' THEN N'2016'
		END AS val1
	FROM (SELECT CONVERT(SYSNAME, SERVERPROPERTY(N'ProductVersion')) AS col1) AS t);


	DECLARE @dir__shortcols BIT;
	SET @dir__shortcols = CONVERT(BIT,0);

	DECLARE @lv__UtilityName NVARCHAR(30);
	DECLARE @lv__CollectionInitiatorID TINYINT;
	SET @lv__UtilityName = N'sp_XR_Branches'


	--Put @start's value into our helpexec string
	SET @helpexec = REPLACE(@helpexec,'<start datetime>', REPLACE(CONVERT(NVARCHAR(20), @start, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @start, 108) + '.' + 
													RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @start)),3)
					);

	--Put @end into our helpexec string
	SET @helpexec = REPLACE(@helpexec,'<end datetime>', REPLACE(CONVERT(NVARCHAR(20), @end, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @end, 108) + '.' + 
														RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @end)),3)
						);

	SET @lv__StartUTC = DATEADD(MINUTE, DATEDIFF(MINUTE, GETDATE(), GETUTCDATE()), @start);
	SET @lv__EndUTC = DATEADD(MINUTE, DATEDIFF(MINUTE, GETDATE(), GETUTCDATE()), @end);

	--We use UTC for this check b/c of the DST "fall-back" scenario. We don't want to prevent a user from calling this proc for a timerange 
	--that already occurred (e.g. 1:30am-1:45am) at the second occurrence of 1:15am that day.
	IF @lv__StartUTC > GETUTCDATE() OR @lv__EndUTC > GETUTCDATE()
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

	IF ISNULL(@spid,0) < 0
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @spid must be >= 0.',16,1);
		RETURN -1;
	END

	IF @rqst IS NULL
	BEGIN
		SET @rqst = 0;
	END
	ELSE
	BEGIN
		IF @rqst < 0
		BEGIN
			RAISERROR(@helpexec,10,1);
			RAISERROR('Parameter @rqst must be >= 0, or left NULL (defaults to 0).',16,1);
			RETURN -1;
		END
	END

	IF @spid IS NULL OR @rqststart IS NULL	--@rqst will never be NULL, per above logic.
	BEGIN
		RAISERROR(@helpexec,10,1);

		--User may not know what SPID to select. Find SPIDs with requests (in the @start/@end timerange) whose 
		-- duration is > than the parallel waits threshold, and return that list of SPID/requests and the input buffer
		EXEC @@XRDATABASENAME@@.AutoWho.Branches_ReturnCandidateRequests @init = @lv__CollectionInitiatorID,
				@startUTC = @lv__StartUTC,
				@endUTC = @lv__EndUTC,
				@spid = @spid,
				@rqst = @rqst,
				@rqststart = @rqststart;
		--The above proc will raise an error after returning the result set. We just return a non-zero code.
		RETURN -1;
	END --IF @spid IS NULL

	--If we get here, we have non-null @spid, @rqst, and @rqststart values. If @stmtid is null, we get a list of statements for this batch
	IF @stmtid IS NULL
	BEGIN
		RAISERROR(@helpexec,10,1);
		EXEC @@XRDATABASENAME@@.AutoWho.Branches_ReturnCandidateStatements @init = @lv__CollectionInitiatorID,
				@startUTC = @lv__StartUTC,
				@endUTC = @lv__EndUTC,
				@spid = @spid,
				@rqst = @rqst,
				@rqststart = @rqststart;
		--The above proc will raise an error after returning the result set. We just return a non-zero code
		RETURN -1;
	END --IF @stmtid IS NULL


	--If we get here, we presumably have valid values for @spid/@rqst/@rqststart/@stmtid. This means we also have a list of UTCCaptureTimes
	--for the statement (from the StatementCaptureTimes table) and thus we have a collection of dm_os_tasks/dm_os_waiting_tasks values 
	--to examine. Now to the real work!

	EXEC @@XRDATABASENAME@@.AutoWho.ViewParallelBranches @init = @lv__CollectionInitiatorID,
			@startUTC = @lv__StartUTC,
			@endUTC = @lv__EndUTC,
			@spid = @spid,
			@rqst = @rqst,
			@rqststart = @rqststart,
			@stmtid = @stmtid;


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

	--If the user DID enter @start/@end info, then we use those values to replace the <datetime> tags
	-- in the @helpexec string.
	IF @start IS NOT NULL
	BEGIN
		SET @helpexec = REPLACE(@helpexec,'<start datetime>', REPLACE(CONVERT(NVARCHAR(20), @start, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @start, 108) + '.' + 
															RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @start)),3)
							);
	END 

	IF @end IS NOT NULL 
	BEGIN
		SET @helpexec = REPLACE(@helpexec,'<end datetime>', REPLACE(CONVERT(NVARCHAR(20), @end, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @end, 108) + '.' + 
															RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @end)),3)
							);
	END

	SET @helpstr = @helpexec;
	RAISERROR(@helpstr,10,1) WITH NOWAIT;

	IF @Help=N'N'
	BEGIN
		RETURN 0;
	END

	SET @helpstr = N'
sp_XR_Branches version 2008R2.1

Key Concepts and Terminology
-------------------------------------------------------------------------------------------------------------------------------------------
TODO
	';
	RAISERROR(@helpstr,10,1) WITH NOWAIT;

	IF @Help NOT IN (N'PARAMS',N'ALL')
	BEGIN
		GOTO helpcolumns
	END

helpparams:
	SET @helpstr = N'
Parameters
-------------------------------------------------------------------------------------------------------------------------------------------
@start			Valid Values: any datetime value in the past

				TODO
	
@end			Valid Values: any datetime in the past more recent than @start

				TODO';
	RAISERROR(@helpstr,10,1);


	SET @helpstr = N'		
@source			Valid Values: "trace" (default), "pastSV", "pastQP" (all case-insensitive)

				As mentioned above, AutoWho code is used to capture DMV data whether the background trace is doing the collecting
				or sp_XR_SessionViewer/QueryProgress are collecting the live data and returning to the user. @source allows the user 
				to point to either data collected by the standard background trace (using "trace"), to data collected by past 
				sp_XR_SessionViewer runs ("pastSV") or to data collected by past sp_XR_QueryProgress runs ("pastQP"). ';
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
TODO';
	RAISERROR(@helpstr,10,1);


exitloc:
	RETURN 0;
END
GO
