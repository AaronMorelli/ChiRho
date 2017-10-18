SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE   [CoreXR].[ChiRhoMaster]
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

	FILE NAME: CoreXR.ChiRhoMaster.StoredProcedure.sql

	PROCEDURE NAME: CoreXR.ChiRhoMaster

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Runs regularly throughout the day (by default, every 15 minutes), and checks whether the various
		traces (that drive data collection) should be running, and if they should but aren't, starts them.
		Also runs the purge/retention procedures for AutoWho & ServerEye (and in the future, other components of
		the ChiRho suite) to keep data volumes manageable.

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
DECLARE @lmsg VARCHAR(MAX)
EXEC CoreXR.ChiRhoMaster @ErrorMessage=@lmsg OUTPUT 
PRINT ISNULL(@lmsg, '<null>')
*/
(
	@AutoWhoJobName NVARCHAR(255)		= NULL,
	@ServerEyeJobName NVARCHAR(255)		= NULL,
	@PurgeDOW NVARCHAR(21)				= 'Sun',	-- to do every day of the week: 'SunMonTueWedThuFriSat'
	@PurgeHour TINYINT					= 3,		-- 3am
	@ErrorMessage VARCHAR(MAX)			= NULL OUTPUT
)
AS
BEGIN
	SET NOCOUNT ON;

	IF @AutoWhoJobName IS NULL
	BEGIN
		SET @AutoWhoJobName = DB_NAME() + N' - AlwaysDisabled - AutoWho Trace';
	END

	IF @ServerEyeJobName IS NULL
	BEGIN
		SET @ServerEyeJobName = DB_NAME() + N' - AlwaysDisabled - ServerEye Trace'
	END

	BEGIN TRY
		--General variables
		DECLARE @lv__masterErrorString NVARCHAR(MAX),
				@lv__curError NVARCHAR(MAX),
				@lv__ProcRC INT,
				@lv__PostProcessingStart DATETIME,
				@lv__PostProcessingEnd DATETIME
			;


		SET @PurgeDOW = LOWER(@PurgeDOW);

		IF @PurgeDOW IS NULL
			OR (@PurgeDOW NOT LIKE N'%sun%'
				AND @PurgeDOW NOT LIKE N'%mon%'
				AND @PurgeDOW NOT LIKE N'%tue%'
				AND @PurgeDOW NOT LIKE N'%wed%'
				AND @PurgeDOW NOT LIKE N'%thu%'
				AND @PurgeDOW NOT LIKE N'%fri%'
				AND @PurgeDOW NOT LIKE N'%sat%'
				AND @PurgeDOW NOT LIKE N'%never%'
			)
		BEGIN
			RAISERROR('Parameter @PurgeDOW must contain one or more 3-letter day-of-week tags (e.g. "Sun", "SunWed"), or contain the tag "Never".',16,1);
			RETURN -5;
		END

		IF @PurgeHour IS NULL OR @PurgeHour > 24
		BEGIN
			RAISERROR('Parameter @PurgeHour must be between 0 and 24 inclusive',16,1);
			RETURN -7;
		END

		--Update our DBID mapping table
		EXEC [CoreXR].[UpdateDBMapping];

		IF OBJECT_ID('tempdb..#CurrentlyRunningJobs1') IS NOT NULL
			BEGIN
				DROP TABLE #CurrentlyRunningJobs1;
			END 
			CREATE TABLE #CurrentlyRunningJobs1( 
				Job_ID uniqueidentifier,
				Last_Run_Date int,
				Last_Run_Time int,
				Next_Run_Date int,
				Next_Run_Time int,
				Next_Run_Schedule_ID int,
				Requested_To_Run int,
				Request_Source int,
				Request_Source_ID varchar(100),
				Running int,
				Current_Step int,
				Current_Retry_Attempt int, 
				aState int
			);

		INSERT INTO #CurrentlyRunningJobs1 
			EXECUTE master.dbo.xp_sqlagent_enum_jobs 1, 'hullabaloo'; --undocumented
			--can't use this because we can't nest an INSERT EXEC: exec msdb.dbo.sp_help_job @execution_status=1

		--In this proc, we do these things
		--	1. Check the status of the AutoWho job, and start, if appropriate
		--	2. Run AutoWho purge and maint if it is the appropriate time
		--	3. Check the status of the ServerEye job, and start, if appropriate
		--	4. Run ServerEye purge and maint if it is the appropriate time

		/*************************************** AutoWho Job stuff ***************************/
		DECLARE @AutoWho__IsEnabled NCHAR(1), 
				@AutoWho__NextStartTime DATETIME, 
				@AutoWho__NextEndTime DATETIME,
				@lv__tmptime DATETIME
				;

		IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs where name = @AutoWhoJobName)
		BEGIN
			RAISERROR('Job specified in parameter @AutoWhoJobName not found.',16,1);
			RETURN -7;
		END
		ELSE
		BEGIN
			SET @lv__tmptime = GETDATE();
			EXEC @lv__ProcRC = CoreXR.TraceTimeInfo @Utility=N'AutoWho', @PointInTime = @lv__tmptime, @UtilityIsEnabled = @AutoWho__IsEnabled OUTPUT,
					@UtilityStartTime = @AutoWho__NextStartTime OUTPUT, @UtilityEndTime = @AutoWho__NextEndTime OUTPUT
				;

			IF @lv__tmptime BETWEEN @AutoWho__NextStartTime AND @AutoWho__NextEndTime 
				AND @AutoWho__IsEnabled = N'Y'
			BEGIN
				--the trace SHOULD be running. check to see if it is already.
				--if not, then start it.

				IF NOT EXISTS (SELECT * 
						FROM #CurrentlyRunningJobs1 t
							INNER JOIN msdb.dbo.sysjobs j 
								ON t.Job_ID = j.job_id
					WHERE j.name = @AutoWhoJobName
					AND t.Running = 1)
				BEGIN
					IF NOT EXISTS (SELECT * FROM AutoWho.SignalTable t WITH (NOLOCK) 
									WHERE LOWER(SignalName) = N'aborttrace' 
									AND LOWER(t.SignalValue) = N'allday'
									AND DATEDIFF(DAY, InsertTime, GETDATE()) = 0)
					--any abort requests will, by default, continue their effect the rest of the day.
					BEGIN
						EXEC msdb.dbo.sp_start_job @job_name = @AutoWhoJobName;

						INSERT INTO AutoWho.[Log]
						(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
						SELECT SYSDATETIME(), NULL, 0, 'XRMaster', 'AutoWho Trace job started.'
						;
					END
					ELSE
					BEGIN
						INSERT INTO AutoWho.[Log]
						(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
						SELECT SYSDATETIME(), NULL, -1, 'XRMaster', N'An AbortTrace signal exists for today. This procedure has been told not to run the rest of the day.'
						;
					END
				END	 
			END		--IF @lv__tmptime BETWEEN @AutoWho__NextStartTime AND @AutoWho__NextEndTime
					-- that is, "IF trace should be running"
		END		--IF job exists/doesn't exist

		BEGIN TRY
			SET @lv__ProcRC = 0;
			EXEC @lv__ProcRC = AutoWho.UpdateStoreLastTouched;
		END TRY
		BEGIN CATCH
			--inside the loop, we swallow the error and just log it
			SET @ErrorMessage = N'Exception occurred when updating the store LastTouched values: ' + ERROR_MESSAGE();

			INSERT INTO AutoWho.[Log]
			(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), NULL, ERROR_NUMBER(), N'ErrorLastTouch', @ErrorMessage;
		END CATCH

		BEGIN TRY
			SET @lv__ProcRC = 0;
			SET @lv__PostProcessingEnd = DATEADD(SECOND, -30, GETDATE());		--so we steer clear of the tail of the table where data is being inserted regularly.
			SET @lv__PostProcessingStart = DATEADD(MINUTE, -30, @lv__PostProcessingEnd);
			EXEC @lv__ProcRC = AutoWho.PostProcessor @optionset=N'BackgroundTrace', @init=255, @start=@lv__PostProcessingStart, @end=@lv__PostProcessingEnd;
		END TRY
		BEGIN CATCH
			--inside the loop, we swallow the error and just log it
			SET @ErrorMessage = N'Exception occurred when resolving waits: ' + ERROR_MESSAGE();

			INSERT INTO AutoWho.[Log]
			(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), NULL, ERROR_NUMBER(), N'ErrorResolve', @ErrorMessage;
		END CATCH


		--Evaluate whether we should run AutoWho purge
		IF @PurgeDOW LIKE '%' + LOWER(SUBSTRING(LTRIM(RTRIM(DATENAME(dw,GETDATE()))),1,3)) + '%'
		BEGIN
			IF DATEPART(HOUR, GETDATE()) = @PurgeHour
				OR (DATEPART(HOUR, GETDATE()) = 0 AND @PurgeHour = 24)
			BEGIN
				IF NOT EXISTS (
					SELECT *
					FROM AutoWho.[Log] l
					WHERE l.LogDT > DATEADD(HOUR, -1, GETDATE())
					AND l.LocationTag = 'XRMaster'
					AND l.LogMessage = 'Purge procedure completed'
				)
				BEGIN
					EXEC AutoWho.ApplyRetentionPolicies;

					INSERT INTO AutoWho.[Log]
					(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
					SELECT SYSDATETIME(), NULL, 0, 'XRMaster', 'Purge procedure completed';

					--Now that we have (potentially) deleted a bunch of rows, do some index maint
					EXEC AutoWho.MaintainIndexes;
				END	--If purge hasn't yet been run 
			END	--If hour is a purge hour
		END	--If DOW is a purge day
		/*************************************** AutoWho Job stuff ***************************/

--SE is still in development
RETURN 0;

		/*************************************** ServerEye Job stuff ***************************/
/*
		DECLARE @ServerEye__IsEnabled NCHAR(1), 
				@ServerEye__NextStartTime DATETIME, 
				@ServerEye__NextEndTime DATETIME
				;

		IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs where name = @ServerEyeJobName)
		BEGIN
			RAISERROR('Job specified in parameter @ServerEyeJobName not found.',16,1);
			RETURN -9;
		END
		ELSE
		BEGIN
			SET @lv__tmptime = GETDATE();
			EXEC @lv__ProcRC = CoreXR.TraceTimeInfo @Utility=N'ServerEye', @PointInTime = @lv__tmptime, @UtilityIsEnabled = @ServerEye__IsEnabled OUTPUT,
					@UtilityStartTime = @ServerEye__NextStartTime OUTPUT, @UtilityEndTime = @ServerEye__NextEndTime OUTPUT
				;

			IF @lv__tmptime BETWEEN @ServerEye__NextStartTime AND @ServerEye__NextEndTime
				AND @ServerEye__IsEnabled = N'Y'
			BEGIN
				--the trace SHOULD be running. check to see if it is already.
				--if not, then start it.
				IF NOT EXISTS (SELECT * 
						FROM #CurrentlyRunningJobs1 t
							INNER JOIN msdb.dbo.sysjobs j ON t.Job_ID = j.job_id
					WHERE j.name = @ServerEyeJobName
					AND t.Running = 1)
				BEGIN
					IF NOT EXISTS (SELECT * FROM ServerEye.SignalTable t WITH (NOLOCK) 
									WHERE LOWER(SignalName) = N'aborttrace' 
									AND LOWER(t.SignalValue) = N'allday'
									AND DATEDIFF(DAY, InsertTime, GETDATE()) = 0)
					--any abort requests will, by default, continue their effect the rest of the day.
					--Thus, anyone putting a signal row into a table will need to later delete that record if they want the ServerEye trace to resume that day
					BEGIN
						EXEC msdb.dbo.sp_start_job @job_name = @ServerEyeJobName;

						INSERT INTO ServerEye.[Log]
						(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
						SELECT SYSDATETIME(), NULL, 0, 'XRMaster', 'ServerEye Trace job started.';
						;
					END
					ELSE
					BEGIN
						INSERT INTO AutoWho.[Log]
						(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
						SELECT SYSDATETIME(), NULL, -1, 'XRMaster', N'An AbortTrace signal exists for today. This procedure has been told not to run the rest of the day.';
						;
					END
				END
			END		--IF @tmptime BETWEEN @ServerEye__NextStartTime AND @ServerEye__NextEndTime
					-- that is, "IF trace should be running"
		END		--IF job exists/doesn't exist
*/

		--TODO: implement ServerEye purge
		/*************************************** ServerEye Job stuff ***************************/

		RETURN 0;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK;

		SET @ErrorMessage = N'Unexpected exception occurred: Error #: ' + CONVERT(NVARCHAR(20),ERROR_NUMBER()) + 
			N'; State: ' + CONVERT(NVARCHAR(20),ERROR_STATE()) + 
			N'; Severity: ' + CONVERT(NVARCHAR(20),ERROR_SEVERITY()) + 
			N'; Message: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

		RAISERROR(@ErrorMessage, 16, 1);
		RETURN -999;
	END CATCH
END


;
GO
