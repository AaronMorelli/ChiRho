SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [CoreXR].[TraceTimeInfo] 
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

	FILE NAME: CoreXR.TraceTimeInfo.StoredProcedure.sql

	PROCEDURE NAME: CoreXR.TraceTimeInfo

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: Given a point in time (usually executed with the current time), finds the start time and end time
	of the next trace for the @Utility supplied. 

	OUTSTANDING ISSUES: None at this time

To Execute
------------------------
DECLARE @rc INT,
	@pit DATETIME, 
	@en NCHAR(1),
	@st DATETIME, 
	@nd DATETIME

EXEC @rc = CoreXR.TraceTimeInfo @Utility=N'AutoWho', @PointInTime = @pit, @UtilityIsEnabled = @en OUTPUT, 
		@UtilityStartTime = @st OUTPUT, @UtilityEndTime = @nd OUTPUT

SELECT @rc as ProcRC, @en as Enabled, @st as StartTime, @nd as EndTime
*/
(
	@Utility NVARCHAR(20),
	@PointInTime DATETIME,
	@UtilityIsEnabled NCHAR(1) OUTPUT,
	@UtilityStartTime DATETIME OUTPUT,
	@UtilityEndTime DATETIME OUTPUT
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @lmsg NVARCHAR(4000),
		@rc INT,
		@timetmp_smaller DATETIME,
		@timetmp_larger DATETIME;

	DECLARE 
		@opt__BeginTime		SMALLINT,		
		@opt__EndTime		SMALLINT
		;

	IF @PointInTime IS NULL
	BEGIN
		SET @PointInTime = GETDATE();
	END

	IF @Utility NOT IN (N'AutoWho', N'ServerEye')
	BEGIN
		RAISERROR('Parameter @Utility must be in the following list: AutoWho, ServerEye', 16, 1);
		RETURN -1;
	END

	IF @Utility = N'AutoWho'
	BEGIN
		SELECT 
			@UtilityIsEnabled		 = [AutoWhoEnabled],
			@opt__BeginTime			 = [BeginTime],
			@opt__EndTime			 = [EndTime]
		FROM AutoWho.Options o;

		--Ok, we have the various option values. Note that if BeginTime is smaller than EndTime, 
		-- we have a trace that does NOT span a day... e.g. 5am to 4pm
		-- However, if EndTime is > BeginTime, then we DO have a trace that spans a day, e.g. 4pm to 5am
		SET @UtilityStartTime = DATEADD(MINUTE, 
										@opt__BeginTime % 100,
										DATEADD(HOUR, 
											@opt__BeginTime / 100, 
											CONVERT(DATETIME, 
													CONVERT(VARCHAR(20), @PointInTime,101)
													)
												)
										);

		SET @UtilityEndTime = DATEADD(MINUTE, 
										@opt__EndTime % 100,
										DATEADD(HOUR, 
											@opt__EndTime / 100, 
											CONVERT(DATETIME, 
													CONVERT(VARCHAR(20), @PointInTime,101)
													)
												)
										);

		IF @UtilityEndTime < @UtilityStartTime
		BEGIN
			SET @UtilityEndTime = DATEADD(DAY, 1, @UtilityEndTime);
		END

		IF @PointInTime >= @UtilityEndTime
		BEGIN
			SET @UtilityStartTime = DATEADD(DAY, 1, @UtilityStartTime);
			SET @UtilityEndTime = DATEADD(DAY, 1, @UtilityEndTime);
		END
	END
	ELSE IF @Utility = N'ServerEye'
	BEGIN
		RETURN 0;
		/* Return to when SE dev gets serious
		--Ok validation succeeeded. Get our option values
		SELECT 
			@UtilityIsEnabled		 = [ServerEyeEnabled],
			@opt__BeginTime			 = [BeginTime],
			@opt__EndTime			 = [EndTime]
		FROM ServerEye.Options o
		;

		--Ok, we have the various option values. Note that if BeginTime is smaller than EndTime, 
		-- we have a trace that does NOT span a day... e.g. 5am to 4pm
		-- However, if EndTime is < BeginTime, then we DO have a trace that spans a day, e.g. 4pm to 5am
		SET @UtilityStartTime = DATEADD(MINUTE, 
										@opt__BeginTime % 100,
										DATEADD(HOUR, 
											@opt__BeginTime / 100, 
											CONVERT(DATETIME, 
													CONVERT(VARCHAR(20), @PointInTime,101)
													)
												)
										);

		SET @UtilityEndTime = DATEADD(MINUTE, 
										@opt__EndTime % 100,
										DATEADD(HOUR, 
											@opt__EndTime / 100, 
											CONVERT(DATETIME, 
												CONVERT(VARCHAR(20), @PointInTime,101)
													)
												)
										);

		IF @UtilityEndTime < @UtilityStartTime
		BEGIN
			SET @UtilityEndTime = DATEADD(DAY, 1, @UtilityEndTime);
		END

		IF @PointInTime >= @UtilityEndTime
		BEGIN
			SET @UtilityStartTime = DATEADD(DAY, 1, @UtilityStartTime);
			SET @UtilityEndTime = DATEADD(DAY, 1, @UtilityEndTime);
		END
		*/
	END --outside IF/ELSE that controls utility-specific logic	

	RETURN 0;
END 
GO
