SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [ServerEye].[Collector]
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

	FILE NAME: ServerEye.Collector.StoredProcedure.sql

	PROCEDURE NAME: ServerEye.Collector

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Runs on a schedule (or initiated by a user via a viewer proc) and calls various sub-procs to gather miscellaneous 
		server-level DMV data.

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------

*/
(
	@init				TINYINT,
	@RunMedium			BIT, 
	@RunLow				BIT, 
	@RunBatch			BIT,
	@LocalCaptureTime	DATETIME OUTPUT, 
	@UTCCaptureTime		DATETIME OUTPUT,
	@RunWasSuccessful	BIT OUTPUT
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	--TODO: everything!

	SET @UTCCaptureTime = GETUTCDATE();
	SET @LocalCaptureTime = DATEADD(MINUTE, 0-DATEDIFF(MINUTE, GETDATE(), GETUTCDATE()), @UTCCaptureTime);

	DECLARE @lv__HighFrequencySuccessful	SMALLINT,
			@lv__MediumFrequencySuccessful	SMALLINT,
			@lv__LowFrequencySuccessful		SMALLINT,
			@lv__BatchFrequencySuccessful	SMALLINT;


	--FOR NOW, DUMMY LOGIC:
	SET @lv__HighFrequencySuccessful = 1;
	SET @lv__MediumFrequencySuccessful = CASE WHEN @RunMedium = 1 THEN 1 ELSE 0 END;
	SET @lv__LowFrequencySuccessful = CASE WHEN @RunLow = 1 THEN 1 ELSE 0 END;
	SET @lv__BatchFrequencySuccessful = CASE WHEN @RunBatch = 1 THEN 1 ELSE 0 END;
	--- END DUMMY LOGIC ---

	SET @lv__HighFrequencySuccessful = ISNULL(@lv__HighFrequencySuccessful,-1);
	SET @lv__MediumFrequencySuccessful = ISNULL(@lv__MediumFrequencySuccessful,-1);
	SET @lv__LowFrequencySuccessful = ISNULL(@lv__LowFrequencySuccessful,-1);
	SET @lv__BatchFrequencySuccessful = ISNULL(@lv__BatchFrequencySuccessful,-1);

	SET @RunWasSuccessful = CASE WHEN @lv__HighFrequencySuccessful = 1
										AND (
											(@RunMedium = 1 AND @lv__MediumFrequencySuccessful = 1)
											OR (@RunMedium = 0 AND @lv__MediumFrequencySuccessful = 0)
											)
										AND (
											(@RunLow = 1 AND @lv__LowFrequencySuccessful = 1)
											OR (@RunLow = 0 AND @lv__LowFrequencySuccessful = 0)
											)
										AND (
											(@RunBatch = 1 AND @lv__BatchFrequencySuccessful = 1)
											OR (@RunBatch = 0 AND @lv__BatchFrequencySuccessful = 0)
											)
									THEN 1
								ELSE 0
								END;

	INSERT INTO [ServerEye].[CaptureTimes] (
		[CollectionInitiatorID],
		[UTCCaptureTime],
		[LocalCaptureTime],
	
		[HighFrequencySuccessful],
		[MediumFrequencySuccessful],
		[LowFrequencySuccessful],
		[BatchFrequencySuccessful],

		[RunWasSuccessful],
		[ExtractedForDW],
		[ServerEyeDuration_ms],
		[DurationBreakdown]
	)
	SELECT 
		@init,
		@UTCCaptureTime,
		@LocalCaptureTime,
		@lv__HighFrequencySuccessful,
		@lv__MediumFrequencySuccessful,
		@lv__LowFrequencySuccessful,
		@lv__BatchFrequencySuccessful,
		
		[RunWasSuccessful] = @RunWasSuccessful,
		[ExtractedForDW] = 0,
		[ServerEyeDuration_ms] = 0,		--TODO
		[DurationBreakdown] = NULL;		--TODO

	RETURN 0;
END
GO