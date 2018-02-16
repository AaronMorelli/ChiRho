SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[Branches_ReturnCandidateRequests] 
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

	FILE NAME: AutoWho.Branches_ReturnCandidateRequests.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.Branches_ReturnCandidateRequests

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: 

	OUTSTANDING ISSUES: 

To Execute
------------------------
*/
(
	@init		TINYINT,
	@startUTC	DATETIME,
	@endUTC		DATETIME,
	@spid		INT,
	@rqst		INT,
	@rqststart	DATETIME
)
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	DECLARE 
		@opt__ParallelWaitsThreshold INT,
		@lv__nullstring				NVARCHAR(8),
		@lv__nullint				INT,
		@lv__nullsmallint			SMALLINT,
		@PKInputBufferStore			BIGINT,
		@ibuf_text					NVARCHAR(4000),
		@ibuf_xml					XML;

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value

	--We rely on the calling proc to do basic validation of the parameters (data type, null, etc). We validate that the time range is valid,
	--and then determine which SPIDs/requests are present that might be of interest for sp_XR_Branches.

	IF @endUTC < (SELECT MIN(ct.UTCCaptureTime) FROM AutoWho.CaptureTimes ct WHERE ct.CollectionInitiatorID = @init)
	BEGIN
		RAISERROR('Value for parameter @end is less than the earliest AutoWho capture time still present in the database.', 16, 1);
		RETURN -1;
	END

	SELECT @opt__ParallelWaitsThreshold = opt.ParallelWaitsThreshold
	FROM AutoWho.Options opt;

	IF @opt__ParallelWaitsThreshold IS NULL
	BEGIN
		--This should never happen
		RAISERROR('Unable to find the config value for Parallel Waits Threshold. This is a fatal error. Please investigate config.', 16, 1);
		RETURN -1;
	END

	CREATE TABLE #CandidateRequests (
		session_id			SMALLINT NOT NULL,
		request_id			SMALLINT NOT NULL,
		rqst__start_time	DATETIME NOT NULL,
		FirstSeenUTC		DATETIME NOT NULL,
		LastSeenUTC			DATETIME NOT NULL,
		TimesSeen				INT NOT NULL,
		FirstInputBufferUTC	DATETIME NULL,

		FirstSeen			DATETIME NULL,
		LastSeen			DATETIME NULL,
		FKInputBufferStoreID	BIGINT NULL
	);

	CREATE TABLE #InputBufferStore (
		PKInputBufferStoreID		BIGINT NOT NULL PRIMARY KEY CLUSTERED,
		inputbuffer					NVARCHAR(4000) NOT NULL,
		inputbuffer_xml				XML
	);


	INSERT INTO #CandidateRequests (
		session_id,
		request_id,
		rqst__start_time,
		FirstSeenUTC,
		LastSeenUTC,
		TimesSeen,
		FirstInputBufferUTC
	)
	SELECT 
		session_id,
		request_id,
		rqst__start_time,
		[FirstSeenUTC] = MIN(UTCCaptureTime),
		[LastSeenUTC] = MAX(UTCCaptureTime),
		[TimesSeen] = COUNT(*),
		[FirstInputBufferUTC] = MIN(UTCCaptureTime_withInputBuffer)
	FROM (
		SELECT 
			sar.session_id,
			sar.request_id,
			sar.rqst__start_time,
			sar.UTCCaptureTime,
			UTCCaptureTime_withInputBuffer = CASE WHEN sar.FKInputBufferStoreID IS NOT NULL THEN sar.UTCCaptureTime ELSE NULL END
		FROM AutoWho.SessionsAndRequests sar
		WHERE sar.CollectionInitiatorID = @init
		AND sar.sess__is_user_process = 1
		AND sar.UTCCaptureTime BETWEEN @startUTC AND @endUTC		--only in our time window
		AND sar.calc__duration_ms >= @opt__ParallelWaitsThreshold	--only requests that ran long enough for parallel waits to be gathered
		AND sar.request_id >= 0		--only active requests
	) ss
	GROUP BY session_id,
		request_id,
		rqst__start_time;

	UPDATE targ 
	SET FirstSeen = xapp1.SPIDCaptureTime,
		LastSeen = xapp2.SPIDCaptureTime,
		FKInputBufferStoreID = xapp3.FKInputBufferStoreID
	FROM #CandidateRequests targ
		OUTER APPLY (
			SELECT sar.SPIDCaptureTime
			FROM AutoWho.SessionsAndRequests sar
			WHERE sar.CollectionInitiatorID = @init
			AND sar.session_id = targ.session_id
			AND sar.request_id = targ.request_id
			AND sar.rqst__start_time = targ.rqst__start_time
			AND sar.UTCCaptureTime = targ.FirstSeenUTC
		) xapp1
		OUTER APPLY (
			SELECT sar.SPIDCaptureTime
			FROM AutoWho.SessionsAndRequests sar
			WHERE sar.CollectionInitiatorID = @init
			AND sar.session_id = targ.session_id
			AND sar.request_id = targ.request_id
			AND sar.rqst__start_time = targ.rqst__start_time
			AND sar.UTCCaptureTime = targ.LastSeenUTC
		) xapp2
		OUTER APPLY (
			SELECT sar.FKInputBufferStoreID
			FROM AutoWho.SessionsAndRequests sar
			WHERE sar.CollectionInitiatorID = @init
			AND sar.session_id = targ.session_id
			AND sar.request_id = targ.request_id
			AND sar.rqst__start_time = targ.rqst__start_time
			AND sar.UTCCaptureTime = targ.FirstInputBufferUTC
		) xapp3;

	--Resolve our input buffers
	INSERT INTO #InputBufferStore (
		PKInputBufferStoreID,
		inputbuffer
		--inputbuffer_xml
	)
	SELECT ibs.PKInputBufferStoreID,
		ibs.InputBuffer
	FROM CoreXR.InputBufferStore ibs
	WHERE EXISTS (
		SELECT * 
		FROM #CandidateRequests cr
		WHERE cr.FKInputBufferStoreID IS NOT NULL
		AND cr.FKInputBufferStoreID = ibs.PKInputBufferStoreID
	);

	DECLARE resolveInputBufferStore  CURSOR LOCAL FAST_FORWARD FOR 
	SELECT 
		PKInputBufferStoreID,
		inputbuffer
	FROM #InputBufferStore;

	OPEN resolveInputBufferStore;
	FETCH resolveInputBufferStore INTO @PKInputBufferStore,
		@ibuf_text;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF @ibuf_text IS NULL
		BEGIN
			SET @ibuf_xml = CONVERT(XML, N'<?IBuf --' + NCHAR(10)+NCHAR(13) + N'The Input Buffer is NULL.' + NCHAR(10) + NCHAR(13) + 
			N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@PKInputBufferStore,-1)) + 
			NCHAR(10) + NCHAR(13) + N'-- ?>');
		END
		ELSE
		BEGIN
			BEGIN TRY
				SET @ibuf_xml = CONVERT(XML, N'<?IBuf --' + NCHAR(10)+NCHAR(13) + @ibuf_text + + NCHAR(10) + NCHAR(13) + 
				N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@PKInputBufferStore,-1)) + 
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END TRY
			BEGIN CATCH
				SET @ibuf_xml = CONVERT(XML, N'<?IBuf --' + NCHAR(10)+NCHAR(13) + N'Error converting Input Buffer to XML: ' + ERROR_MESSAGE() + NCHAR(10) + NCHAR(13) + 
				N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@PKInputBufferStore,-1)) + 
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END CATCH
		END

		UPDATE #InputBufferStore
		SET inputbuffer_xml = @ibuf_xml
		WHERE PKInputBufferStoreID = @PKInputBufferStore;

		FETCH resolveInputBufferStore INTO @PKInputBufferStore,
			@ibuf_text;
	END

	CLOSE resolveInputBufferStore;
	DEALLOCATE resolveInputBufferStore;

	SELECT 
		session_id,
		request_id,
		rqst__start_time,
		FirstSeen,
		LastSeen,
		[Duration_sec] = DATEDIFF(SECOND, FirstSeenUTC, LastSeenUTC),
		TimesSeen,
		ibs.inputbuffer_xml
	FROM #CandidateRequests cr
		LEFT OUTER JOIN #InputBufferStore ibs
			ON cr.FKInputBufferStoreID = ibs.PKInputBufferStoreID
	ORDER BY FirstSeenUTC, session_id, request_id;

		RAISERROR('On the Results pane are the SPIDs (session_ids), requests, and request start times which ran long enough to (potentially) generate parallel waits in the @start/@end time window specified. 
Please select a SPID, request (if not =0), and request start time as parameters for this procedure.', 16, 1);

	RETURN -1;
END 
GO
