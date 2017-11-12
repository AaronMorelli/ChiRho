SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [ServerEye].[CollectorLowFreqRingBuffer]
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

	FILE NAME: ServerEye.CollectorLowFreqRingBuffer.StoredProcedure.sql

	PROCEDURE NAME: ServerEye.CollectorLowFreqRingBuffer

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Runs underneath the low-frequency collection proc and pulls various ring buffer data.

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------

*/
(
	@init				TINYINT,
	@LocalCaptureTime	DATETIME, 
	@UTCCaptureTime		DATETIME,
	@SQLServerStartTime DATETIME
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @errorloc NVARCHAR(100);

	DECLARE @err__ErrorSeverity INT, 
			@err__ErrorState INT, 
			@err__ErrorText NVARCHAR(4000);

	DECLARE @lv__AllRBsSuccessful INT,
		@lv__scratchint INT;

BEGIN TRY
	/* TODOs

		There are some thing that need to happen outside of this proc for it to work properly:

			- Need to obtain the @SQLServerStartTime in some higher level of the call stack 
				and pass it to this procedure

			- The RingBufferProgress table is apparently "pre-populated when the Executor is starting up"
				Need to replicate that logic in my official code, from the POC code I did a long time ago.


			
		Also: 
			- There's a DELETE below that removes any ring buffers not in an IN-list. That DELETE is 
			temporary as I implement the various ring buffers that I actually want to persist. It
			allows the #RingBufferTempContents to just hold ring buffers that the below code
			already handles.

	*/

	SET @lv__AllRBsSuccessful = 1;		--start assuming things will work

	IF OBJECT_ID('tempdb..#RingBufferTempContents') IS NOT NULL DROP TABLE #RingBufferTempContents;
	CREATE TABLE #RingBufferTempContents (
		ring_buffer_address varbinary(8) NOT NULL, 
		ring_buffer_type nvarchar(60) NOT NULL, 
		timestamp bigint not null, 
		record nvarchar(3072) null
	);


	INSERT INTO #RingBufferTempContents (
		ring_buffer_address,	--don't need this for anything, AFAIK
		ring_buffer_type,
		timestamp,
		record
	)
	SELECT rb.ring_buffer_address, rb.ring_buffer_type, rb.timestamp, rb.record
	FROM ServerEye.RingBufferProgress rbp
		RIGHT OUTER hash JOIN
		sys.dm_os_ring_buffers rb
			ON rbp.ring_buffer_type = rb.ring_buffer_type
	WHERE 
		--if a totally new ring buffer (i.e. isn't in the RingBufferProgress table, 
		-- which is pre-populated when the Executor is starting up), then save it off 
		-- so we can post process this unknown/new type.
		rbp.ring_buffer_type IS NULL 
		OR (
			rbp.ring_buffer_type IS NOT NULL 

			/* This deserves some explaining: 
				the "timestamp" field in dm_os_ring_buffers is NOT unique per ring_buffer_type; in fact, there are 
				often duplicates. This means, of course that multiple entries for a ring buffer can occur for the same
				timestamp value. The unique identifier is the RecordID value. However, RecordID is the XML, and parsing
				the XML is expensive. Thus, we use a hack to maintain efficiency while also giving a reasonable effort
				to only eliminate records we've already processed. We basically throw away ring buffer entries 
				whose timestamp is less than *last max timestamp processed - 5 seconds*. The 5 second buffer means
				that if our last captured managed to capture some ring buffer entries for a given timestamp but other
				entries for that timestamp were still being written to the ring buffer (i.e. were not captured by us),
				then we are likely to captured them in the next run.

				The actual persist to the permanent table uses RecordID to weed out things we've already persisted, of course.
			*/
			AND rb.timestamp > (rbp.max_timestamp_processed - 5000)	

			--certain RBs just aren't very interesting (too geeky or data is better-accessible from a DMV now)
			AND rb.ring_buffer_type NOT IN (N'RING_BUFFER_HOBT_SCHEMAMGR', 
				N'RING_BUFFER_MEMORY_BROKER_CLERKS', N'RING_BUFFER_XE_BUFFER_STATE',
				N'RING_BUFFER_SCHEDULER')
		)
	OPTION(FORCE ORDER);

	DELETE 
	FROM #RingBufferTempContents
	WHERE ring_buffer_type NOT IN (N'RING_BUFFER_SCHEDULER_MONITOR');



	--we do this before each collection, as it makes our timestamp processing more accurate
	DECLARE @cpu_ticks BIGINT, 
			@ms_ticks BIGINT;
	SELECT 
		@cpu_ticks = i.cpu_ticks,
		@ms_ticks = i.ms_ticks
	FROM sys.dm_os_sys_info i;

	/* RING_BUFFER_SCHEDULER_MONITOR
		This is the only format I've seen. Useful data!

		<Record id = "2333" type ="RING_BUFFER_SCHEDULER_MONITOR" time ="438203849">
			<SchedulerMonitorEvent>
				<SystemHealth>
					<ProcessUtilization>0</ProcessUtilization>
					<SystemIdle>96</SystemIdle>
					<UserModeTime>0</UserModeTime>
					<KernelModeTime>0</KernelModeTime>
					<PageFaults>69</PageFaults>
					<WorkingSetDelta>0</WorkingSetDelta>
					<MemoryUtilization>100</MemoryUtilization>
				</SystemHealth>
			</SchedulerMonitorEvent>
		</Record>
	*/
	BEGIN TRY
		INSERT INTO ServerEye.Ring_Buffer_Scheduler_Monitor (
			SQLServerStartTime, 
			RecordID,
			[timestamp],
			ExceptionTime,
			UTCCaptureTime,
			LocalCaptureTime,
			ProcessUtilization,
			SystemIdle,
			UserModeTime,
			KernelModeTime,
			PageFaults,
			WorkingSetDelta,
			MemoryUtilization
		)
		SELECT 
			@SQLServerStartTime,
			RecordID, 
			ss1.timestamp,
			ExceptionTime,
			@UTCCaptureTime,
			@LocalCaptureTime,
			ProcessUtilization,
			SystemIdle,
			UserModeTime,
			KernelModeTime,
			PageFaults,
			WorkingSetDelta,
			MemoryUtilization
		FROM (
			SELECT 
				[RecordID] = recordXML.value('(./Record/@id)[1]', 'int'),
				ss0.timestamp,
				--Got this calculation from Jonathan Kehayias
				--https://www.sqlskills.com/blogs/jonathan/identifying-external-memory-pressure-with-dm_os_ring_buffers-and-ring_buffer_resource_monitor/
				ExceptionTime = DATEADD (ss, (-1 * ((@cpu_ticks / CONVERT (float, ( @cpu_ticks / @ms_ticks ))) - ss0.timestamp)/1000), GETDATE()),

				[ProcessUtilization] = recordXML.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int'),
				[SystemIdle] = recordXML.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int'),
				[UserModeTime] = recordXML.value('(./Record/SchedulerMonitorEvent/SystemHealth/UserModeTime)[1]', 'int'),
				[KernelModeTime] = recordXML.value('(./Record/SchedulerMonitorEvent/SystemHealth/KernelModeTime)[1]', 'int'),
				[PageFaults] = recordXML.value('(./Record/SchedulerMonitorEvent/SystemHealth/PageFaults)[1]', 'int'),
				[WorkingSetDelta] = recordXML.value('(./Record/SchedulerMonitorEvent/SystemHealth/WorkingSetDelta)[1]', 'int'),
				[MemoryUtilization] = recordXML.value('(./Record/SchedulerMonitorEvent/SystemHealth/MemoryUtilization)[1]', 'int')
			FROM (
				SELECT rb.timestamp,
					CONVERT(XML,rb.record) as recordXML
				FROM #RingBufferTempContents rb
				WHERE rb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
			) ss0
		) ss1
		WHERE NOT EXISTS (
			SELECT *
			FROM ServerEye.Ring_Buffer_Scheduler_Monitor e
			WHERE e.SQLServerStartTime = @SQLServerStartTime
			AND ss1.RecordID = e.RecordID
		);

		UPDATE targ 
		SET targ.max_timestamp_processed = ISNULL(ss.max_timestamp, targ.max_timestamp_processed)
		FROM ServerEye.RingBufferProgress targ 
			LEFT OUTER JOIN (
				SELECT MAX(t.timestamp) as max_timestamp
				FROM #RingBufferTempContents t
				WHERE t.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
				) ss
					ON 1=1
		WHERE targ.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR';
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK;
		SET @lv__AllRBsSuccessful = 0;

		--save off our problematic data for later analysis
		INSERT INTO ServerEye.RingBufferCausedExceptions (
			UTCCaptureTime,
			LocalCaptureTime,
			ring_buffer_address,
			ring_buffer_type,
			timestamp,
			record
		)
		SELECT @UTCCaptureTime,
			@LocalCaptureTime,
			t.ring_buffer_address, 
			t.ring_buffer_type, 
			t.timestamp,
			t.record
		FROM #RingBufferTempContents t
		WHERE t.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR';

		SET @err__ErrorSeverity = ERROR_SEVERITY();
		SET @err__ErrorState = ERROR_STATE();
		SET @err__ErrorText = N'Error occurred during processing of RING_BUFFER_SCHEDULER_MONITOR. Error #: ' + 
			CONVERT(NVARCHAR(20),ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20),ERROR_STATE()) + N'; Severity: ' + 
			CONVERT(NVARCHAR(20),ERROR_SEVERITY()) + N'; message: ' + ERROR_MESSAGE();

		EXEC ServerEye.LogEvent @ProcID=@@PROCID, @EventCode=-999, @TraceID=NULL, @Location='SchedMonRB', @Message=@err__ErrorText;
		--we log the message, but we don't re-raise the exception. 
	END CATCH

	RETURN 0;
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK;

	SET @err__ErrorSeverity = ERROR_SEVERITY();
	SET @err__ErrorState = ERROR_STATE();
	SET @err__ErrorText = N'Unexpected exception occurred at location "' + ISNULL(@errorloc,N'<null>') + '". Error #: ' + 
		CONVERT(NVARCHAR(20),ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20),ERROR_STATE()) + N'; Severity: ' + 
		CONVERT(NVARCHAR(20),ERROR_SEVERITY()) + N'; message: ' + ERROR_MESSAGE();

	RAISERROR(@err__ErrorText, @err__ErrorSeverity, @err__ErrorState);
	RETURN -1;
END CATCH

	RETURN 0;
END
GO