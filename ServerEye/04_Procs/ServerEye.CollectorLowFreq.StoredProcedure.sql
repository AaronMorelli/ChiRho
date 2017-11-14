SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [ServerEye].[CollectorLowFreq]
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

	FILE NAME: ServerEye.CollectorLowFreq.StoredProcedure.sql

	PROCEDURE NAME: ServerEye.CollectorLowFreq

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Runs on a schedule (or initiated by a user via a viewer proc) and calls various sub-procs to gather miscellaneous 
		server-level DMV data. Collects data that we want captured fairly infrequently (by default every 10 minutes)

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------

*/
(
	@init				TINYINT,
	@LocalCaptureTime	DATETIME, 
	@UTCCaptureTime		DATETIME,
	@SQLServerStartTime	DATETIME
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @errorloc NVARCHAR(100),
			@err__ErrorSeverity INT, 
			@err__ErrorState INT, 
			@err__ErrorText NVARCHAR(4000),
			@lv__ProcRC INT;

BEGIN TRY

	/*
		We do ring buffers first because they are a bit more time sensitive. But we swallow any exceptions for now 
		because the semi-structured nature of the data makes this much more unpredictable. The viewer procs need to be 
		written in such a way as to not assume that ring buffer data is present.

		TODO: is that really the right decision?
	*/
	BEGIN TRY
		EXEC @lv__ProcRC = ServerEye.CollectorLowFreqRingBuffer @init = 255,
						@LocalCaptureTime = @LocalCaptureTime, 
						@UTCCaptureTime = @UTCCaptureTime,
						@SQLServerStartTime	= @SQLServerStartTime;

	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK;
	END CATCH

	SET @errorloc = N'dm_io_virtual_file_stats';
	INSERT INTO ServerEye.dm_io_virtual_file_stats (
		UTCCaptureTime, 
		LocalCaptureTime,
		database_id, 
		file_id,

		num_of_reads, 
		num_of_bytes_read,
		io_stall_read_ms,
		io_stall_queued_read_ms,

		num_of_writes, 
		num_of_bytes_written,
		io_stall_write_ms,
		io_stall_queued_write_ms, 

		io_stall, 
		size_on_disk_bytes
	)
	SELECT 
		@UTCCaptureTime, 
		@LocalCaptureTime,
		vfs.database_id, 
		vfs.file_id,

		vfs.num_of_reads, 
		vfs.num_of_bytes_read,
		vfs.io_stall_read_ms,
		vfs.io_stall_queued_read_ms,

		vfs.num_of_writes, 
		vfs.num_of_bytes_written,
		vfs.io_stall_write_ms,
		vfs.io_stall_queued_write_ms, 

		vfs.io_stall, 
		vfs.size_on_disk_bytes
	FROM sys.dm_io_virtual_file_stats(null, null) vfs;

	SET @errorloc = N'dm_os_wait_stats';
	INSERT INTO ServerEye.dm_os_wait_stats (
		UTCCaptureTime,
		LocalCaptureTime, 
		DimWaitTypeID, 
		waiting_tasks_count, 
		wait_time_ms, 
		max_wait_time_ms, 
		signal_wait_time_ms
	)
	SELECT 
		@UTCCaptureTime,
		@LocalCaptureTime, 
		d.DimWaitTypeID,
		w.waiting_tasks_count, 
		w.wait_time_ms, 
		w.max_wait_time_ms, 
		w.signal_wait_time_ms 
	FROM ServerEye.DimWaitType d
		INNER hash JOIN sys.dm_os_wait_stats w
			ON d.wait_type = w.wait_type
	WHERE w.max_wait_time_ms > 0
	OR w.signal_wait_time_ms > 0
	OR w.wait_time_ms > 0
	OR w.waiting_tasks_count > 0
	OPTION(FORCE ORDER);

	SET @errorloc = N'dm_os_latch_stats';
	INSERT INTO ServerEye.dm_os_latch_stats (
		UTCCaptureTime,
		LocalCaptureTime, 
		DimLatchClassID, 
		waiting_requests_count, 
		wait_time_ms, 
		max_wait_time_ms
	)
	SELECT 
		@UTCCaptureTime,
		@LocalCaptureTime, 
		d.DimLatchClassID,
		waiting_requests_count, 
		wait_time_ms, 
		max_wait_time_ms
	FROM ServerEye.DimLatchClass d
		INNER hash JOIN sys.dm_os_latch_stats l
			ON d.latch_class = l.latch_class
	WHERE l.waiting_requests_count > 0
	OR l.wait_time_ms > 0
	OR l.max_wait_time_ms > 0
	OPTION(FORCE ORDER);



	SET @errorloc = N'dm_os_spinlock_stats';
	INSERT INTO ServerEye.dm_os_spinlock_stats (
		UTCCaptureTime,
		LocalCaptureTime, 
		DimSpinlockID, 
		collisions, 
		spins, 
		spins_per_collision, 
		sleep_time, 
		backoffs
	)
	SELECT 
		@UTCCaptureTime,
		@LocalCaptureTime, 
		d.DimSpinlockID, 
		s.collisions, 
		s.spins, 
		s.spins_per_collision, 
		s.sleep_time, 
		s.backoffs
	FROM ServerEye.DimSpinlock d
		INNER hash JOIN sys.dm_os_spinlock_stats s
			ON d.SpinlockName = s.name
	WHERE s.collisions > 0
	OR s.spins > 0
	OR s.spins_per_collision > 0
	OR s.sleep_time > 0 
	OR s.backoffs > 0
	OPTION(FORCE ORDER);

	INSERT INTO [ServerEye].[dm_server_memory_dumps] (
		[filename],
		[creation_time],
		[size_in_bytes]
	)
	SELECT DISTINCT
		d.filename,
		d.creation_time,
		d.size_in_bytes
	FROM sys.dm_server_memory_dumps d
	WHERE NOT EXISTS (
		SELECT *
		FROM ServerEye.dm_server_memory_dumps d2
		WHERE d2.filename = d.filename 
		AND d2.creation_time = d.creation_time
	);

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