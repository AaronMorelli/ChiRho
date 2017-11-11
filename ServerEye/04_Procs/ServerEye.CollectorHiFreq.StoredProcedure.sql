SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [ServerEye].[CollectorHiFreq]
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

	FILE NAME: ServerEye.CollectorHiFreq.StoredProcedure.sql

	PROCEDURE NAME: CollectorHiFreq.Collector

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Runs on a schedule (or initiated by a user via a viewer proc) and calls various sub-procs to gather miscellaneous 
		server-level DMV data. Collects data for metrics that we want captured at a high frequency (by default every 1 minute)

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------

*/
(
	@init				TINYINT,
	@LocalCaptureTime	DATETIME, 
	@UTCCaptureTime		DATETIME
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @errorloc NVARCHAR(100);
	DECLARE @lv__osisNULLID INT,
		@lv__CurrentOsisID INT,
		@lv__scratchint INT;

	DECLARE @err__ErrorSeverity INT, 
			@err__ErrorState INT, 
			@err__ErrorText NVARCHAR(4000);


BEGIN TRY
	--sys.dm_os_sys_info. Find the row for our "Current" time window
	SET @errorloc = 'osis1'
	SELECT @lv__osisNULLID = d.osisID
	FROM ServerEye.dm_os_sys_info_stable d
	WHERE d.EffectiveEndTimeUTC IS NULL;

	IF @lv__osisNULLID IS NULL
	BEGIN
		--no rows exist here yet. Insert
		SET @errorloc = 'osis2'
		INSERT INTO ServerEye.dm_os_sys_info_stable (
			EffectiveStartTimeUTC,
			EffectiveEndTimeUTC,
			EffectiveStartTime,
			EffectiveEndTime,
			sqlserver_start_time_ms_ticks,
			sqlserver_start_time,
			cpu_count,
			hyperthread_ratio,
			--physical_memory_in_bytes,
			physical_memory_kb,
			--virtual_memory_in_bytes,
			virtual_memory_kb,
			stack_size_in_bytes,
			os_quantum,
			os_error_mode,
			os_priority_class,
			max_workers_count,
			scheduler_count,
			scheduler_total_count,
			deadlock_monitor_serial_number,
			affinity_type,
			affinity_type_desc,
			time_source,
			time_source_desc,
			virtual_machine_type,
			virtual_machine_type_desc
		)
		SELECT
			[EffectiveStartTimeUTC] = @UTCCaptureTime, 
			[EffectiveEndTimeUTC] = NULL,  
			[EffectiveStartTime] = @LocalCaptureTime, 
			[EffectiveEndTime] = NULL, 
			sqlserver_start_time_ms_ticks,
			sqlserver_start_time,
			cpu_count,
			hyperthread_ratio,
			--physical_memory_in_bytes,
			physical_memory_kb,
			--virtual_memory_in_bytes,
			virtual_memory_kb,
			stack_size_in_bytes,
			os_quantum,
			os_error_mode,
			os_priority_class,
			max_workers_count,
			scheduler_count,
			scheduler_total_count,
			deadlock_monitor_serial_number,
			affinity_type,
			affinity_type_desc,
			time_source,
			time_source_desc,
			virtual_machine_type,
			virtual_machine_type_desc
		FROM sys.dm_os_sys_info dosi;

		SET @lv__CurrentOsisID = SCOPE_IDENTITY();
	END
	ELSE
	BEGIN
		--rows exist, and we have the ID of the "current status" row.
		-- Compare to see if the DMV contents are different. 
		SET @errorloc = 'osis3'
		INSERT INTO ServerEye.dm_os_sys_info_stable (
			EffectiveStartTimeUTC,
			EffectiveEndTimeUTC,
			EffectiveStartTime,
			EffectiveEndTime,
			sqlserver_start_time_ms_ticks,
			sqlserver_start_time,
			cpu_count,
			hyperthread_ratio,
			--physical_memory_in_bytes,
			physical_memory_kb,
			--virtual_memory_in_bytes,
			virtual_memory_kb,
			stack_size_in_bytes,
			os_quantum,
			os_error_mode,
			os_priority_class,
			max_workers_count,
			scheduler_count,
			scheduler_total_count,
			deadlock_monitor_serial_number,
			affinity_type,
			affinity_type_desc,
			time_source,
			time_source_desc,
			virtual_machine_type,
			virtual_machine_type_desc
		)
		SELECT 
			[EffectiveStartTimeUTC] = @UTCCaptureTime, 
			[EffectiveEndTimeUTC] = NULL, 
			[EffectiveStartTime] = @LocalCaptureTime, 
			[EffectiveEndTime] = NULL, 
			sqlserver_start_time_ms_ticks,
			sqlserver_start_time,
			cpu_count,
			hyperthread_ratio,
			--physical_memory_in_bytes,
			physical_memory_kb,
			--virtual_memory_in_bytes,
			virtual_memory_kb,
			stack_size_in_bytes,
			os_quantum,
			os_error_mode,
			os_priority_class,
			max_workers_count,
			scheduler_count,
			scheduler_total_count,
			deadlock_monitor_serial_number,
			affinity_type,
			affinity_type_desc,
			time_source,
			time_source_desc,
			virtual_machine_type,
			virtual_machine_type_desc
		FROM sys.dm_os_sys_info dosi
		WHERE NOT EXISTS (
			SELECT *
			FROM ServerEye.dm_os_sys_info_stable osis
			WHERE osisID = @lv__osisNULLID
			AND osis.sqlserver_start_time_ms_ticks = dosi.sqlserver_start_time_ms_ticks
			AND osis.sqlserver_start_time = dosi.sqlserver_start_time
			AND osis.cpu_count = dosi.cpu_count
			AND osis.hyperthread_ratio = dosi.hyperthread_ratio
			--AND osis.physical_memory_in_bytes = dosi.physical_memory_in_bytes
			AND osis.physical_memory_kb = dosi.physical_memory_kb
			--AND osis.virtual_memory_in_bytes = dosi.virtual_memory_in_bytes
			AND osis.virtual_memory_kb = dosi.virtual_memory_kb
			AND osis.stack_size_in_bytes = dosi.stack_size_in_bytes
			AND osis.os_quantum = dosi.os_quantum
			AND osis.os_error_mode = dosi.os_error_mode
			AND ISNULL(osis.os_priority_class,-555) = ISNULL(dosi.os_priority_class,-555)
			AND osis.max_workers_count = dosi.max_workers_count
			AND osis.scheduler_count = dosi.scheduler_count
			AND osis.scheduler_total_count = dosi.scheduler_total_count
			AND osis.deadlock_monitor_serial_number = dosi.deadlock_monitor_serial_number
			AND osis.affinity_type = dosi.affinity_type
			AND osis.affinity_type_desc = dosi.affinity_type_desc
			AND osis.time_source = dosi.time_source
			AND osis.time_source_desc = dosi.time_source_desc
			AND osis.virtual_machine_type = dosi.virtual_machine_type
			AND osis.virtual_machine_type_desc = dosi.virtual_machine_type_desc
		);

		SET @lv__scratchint = @@ROWCOUNT;

		IF @lv__scratchint > 0
		BEGIN
			SET @lv__CurrentOsisID = SCOPE_IDENTITY();

			SET @errorloc = 'osis4'
			UPDATE ServerEye.dm_os_sys_info_stable
			SET EffectiveEndTimeUTC = @UTCCaptureTime,
				EffectiveEndTime = @LocalCaptureTime
			WHERE osisID = @lv__osisNULLID;
		END
		ELSE
		BEGIN
			SET @lv__CurrentOsisID = @lv__osisNULLID;
		END
	END	--IF @osisNULL IS NULL

	SET @errorloc = 'osis5'
	INSERT INTO ServerEye.dm_os_sys_info_volatile (
		UTCCaptureTime, 
		LocalCaptureTime, 
		StableOSIID, 
		cpu_ticks, 
		ms_ticks, 
		committed_kb, 
		--bpool_committed, 
		committed_target_kb, 
		--bpool_commit_target, 
		visible_target_kb, 
		--bpool_visible, 
		process_kernel_time_ms, 
		process_user_time_ms
	)
	SELECT 
		@UTCCaptureTime, 
		@LocalCaptureTime, 
		@lv__CurrentOsisID,
		i.cpu_ticks, 
		i.ms_ticks, 
		i.committed_kb, 
		--i.bpool_committed, 
		i.committed_target_kb,
		--i.bpool_commit_target, 
		i.visible_target_kb, 
		--i.bpool_visible, 
		i.process_kernel_time_ms, 
		i.process_user_time_ms
	FROM sys.dm_os_sys_info i;



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