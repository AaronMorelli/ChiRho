select 
	o.type,
	DevStatus = '/*Not Yet Implemented*/						SELECT * FROM sys.' + o.name
from sys.all_objects o
where o.name like 'dm%'
--and o.type not in ('AF','FN', 'FS', 'IF','IT','S','X','U','PC','SQ','TF','P')
order by o.type, o.name 




/* DMVs (this query was run on a SQL 2014 instance)

******** Priority 1 ********
COMPLETE HI-FREQ		SELECT * FROM sys.dm_os_sys_info
COMPLETE HI-FREQ		SELECT * FROM sys.dm_db_file_space_usage		--for tempdb usage (though could do for all DBs)

COMPLETE MED-FREQ		DB/file stats  (sys.databases, sys.master_files, sys.database_files, DBCC SQLPERF(LOGSPACE))

COMPLETE LOW-FREQ		SELECT * FROM sys.dm_io_virtual_file_stats
COMPLETE LOW-FREQ		SELECT * FROM sys.dm_os_wait_stats
COMPLETE LOW-FREQ		SELECT * FROM sys.dm_os_latch_stats
COMPLETE LOW-FREQ		SELECT * FROM sys.dm_os_spinlock_stats
SELECT * FROM sys.dm_os_ring_buffers			--only certain ring buffers are higher priority
			Connectivity
			Exception	(aggregate)
			Security_Error
			DONE Scheduler_Monitor

			These are NOT yet a priority
				CLRAppDomain
				Memory_Broker
				Resource_Monitor	
				XE_Log

SELECT * FROM sys.dm_os_performance_counters	--only certain counters are truly important. Need the perf counter table and some prioritization scheme.



Create a ticket to cover the work to research the following (which version they were introduced into, when I should use them, other workarounds, etc)
SELECT * FROM sys.dm_db_log_space_usage		I'm already using DBCC SQLPERF(LOGSPACE) to get log usage 
SELECT * FROM sys.dm_os_volume_stats		--Which SQL version was this released in? Is there a workaround for older versions?
use msdb 
go
exec sp_getVolumeFreeSpace @database_name='master', @file_id=1		--must be run in msdb for some reason???

******** Priority 1 ********

******** Priority 2 ********
	--Misc
	SELECT * FROM sys.dm_server_memory_dumps		--we just want to know if there have been any mem dumps
	exec sp_server_diagnostics						--Any value here? this runs as an XE session. Some of the info it collects is useful, other info is redundant

	--CPU
	SELECT * FROM sys.dm_os_schedulers
	SELECT * FROM sys.dm_os_nodes
	SELECT * FROM sys.dm_os_workers
	SELECT * FROM sys.dm_os_tasks		--any value here?
	SELECT * FROM sys.dm_os_threads		--any value here?

	--Memory
	SELECT * FROM sys.dm_os_memory_nodes
	SELECT * FROM sys.dm_os_process_memory
	SELECT * FROM sys.dm_os_sys_memory
	SELECT * FROM sys.dm_os_memory_broker_clerks
	SELECT * FROM sys.dm_os_memory_clerks
	SELECT * FROM sys.dm_os_memory_cache_clock_hands

	--DB and TempDB
	SELECT * FROM sys.dm_db_session_space_usage		--agg bkgrd usage
	SELECT * FROM sys.dm_db_task_space_usage		--ditto
	SELECT * FROM sys.dm_db_partition_stats			--maybe cap a baseline at the first run, then trigger collection when we see a DB grow? to see which table(s) are growing?
	select * from sys.partitions			--any value here?
	select * from sys.allocation_units		--any value here?
	select * from sys.system_internals_allocation_units
	select * from sys.system_internals_partitions


	--Connections profile (maybe aggregate these to get a profile of who is connected to the system?)
	SELECT * FROM sys.dm_exec_requests
	SELECT * FROM sys.dm_exec_sessions
	SELECT * FROM sys.dm_exec_connections
******** Priority 2 ********


******** Priority 3 ********
	--Misc
	SELECT * FROM sys.dm_resource_governor_resource_pool_volumes
	SELECT * FROM sys.dm_resource_governor_resource_pools
	SELECT * FROM sys.dm_resource_governor_workload_groups
	SELECT * FROM sys.dm_server_services		--so we can easily show which services are up and running and since when.
	SELECT * FROM sys.dm_tcp_listener_states	--only display when something is wrong!
	sp_readerrorlog
	exec sp_enumerrorlogs
	select * from sys.configurations		--any value here?
	select * from sys.sysprocesses			--any value here?


	--Memory
	SELECT * FROM sys.dm_os_buffer_descriptors		--may move to priority 2
	SELECT * FROM sys.dm_exec_query_memory_grants		--agg by pools or groups or whatever?
	SELECT * FROM sys.dm_exec_query_resource_semaphores
	SELECT * FROM sys.dm_os_memory_brokers
	SELECT * FROM sys.dm_os_memory_cache_counters
	SELECT * FROM sys.dm_os_memory_cache_hash_tables
	SELECT * FROM sys.dm_os_memory_pools

	--query stats module
	SELECT * FROM sys.dm_exec_procedure_stats
	SELECT * FROM sys.dm_exec_query_stats
	SELECT * FROM sys.dm_exec_trigger_stats
	SELECT * FROM sys.dm_exec_cached_plans

	--TempDB
	SELECT * FROM sys.dm_tran_version_store		--trigger an aggregate query if tempdb usage is very high.
	SELECT * FROM sys.dm_tran_top_version_generators	--is this cheap to query? Maybe just use this instead

	--DB details
	select * from sys.indexes 
	SELECT * FROM sys.dm_db_missing_index_details
	SELECT * FROM sys.dm_db_missing_index_group_stats
	SELECT * FROM sys.dm_db_missing_index_groups
	SELECT * FROM sys.dm_db_index_usage_stats
	SELECT * FROM sys.dm_db_index_operational_stats
	SELECT * FROM sys.dm_db_index_physical_stats
	SELECT * FROM sys.dm_db_missing_index_columns
	SELECT * FROM sys.dm_db_database_page_allocations
	SELECT * FROM sys.dm_db_stats_properties
	SELECT * FROM sys.dm_db_stats_properties_internal


	--traces
	select * from sys.traces
	SELECT * FROM sys.dm_xe_sessions			--this is probably the more useful XE view here

	select * from sys.server_event_sessions		--this is the definition, not the execution state
	select * from sys.server_event_session_actions
	select * from sys.server_event_session_events
	select * from sys.server_event_session_fields
	select * from sys.server_event_session_targets
	SELECT * FROM sys.dm_xe_session_targets		--is there any value here?
******** Priority 3 ********

*/





/* The kind of output that sp_server_diagnostics returns
component_name=system
state=1
state_desc=clean
<system spinlockBackoffs="0" sickSpinlockType="none" sickSpinlockTypeAfterAv="none" latchWarnings="0" isAccessViolationOccurred="0" 
	writeAccessViolationCount="0" totalDumpRequests="0" intervalDumpRequests="0" nonYieldingTasksReported="0" 
	pageFaults="4637" systemCpuUtilization="7" sqlCpuUtilization="0" 
	BadPagesDetected="0" BadPagesFixed="0" LastBadPageAddress="0x0"/>


component_name=io_subsystem
state=1
state_desc=clean
<ioSubsystem ioLatchTimeouts="0" intervalLongIos="0" totalLongIos="0"><longestPendingRequests></longestPendingRequests></ioSubsystem>

component_name=events
state=0
state_desc=unknown
<events><session startTime="2017-10-31T11:50:44.120" droppedEvents="0" largestDroppedEvent="0">
	<RingBufferTarget truncated="0" processingTime="0" totalEventsProcessed="0" eventCount="0" droppedCount="0" memoryUsed="0"></RingBufferTarget>
</session></events>

component_name=resource
state=1
state_desc=clean
<resource lastNotification="RESOURCE_MEMPHYSICAL_HIGH" outOfMemoryExceptions="0" isAnyPoolOutOfMemory="0" 
	processOutOfMemoryPeriod="0">
	<memoryReport name="Process/System Counts" unit="Value">
		<entry description="Available Physical Memory" value="523055104"/>
		<entry description="Available Virtual Memory" value="8777396142080"/>
		<entry description="Available Paging File" value="1107722240"/>
		<entry description="Working Set" value="1101684736"/>
		<entry description="Percent of Committed Memory in WS" value="85"/>
		<entry description="Page Faults" value="5101862"/>
		<entry description="System physical memory high" value="1"/>
		<entry description="System physical memory low" value="0"/>
		<entry description="Process physical memory low" value="0"/>
		<entry description="Process virtual memory low" value="0"/>
	</memoryReport>
	<memoryReport name="Memory Manager" unit="KB">
		<entry description="VM Reserved" value="17751172"/>
		<entry description="VM Committed" value="1261056"/>
		<entry description="Locked Pages Allocated" value="0"/>
		<entry description="Large Pages Allocated" value="0"/>
		<entry description="Emergency Memory" value="1024"/>
		<entry description="Emergency Memory In Use" value="16"/>
		<entry description="Target Committed" value="1505712"/>
		<entry description="Current Committed" value="1261056"/>
		<entry description="Pages Allocated" value="1103240"/>
		<entry description="Pages Reserved" value="0"/>
		<entry description="Pages Free" value="76088"/>
		<entry description="Pages In Use" value="775080"/>
		<entry description="Page Alloc Potential" value="1388144"/>
		<entry description="NUMA Growth Phase" value="0"/>
		<entry description="Last OOM Factor" value="0"/>
		<entry description="Last OS Error" value="0"/>
	</memoryReport>
</resource>


component_name=query_processing
state=1
state_desc=clean
<queryProcessing maxWorkers="512" workersCreated="67" workersIdle="25" tasksCompletedWithinInterval="23" pendingTasks="0" oldestPendingTaskWaitingTime="0" 
	hasUnresolvableDeadlockOccurred="0" hasDeadlockedSchedulersOccurred="0" trackingNonYieldingScheduler="0x0">
	<cpuIntensiveRequests></cpuIntensiveRequests>
	<pendingTasks></pendingTasks>
	<blockingTasks></blockingTasks>
		<topWaits>
			<nonPreemptive>
				<byCount>
					<wait waitType="CXPACKET" waits="69460742" averageWaitTime="0" maxWaitTime="124"/>
					<wait waitType="PAGELATCH_EX" waits="1408479" averageWaitTime="0" maxWaitTime="748"/>
					<wait waitType="HADR_FILESTREAM_IOMGR_IOCOMPLETION" waits="1384930" averageWaitTime="624" maxWaitTime="57573406"/>
					<wait waitType="WRITELOG" waits="235952" averageWaitTime="100" maxWaitTime="12658953"/>
					<wait waitType="WAIT_XTP_OFFLINE_CKPT_NEW_LOG" waits="140958" averageWaitTime="6137" maxWaitTime="57576729"/>
					<wait waitType="MSQL_XP" waits="27392" averageWaitTime="0" maxWaitTime="660"/>
					<wait waitType="PAGELATCH_SH" waits="19358" averageWaitTime="0" maxWaitTime="91"/>
					<wait waitType="ASYNC_NETWORK_IO" waits="15846" averageWaitTime="0" maxWaitTime="2013"/>
					<wait waitType="SLEEP_BPOOL_FLUSH" waits="15027" averageWaitTime="0" maxWaitTime="102"/>
					<wait waitType="QDS_PERSIST_TASK_MAIN_LOOP_SLEEP" waits="11771" averageWaitTime="73496" maxWaitTime="57591680"/>
				</byCount>
				<byDuration>
					<wait waitType="HADR_FILESTREAM_IOMGR_IOCOMPLETION" waits="1384930" averageWaitTime="624" maxWaitTime="57573406"/>
					<wait waitType="WAIT_XTP_OFFLINE_CKPT_NEW_LOG" waits="140958" averageWaitTime="6137" maxWaitTime="57576729"/>
					<wait waitType="QDS_PERSIST_TASK_MAIN_LOOP_SLEEP" waits="11771" averageWaitTime="73496" maxWaitTime="57591680"/>
					<wait waitType="WRITELOG" waits="235952" averageWaitTime="100" maxWaitTime="12658953"/>
					<wait waitType="CXPACKET" waits="69460742" averageWaitTime="0" maxWaitTime="124"/>
					<wait waitType="LCK_M_IS" waits="1" averageWaitTime="436687" maxWaitTime="436687"/>
					<wait waitType="CLR_AUTO_EVENT" waits="8" averageWaitTime="5176" maxWaitTime="20465"/>
					<wait waitType="LCK_M_S" waits="40" averageWaitTime="719" maxWaitTime="8601"/>
					<wait waitType="LCK_M_X" waits="125" averageWaitTime="209" maxWaitTime="14532"/>
					<wait waitType="PWAIT_ALL_COMPONENTS_INITIALIZED" waits="3" averageWaitTime="8506" maxWaitTime="8517"/>
				</byDuration>
			</nonPreemptive>
		<preemptive><byCount><wait waitType="PREEMPTIVE_OS_RSFXDEVICEOPS" waits="488508" averageWaitTime="0" maxWaitTime="957"/><wait waitType="PREEMPTIVE_OS_AUTHENTICATIONOPS" waits="407089" averageWaitTime="0" maxWaitTime="186"/><wait waitType="PREEMPTIVE_XE_CALLBACKEXECUTE" waits="141063" averageWaitTime="0" maxWaitTime="7"/>
			<wait waitType="PREEMPTIVE_OS_DELETESECURITYCONTEXT" waits="94981" averageWaitTime="0" maxWaitTime="254"/><wait waitType="PREEMPTIVE_OS_AUTHORIZATIONOPS" waits="70899" averageWaitTime="1" maxWaitTime="3290"/><wait waitType="PREEMPTIVE_OS_REVERTTOSELF" waits="67914" averageWaitTime="0" maxWaitTime="20"/>
			<wait waitType="PREEMPTIVE_OS_QUERYCONTEXTATTRIBUTES" waits="67816" averageWaitTime="0" maxWaitTime="88"/><wait waitType="PREEMPTIVE_OS_DECRYPTMESSAGE" waits="67815" averageWaitTime="0" maxWaitTime="42"/><wait waitType="PREEMPTIVE_OS_DISCONNECTNAMEDPIPE" waits="52332" averageWaitTime="0" maxWaitTime="64"/>
			<wait waitType="PREEMPTIVE_OS_GETPROCADDRESS" waits="27392" averageWaitTime="0" maxWaitTime="0"/>
				</byCount>
			<byDuration><wait waitType="PREEMPTIVE_OS_AUTHZINITIALIZECONTEXTFROMSID" waits="55" averageWaitTime="1653" maxWaitTime="13680"/><wait waitType="PREEMPTIVE_OS_AUTHORIZATIONOPS" waits="70899" averageWaitTime="1" maxWaitTime="3290"/><wait waitType="PREEMPTIVE_OS_FILEOPS" waits="934" averageWaitTime="64" maxWaitTime="990"/>
				<wait waitType="PREEMPTIVE_OS_RSFXDEVICEOPS" waits="488508" averageWaitTime="0" maxWaitTime="957"/><wait waitType="PREEMPTIVE_OS_AUTHENTICATIONOPS" waits="407089" averageWaitTime="0" maxWaitTime="186"/><wait waitType="PREEMPTIVE_OS_WRITEFILE" waits="12014" averageWaitTime="3" maxWaitTime="956"/>
				<wait waitType="PREEMPTIVE_OS_DELETESECURITYCONTEXT" waits="94981" averageWaitTime="0" maxWaitTime="254"/><wait waitType="PREEMPTIVE_OS_LIBRARYOPS" waits="1" averageWaitTime="13200" maxWaitTime="13200"/><wait waitType="PREEMPTIVE_OS_QUERYREGISTRY" waits="11755" averageWaitTime="1" maxWaitTime="957"/>
				<wait waitType="PREEMPTIVE_OS_GENERICOPS" waits="72" averageWaitTime="159" maxWaitTime="10302"/></byDuration></preemptive></topWaits>
</queryProcessing>


*/







/*
--Special feature stuff, unlikely to implement anytime soon.
SELECT * FROM sys.dm_server_audit_status
SELECT * FROM sys.dm_audit_actions
SELECT * FROM sys.dm_audit_class_type_map
SELECT * FROM sys.dm_broker_activated_tasks
SELECT * FROM sys.dm_broker_connections
SELECT * FROM sys.dm_broker_forwarded_messages
SELECT * FROM sys.dm_broker_queue_monitors
SELECT * FROM sys.dm_cryptographic_provider_algorithms
SELECT * FROM sys.dm_cryptographic_provider_keys
SELECT * FROM sys.dm_cryptographic_provider_sessions
SELECT * FROM sys.dm_cdc_errors
SELECT * FROM sys.dm_cdc_log_scan_sessions
SELECT * FROM sys.dm_clr_appdomains
SELECT * FROM sys.dm_clr_loaded_assemblies
SELECT * FROM sys.dm_clr_properties
SELECT * FROM sys.dm_clr_tasks
SELECT * FROM sys.dm_cryptographic_provider_properties
SELECT * FROM sys.dm_database_encryption_keys
SELECT * FROM sys.dm_db_mirroring_auto_page_repair
SELECT * FROM sys.dm_db_mirroring_connections
SELECT * FROM sys.dm_db_mirroring_past_actions
SELECT * FROM sys.dm_db_xtp_checkpoint_files
SELECT * FROM sys.dm_db_xtp_checkpoint_stats
SELECT * FROM sys.dm_db_xtp_gc_cycle_stats
SELECT * FROM sys.dm_db_xtp_hash_index_stats
SELECT * FROM sys.dm_db_xtp_index_stats
SELECT * FROM sys.dm_db_xtp_memory_consumers
SELECT * FROM sys.dm_db_xtp_merge_requests
SELECT * FROM sys.dm_db_xtp_nonclustered_index_stats
SELECT * FROM sys.dm_db_xtp_object_stats
SELECT * FROM sys.dm_db_xtp_table_memory_stats
SELECT * FROM sys.dm_db_xtp_transactions
SELECT * FROM sys.dm_filestream_file_io_handles
SELECT * FROM sys.dm_filestream_file_io_requests
SELECT * FROM sys.dm_filestream_non_transacted_handles
SELECT * FROM sys.dm_db_fts_index_physical_stats
SELECT * FROM sys.dm_fts_active_catalogs
SELECT * FROM sys.dm_fts_fdhosts
SELECT * FROM sys.dm_fts_index_population
SELECT * FROM sys.dm_fts_memory_buffers
SELECT * FROM sys.dm_fts_memory_pools
SELECT * FROM sys.dm_fts_outstanding_batches
SELECT * FROM sys.dm_fts_population_ranges
SELECT * FROM sys.dm_fts_semantic_similarity_population
SELECT * FROM sys.dm_fts_index_keywords
SELECT * FROM sys.dm_fts_index_keywords_by_document
SELECT * FROM sys.dm_fts_index_keywords_by_property
SELECT * FROM sys.dm_fts_index_keywords_position_by_document
SELECT * FROM sys.dm_fts_parser
SELECT * FROM sys.dm_hadr_auto_page_repair
SELECT * FROM sys.dm_hadr_availability_group_states
SELECT * FROM sys.dm_hadr_availability_replica_cluster_nodes
SELECT * FROM sys.dm_hadr_availability_replica_cluster_states
SELECT * FROM sys.dm_hadr_availability_replica_states
SELECT * FROM sys.dm_hadr_cluster
SELECT * FROM sys.dm_hadr_cluster_members
SELECT * FROM sys.dm_hadr_cluster_networks
SELECT * FROM sys.dm_hadr_database_replica_cluster_states
SELECT * FROM sys.dm_hadr_database_replica_states
SELECT * FROM sys.dm_hadr_instance_node_map
SELECT * FROM sys.dm_hadr_name_id_map
SELECT * FROM sys.dm_qn_subscriptions
SELECT * FROM sys.dm_repl_articles
SELECT * FROM sys.dm_repl_schemas
SELECT * FROM sys.dm_repl_tranhash
SELECT * FROM sys.dm_repl_traninfo
SELECT * FROM sys.dm_xtp_gc_queue_stats
SELECT * FROM sys.dm_xtp_gc_stats
SELECT * FROM sys.dm_xtp_system_memory_consumers
SELECT * FROM sys.dm_xtp_threads
SELECT * FROM sys.dm_xtp_transaction_recent_rows
SELECT * FROM sys.dm_xtp_transaction_stats
sp_xtp_control_proc_exec_stats
sp_xtp_control_query_exec_stats
SELECT * FROM sys.dm_logconsumer_cachebufferrefs
SELECT * FROM sys.dm_logconsumer_privatecachebuffers
SELECT * FROM sys.dm_logpool_consumers
SELECT * FROM sys.dm_logpool_sharedcachebuffers
SELECT * FROM sys.dm_logpoolmgr_freepools
SELECT * FROM sys.dm_logpoolmgr_respoolsize
SELECT * FROM sys.dm_logpoolmgr_stats








--Data is too obscure. May write queries that can be run through a @Directives parameter
SELECT * FROM sys.dm_exec_query_optimizer_info
SELECT * FROM sys.dm_exec_query_transformation_stats
SELECT * FROM sys.dm_os_virtual_address_dump
SELECT * FROM sys.dm_exec_cached_plan_dependent_objects
SELECT * FROM sys.dm_os_memory_node_access_stats
SELECT * FROM sys.dm_os_memory_allocations
SELECT * FROM sys.dm_os_memory_objects
SELECT * FROM sys.dm_os_memory_cache_entries
SELECT * FROM sys.dm_os_worker_local_storage


--More of an AutoWho thing
SELECT * FROM sys.dm_os_waiting_tasks
SELECT * FROM sys.dm_exec_query_profiles
SELECT * FROM sys.dm_exec_cursors(null)
SELECT * FROM sys.dm_exec_plan_attributes
SELECT * FROM sys.dm_exec_query_plan
SELECT * FROM sys.dm_exec_sql_text
SELECT * FROM sys.dm_exec_text_query_plan
SELECT * FROM sys.dm_exec_xml_handles(null)

SELECT * FROM sys.dm_tran_active_snapshot_database_transactions
SELECT * FROM sys.dm_tran_active_transactions
SELECT * FROM sys.dm_tran_commit_table
SELECT * FROM sys.dm_tran_current_snapshot
SELECT * FROM sys.dm_tran_current_transaction
SELECT * FROM sys.dm_tran_database_transactions
SELECT * FROM sys.dm_tran_locks
SELECT * FROM sys.dm_tran_session_transactions
SELECT * FROM sys.dm_tran_transactions_snapshot


--No need right now, miscellaneous reasons.
SELECT * FROM sys.dm_db_persisted_sku_features
SELECT * FROM sys.dm_db_script_level
SELECT * FROM sys.dm_db_uncontained_entities
SELECT * FROM sys.dm_exec_background_job_queue
SELECT * FROM sys.dm_exec_background_job_queue_stats
SELECT * FROM sys.dm_io_backup_tapes
SELECT * FROM sys.dm_io_cluster_shared_drives
SELECT * FROM sys.dm_io_cluster_valid_path_names
SELECT * FROM sys.dm_io_pending_io_requests
SELECT * FROM sys.dm_logpool_hashentries
SELECT * FROM sys.dm_logpool_stats
SELECT * FROM sys.dm_os_stacks
SELECT * FROM sys.dm_os_sublatches
SELECT * FROM sys.dm_os_buffer_pool_extension_configuration
SELECT * FROM sys.dm_os_child_instances
SELECT * FROM sys.dm_os_cluster_nodes
SELECT * FROM sys.dm_os_cluster_properties
SELECT * FROM sys.dm_os_dispatcher_pools
SELECT * FROM sys.dm_os_dispatchers
SELECT * FROM sys.dm_os_hosts
SELECT * FROM sys.dm_os_loaded_modules
SELECT * FROM sys.dm_os_server_diagnostics_log_configurations
SELECT * FROM sys.dm_db_objects_disabled_on_compatibility_level_change
SELECT * FROM sys.dm_exec_describe_first_result_set
SELECT * FROM sys.dm_exec_describe_first_result_set_for_object
SELECT * FROM sys.dm_sql_referenced_entities
SELECT * FROM sys.dm_sql_referencing_entities
SELECT * FROM sys.dm_os_windows_info
SELECT * FROM sys.dm_resource_governor_configuration
SELECT * FROM sys.dm_resource_governor_resource_pool_affinity
SELECT * FROM sys.dm_server_registry
SELECT * FROM sys.dm_xe_map_values
SELECT * FROM sys.dm_xe_packages
SELECT * FROM sys.dm_xe_objects
SELECT * FROM sys.dm_xe_object_columns
SELECT * FROM sys.dm_xe_session_event_actions
SELECT * FROM sys.dm_xe_session_events
SELECT * FROM sys.dm_xe_session_object_columns
*/
















