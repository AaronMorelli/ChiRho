/****** Object:  StoredProcedure [ServerEye].[IntervalMetrics_TopQueries]    Script Date: 11/22/2017 12:44:53 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [ServerEye].[IntervalMetrics_TopQueries] 
(
	@CollectionTime DATETIME, 
	@SQLServerStartTime DATETIME
)
AS
BEGIN
	/* My before/after logic needs improvements: 
		- I have logic that attempts to detect whether the plan has been purged between the before
			snapshot and the after snapshot. e.g. if b.execution_count < a.execution_count then use b
													else use b - a.
			2 things: 
				I should standardize on 1 check, rather than having the execution_count delta being based
					on whether b.execution_count is < a.execution count, and the physical_io delta being
					based on the a.physical_io < a.physical_io, etc. Instead, just use execution count
					as the check for everything.

			In addition to execution_count, add the appropriate datetime field on when the entry was created
			(need to do some testing to determine which field to use. Could be in either dm_exec_query_stats
			or dm_exec_cached_plans). I need to do this because it is possible to have an "after" execution_count
			that is higher than a "before" execution count, but the plan WAS dropped and it just has been executed
			a lot since it was re-cached. 

			Remember to test with OPTION(RECOMPILE) as well!


	*/
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @errorloc NVARCHAR(40),
			@errormsg NVARCHAR(4000),
			@errorsev INT,
			@errorstate INT;

BEGIN TRY
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	DECLARE @lv__CurrentTable NCHAR(1),
			@const__SwitchName NVARCHAR(50);

	SET @const__SwitchName = N'PlanCacheDMVs'

	SELECT @lv__CurrentTable = ts.CurrentTable
	FROM ServerEye.TableSwitcher ts WITH (FORCESEEK)
	WHERE ts.SwitchName = @const__SwitchName
	;

	SET @errorloc = N'Create #Tables';
	CREATE TABLE #ObjectStatDelta (
		--key fields
		[database_id]				[int] NOT NULL,
		[object_id]					[int] NOT NULL,

		--major attributes
		[type]						[char](2) NULL,
		[sql_handle]				[varbinary](64) NULL,
		[plan_handle]				[varbinary](64) NULL,
		[cached_time]				[datetime] NULL,
		[last_execution_time]		[datetime] NULL,

		--the measures we're after
		[execution_count]			[bigint] NOT NULL,
		[execution_count_delta]		[bigint] NOT NULL,

		[total_worker_time]			[bigint] NOT NULL,
		[total_worker_time_delta]	[bigint] NOT NULL,
		[last_worker_time]			[bigint] NOT NULL,
		[min_worker_time]			[bigint] NOT NULL,
		[max_worker_time]			[bigint] NOT NULL,

		[total_physical_reads]		[bigint] NOT NULL,
		[total_physical_reads_delta] [bigint] NOT NULL,
		[last_physical_reads]		[bigint] NOT NULL,
		[min_physical_reads]		[bigint] NOT NULL,
		[max_physical_reads]		[bigint] NOT NULL,

		[total_logical_writes]		[bigint] NOT NULL,
		[total_logical_writes_delta] [bigint] NOT NULL,
		[last_logical_writes]		[bigint] NOT NULL,
		[min_logical_writes]		[bigint] NOT NULL,
		[max_logical_writes]		[bigint] NOT NULL,

		[total_logical_reads]		[bigint] NOT NULL,
		[total_logical_reads_delta] [bigint] NOT NULL,
		[last_logical_reads]		[bigint] NOT NULL,
		[min_logical_reads]			[bigint] NOT NULL,
		[max_logical_reads]			[bigint] NOT NULL,

		[total_elapsed_time]		[bigint] NOT NULL,
		[total_elapsed_time_delta] [bigint] NOT NULL,
		[last_elapsed_time]			[bigint] NOT NULL,
		[min_elapsed_time]			[bigint] NOT NULL,
		[max_elapsed_time]			[bigint] NOT NULL,

		rnk_total_rows				int, 
		rnk_worker_time				int,
		pctallplans_worker_time		decimal(5,2),
		rnk_physical_reads			int,
		pctallplans_phys_reads		decimal(5,2),
		rnk_logical_writes			int,
		pctallplans_logical_writes	decimal(5,2),
		rnk_logical_reads			int,
		pctallplans_logical_reads	decimal(5,2),
		rnk_elapsed_time			int,
		pctallplans_elapsed_time	decimal(5,2)
	);

	CREATE TABLE #QueryPatternDelta (
		--key fields
		[cacheobjtype]				[nvarchar](50) NOT NULL,
		[objtype]					[nvarchar](20) NOT NULL,
		[query_hash]				[binary](8) NOT NULL,

		--measures we're after
		[NumEntries]				[int] NOT NULL,
		[NumEntries_delta]			[int] NOT NULL,
		[size_in_bytes]				[bigint] NOT NULL,
		[size_in_bytes_delta]		[bigint] NOT NULL, 
		[total_rows]				[bigint] NULL,
		[total_rows_delta]			[bigint] NULL,
		[plan_generation_num]		[bigint] NULL,
		[plan_generation_num_delta] [bigint] NULL,
		[refcounts]					[bigint] NULL,
		[usecounts]					[bigint] NULL,
		[usecounts_delta]			[bigint] NULL,
		[execution_count]			[bigint] NOT NULL,
		[execution_count_delta]		[bigint] NOT NULL,
		[total_worker_time]			[bigint] NOT NULL,
		[total_worker_time_delta]	[bigint] NOT NULL,
		[total_physical_reads]		[bigint] NOT NULL,
		[total_physical_reads_delta] [bigint] NOT NULL,
		[total_logical_writes]		[bigint] NOT NULL,
		[total_logical_writes_delta] [bigint] NOT NULL,
		[total_logical_reads]		[bigint] NOT NULL,
		[total_logical_reads_delta] [bigint] NOT NULL,
		[total_clr_time]			[bigint] NOT NULL,
		[total_clr_time_delta]		[bigint] NOT NULL,
		[total_elapsed_time]		[bigint] NOT NULL,
		[total_elapsed_time_delta] [bigint] NOT NULL,

		rnk_total_rows				int, 
		rnk_worker_time				int,
		pctallplans_worker_time		decimal(5,2),
		rnk_physical_reads			int,
		pctallplans_phys_reads		decimal(5,2),
		rnk_logical_writes			int,
		pctallplans_logical_writes	decimal(5,2),
		rnk_logical_reads			int,
		pctallplans_logical_reads	decimal(5,2),
		rnk_elapsed_time			int,
		pctallplans_elapsed_time	decimal(5,2),
		rnk_NumEntries				int,
		pctallplans_NumEntries		decimal(5,2)
	);

	CREATE TABLE #TopPatterns_RepresentativeStmts (
		[cacheobjtype] nvarchar(50) NOT NULL, 
		[objtype] nvarchar(20) NOT NULL, 
		[query_hash] binary(8) NOT NULL,
		[sql_handle] [varbinary](64) NULL,
		[statement_start_offset] [int] NOT NULL,
		[statement_end_offset] [int] NOT NULL,
		[plan_handle] [varbinary](64) NULL,
		[plan_generation_num] [bigint] NULL,
		[plan_generation_num_delta] [bigint] NULL,
		[creation_time] [datetime] NULL,
		[last_execution_time] [datetime] NULL,
		[execution_count] [bigint] NOT NULL,
		[execution_count_delta] [bigint] NOT NULL,
		[total_worker_time] [bigint] NOT NULL,
		[total_worker_time_delta] [bigint] NOT NULL,
		[last_worker_time] [bigint] NOT NULL,
		[min_worker_time] [bigint] NOT NULL,
		[max_worker_time] [bigint] NOT NULL,
		[total_physical_reads] [bigint] NOT NULL,
		[total_physical_reads_delta] [bigint] NOT NULL,
		[last_physical_reads] [bigint] NOT NULL,
		[min_physical_reads] [bigint] NOT NULL,
		[max_physical_reads] [bigint] NOT NULL,
		[total_logical_writes] [bigint] NOT NULL,
		[total_logical_writes_delta] [bigint] NOT NULL,
		[last_logical_writes] [bigint] NOT NULL,
		[min_logical_writes] [bigint] NOT NULL,
		[max_logical_writes] [bigint] NOT NULL,
		[total_logical_reads] [bigint] NOT NULL,
		[total_logical_reads_delta] [bigint] NOT NULL,
		[last_logical_reads] [bigint] NOT NULL,
		[min_logical_reads] [bigint] NOT NULL,
		[max_logical_reads] [bigint] NOT NULL,
		[total_clr_time] [bigint] NOT NULL,
		[total_clr_time_delta] [bigint] NOT NULL,
		[last_clr_time] [bigint] NOT NULL,
		[min_clr_time] [bigint] NOT NULL,
		[max_clr_time] [bigint] NOT NULL,
		[total_elapsed_time] [bigint] NOT NULL,
		[total_elapsed_time_delta] [bigint] NOT NULL,
		[last_elapsed_time] [bigint] NOT NULL,
		[min_elapsed_time] [bigint] NOT NULL,
		[max_elapsed_time] [bigint] NOT NULL,
		[query_plan_hash] [binary](8) NULL,
		[total_rows] [bigint] NULL,
		[total_rows_delta] [bigint] NULL,
		[last_rows] [bigint] NULL,
		[min_rows] [bigint] NULL,
		[max_rows] [bigint] NULL,
		[refcounts] [int] NULL,
		[usecounts] [int] NULL,
		[size_in_bytes] [int] NULL, 
		[pool_id] [int] NULL, 
		[parent_plan_handle] [varbinary](64),
		--keys to the stores
		FKSQLStmtStoreID BIGINT NULL,
		FKQueryPlanStmtStoreID BIGINT NULL
	);

	CREATE TABLE #TopObjects_StmtStats (
		[database_id] [int] NOT NULL,
		[object_id] [int] NOT NULL,
		[type] [char](2) NULL,
		[sql_handle] [varbinary](64) NULL,
		[plan_handle] [varbinary](64) NULL,
		[statement_start_offset] [int] NOT NULL,
		[statement_end_offset] [int] NOT NULL,
		[plan_generation_num] [bigint] NULL,
		[plan_generation_num_delta] [bigint] NULL,
		[creation_time] [datetime] NULL,
		[last_execution_time] [datetime] NULL,
		[execution_count] [bigint] NOT NULL,
		[execution_count_delta] [bigint] NOT NULL,
		[total_worker_time] [bigint] NOT NULL,
		[total_worker_time_delta] [bigint] NOT NULL,
		[last_worker_time] [bigint] NOT NULL,
		[min_worker_time] [bigint] NOT NULL,
		[max_worker_time] [bigint] NOT NULL,
		[total_physical_reads] [bigint] NOT NULL,
		[total_physical_reads_delta] [bigint] NOT NULL,
		[last_physical_reads] [bigint] NOT NULL,
		[min_physical_reads] [bigint] NOT NULL,
		[max_physical_reads] [bigint] NOT NULL,
		[total_logical_writes] [bigint] NOT NULL,
		[total_logical_writes_delta] [bigint] NOT NULL,
		[last_logical_writes] [bigint] NOT NULL,
		[min_logical_writes] [bigint] NOT NULL,
		[max_logical_writes] [bigint] NOT NULL,
		[total_logical_reads] [bigint] NOT NULL,
		[total_logical_reads_delta] [bigint] NOT NULL,
		[last_logical_reads] [bigint] NOT NULL,
		[min_logical_reads] [bigint] NOT NULL,
		[max_logical_reads] [bigint] NOT NULL,
		[total_clr_time] [bigint] NOT NULL,
		[total_clr_time_delta] [bigint] NOT NULL,
		[last_clr_time] [bigint] NOT NULL,
		[min_clr_time] [bigint] NOT NULL,
		[max_clr_time] [bigint] NOT NULL,
		[total_elapsed_time] [bigint] NOT NULL,
		[total_elapsed_time_delta] [bigint] NOT NULL,
		[last_elapsed_time] [bigint] NOT NULL,
		[min_elapsed_time] [bigint] NOT NULL,
		[max_elapsed_time] [bigint] NOT NULL,
		[query_hash] [binary](8) NULL,
		[query_plan_hash] [binary](8) NULL,
		[total_rows] [bigint] NULL,
		[total_rows_delta] [bigint] NULL,
		[last_rows] [bigint] NULL,
		[min_rows] [bigint] NULL,
		[max_rows] [bigint] NULL,

		[pct_worker_time] DECIMAL(5,2) NULL, 
		[pct_phys_reads] DECIMAL(5,2) NULL, 
		[pct_logical_writes] DECIMAL(5,2) NULL, 
		[pct_logical_reads] DECIMAL(5,2) NULL, 
		[pct_elapsed_time] DECIMAL(5,2) NULL,

		--keys to the stores
		FKSQLStmtStoreID BIGINT NULL,
		FKQueryPlanStmtStoreID BIGINT NULL
	);

	CREATE TABLE #t__stmt (
		[sql_handle]				[varbinary](64)		NOT NULL,
		[statement_start_offset]	[int]				NOT NULL,
		[statement_end_offset]		[int]				NOT NULL,
		[dbid]						[smallint]			NOT NULL,
		[objectid]					[int]				NOT NULL,
		[fail_to_obtain]			[bit]				NOT NULL, 
		[datalen_batch]				[int]				NOT NULL,
		[stmt_text]					[nvarchar](max)		NOT NULL
	);

	CREATE TABLE #t__stmtqp (
		[plan_handle]				[varbinary](64)		NOT NULL, 
		[statement_start_offset]	[int]				NOT NULL,
		[statement_end_offset]		[int]				NOT NULL,
		[dbid]						[smallint]			NOT NULL,
		[objectid]					[int]				NOT NULL,
		[fail_to_obtain]			[bit]				NOT NULL, 
		[query_plan]				[nvarchar](max)		NOT NULL,
		[aw_stmtplan_hash]			[varbinary](64)		NOT NULL,
		[PKQueryPlanStmtStoreID]	[bigint]			NULL
	);

	--Because we're switching between A & B every time, we definitely want to wrap things in a transaction
	SET @errorloc = N'Beginning Transaction';
	BEGIN TRANSACTION

	IF @lv__CurrentTable = N'A'
	BEGIN
		--just for safety (these tables should have been truncated at the end of the previous run:
		IF EXISTS (SELECT * FROM ServerEye.dm_exec_object_stats__A)
		BEGIN
			TRUNCATE TABLE ServerEye.dm_exec_object_stats__A;
		END

		IF EXISTS (SELECT * FROM ServerEye.dm_exec_query_stats__A)
		BEGIN
			TRUNCATE TABLE ServerEye.dm_exec_query_stats__A;
		END

		IF EXISTS (SELECT * FROM ServerEye.QueryPatternStats__A)
		BEGIN
			TRUNCATE TABLE ServerEye.QueryPatternStats__A;
		END

		SET @errorloc = N'A objstats insert';
		INSERT INTO ServerEye.dm_exec_object_stats__A (
			database_id, 
			object_id, 
			type, 
			sql_handle, 
			plan_handle, 
			cached_time, 
			last_execution_time, 
			execution_count, 
			total_worker_time, 
			last_worker_time, 
			min_worker_time, 
			max_worker_time, 
			total_physical_reads, 
			last_physical_reads, 
			min_physical_reads, 
			max_physical_reads, 
			total_logical_writes, 
			last_logical_writes, 
			min_logical_writes, 
			max_logical_writes, 
			total_logical_reads, 
			last_logical_reads, 
			min_logical_reads, 
			max_logical_reads, 
			total_elapsed_time, 
			last_elapsed_time, 
			min_elapsed_time, 
			max_elapsed_time
		)
		SELECT 
			ps.database_id, 
			ps.object_id, 
			ps.type, 
			ps.sql_handle, 
			ps.plan_handle, 
			ps.cached_time, 
			ps.last_execution_time,
			ps.execution_count,
			ps.total_worker_time,
			ps.last_worker_time,
			ps.min_worker_time,
			ps.max_worker_time,
			ps.total_physical_reads,
			ps.last_physical_reads,
			ps.min_physical_reads,
			ps.max_physical_reads,
			ps.total_logical_writes,
			ps.last_logical_writes,
			ps.min_logical_writes,
			ps.max_logical_writes,
			ps.total_logical_reads,
			ps.last_logical_reads,
			ps.min_logical_reads,
			ps.max_logical_reads,
			ps.total_elapsed_time,
			ps.last_elapsed_time,
			ps.min_elapsed_time,
			ps.max_elapsed_time
		FROM sys.dm_exec_procedure_stats ps
			UNION ALL 
		SELECT 
			ts.database_id, 
			ts.object_id, 
			ts.type, 
			ts.sql_handle, 
			ts.plan_handle, 
			ts.cached_time, 
			ts.last_execution_time,
			ts.execution_count,
			ts.total_worker_time,
			ts.last_worker_time,
			ts.min_worker_time,
			ts.max_worker_time,
			ts.total_physical_reads,
			ts.last_physical_reads,
			ts.min_physical_reads,
			ts.max_physical_reads,
			ts.total_logical_writes,
			ts.last_logical_writes,
			ts.min_logical_writes,
			ts.max_logical_writes,
			ts.total_logical_reads,
			ts.last_logical_reads,
			ts.min_logical_reads,
			ts.max_logical_reads,
			ts.total_elapsed_time,
			ts.last_elapsed_time,
			ts.min_elapsed_time,
			ts.max_elapsed_time
		FROM sys.dm_exec_trigger_stats ts
		;

		SET @errorloc = N'A #ObjectStatDelta insert';
		INSERT INTO #ObjectStatDelta (
			[database_id],
			[object_id],
			[type],
			[sql_handle],
			[plan_handle],
			[cached_time],
			[last_execution_time],
			[execution_count],
			[execution_count_delta],
			[total_worker_time],
			[total_worker_time_delta],
			[last_worker_time],
			[min_worker_time],
			[max_worker_time],
			[total_physical_reads],
			[total_physical_reads_delta],
			[last_physical_reads],
			[min_physical_reads],
			[max_physical_reads],
			[total_logical_writes],
			[total_logical_writes_delta],
			[last_logical_writes],
			[min_logical_writes],
			[max_logical_writes],
			[total_logical_reads],
			[total_logical_reads_delta],
			[last_logical_reads],
			[min_logical_reads],
			[max_logical_reads],
			[total_elapsed_time],
			[total_elapsed_time_delta],
			[last_elapsed_time],
			[min_elapsed_time],
			[max_elapsed_time]
		)
		SELECT 
			a.database_id, 
			a.object_id,
			a.type, 
			a.sql_handle,
			a.plan_handle,
			a.cached_time,
			a.last_execution_time,
			a.execution_count,
			[execution_count_delta] = CASE WHEN a.execution_count - ISNULL(b.execution_count,0) < 0 THEN a.execution_count
										ELSE a.execution_count - ISNULL(b.execution_count,0) END,
			a.total_worker_time,
			total_worker_time_delta = CASE WHEN a.total_worker_time - ISNULL(b.total_worker_time,0) < 0 THEN a.total_worker_time
										ELSE a.total_worker_time - ISNULL(b.total_worker_time,0) END,
			a.last_worker_time,
			[min_worker_time] = CASE WHEN ISNULL(b.min_worker_time,a.min_worker_time) < a.min_worker_time 
									THEN b.min_worker_time
									ELSE a.min_worker_time
									END,
			[max_worker_time] = CASE WHEN ISNULL(b.max_worker_time,a.max_worker_time) > a.max_worker_time
									THEN b.max_worker_time
									ELSE a.max_worker_time
									END,
			a.total_physical_reads,
			[total_physical_reads_delta] = CASE WHEN a.total_physical_reads - ISNULL(b.total_physical_reads,0) < 0 THEN a.total_physical_reads
												ELSE a.total_physical_reads - ISNULL(b.total_physical_reads,0) END,
			a.last_physical_reads,
			[min_physical_reads] = CASE WHEN ISNULL(b.min_physical_reads,a.min_physical_reads) < a.min_physical_reads
									THEN b.min_physical_reads
									ELSE a.min_physical_reads
									END,
			[max_physical_reads] = CASE WHEN ISNULL(b.max_physical_reads,a.max_physical_reads) > a.max_physical_reads
									THEN b.max_physical_reads
									ELSE a.max_physical_reads
									END,
			a.total_logical_writes,
			[total_logical_writes_delta] = CASE WHEN a.total_logical_writes - ISNULL(b.total_logical_writes,0) < 0 THEN a.total_logical_writes
											ELSE a.total_logical_writes - ISNULL(b.total_logical_writes,0) END,
			a.last_logical_writes,
			[min_logical_writes] = CASE WHEN ISNULL(b.min_logical_writes,a.min_logical_writes) < a.min_logical_writes
									THEN b.min_logical_writes
									ELSE a.min_logical_writes
									END,
			[max_logical_writes] = CASE WHEN ISNULL(b.max_logical_writes,a.max_logical_writes) > a.max_logical_writes
									THEN b.max_logical_writes
									ELSE a.max_logical_writes
									END,
			a.total_logical_reads,
			[total_logical_reads_delta] = CASE WHEN a.total_logical_reads - ISNULL(b.total_logical_reads,0) < 0 THEN a.total_logical_reads
											ELSE a.total_logical_reads - ISNULL(b.total_logical_reads,0) END,
			a.last_logical_reads,
			[min_logical_reads] = CASE WHEN ISNULL(b.min_logical_reads,a.min_logical_reads) < a.min_logical_reads
									THEN b.min_logical_reads
									ELSE a.min_logical_reads
									END,
			[max_logical_reads] = CASE WHEN ISNULL(b.max_logical_reads,a.max_logical_reads) > a.max_logical_reads
									THEN b.max_logical_reads
									ELSE a.max_logical_reads
									END,
			a.total_elapsed_time,
			[total_elapsed_time_delta] = CASE WHEN a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) < 0 THEN a.total_elapsed_time
											ELSE a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) END,
			a.last_elapsed_time,
			[min_elapsed_time] = CASE WHEN ISNULL(b.min_elapsed_time,a.min_elapsed_time) < a.min_elapsed_time
									THEN b.min_elapsed_time
									ELSE a.min_elapsed_time
									END,
			[max_elapsed_time] = CASE WHEN ISNULL(b.max_elapsed_time,a.max_elapsed_time) > a.max_elapsed_time
									THEN b.max_elapsed_time
									ELSE a.max_elapsed_time
									END
		FROM ServerEye.dm_exec_object_stats__A a
			LEFT OUTER JOIN ServerEye.dm_exec_object_stats__B b
				ON a.database_id = b.database_id
				AND a.object_id = b.object_id
				AND a.type = b.type
		;

		SET @errorloc = N'A querystats insert';
		INSERT INTO ServerEye.dm_exec_query_stats__A (
			[sql_handle],
			statement_start_offset,
			statement_end_offset,
			plan_generation_num,
			plan_handle,
			creation_time,
			last_execution_time,
			execution_count,
			total_worker_time,
			last_worker_time,
			min_worker_time,
			max_worker_time,
			total_physical_reads,
			last_physical_reads,
			min_physical_reads,
			max_physical_reads,
			total_logical_writes,
			last_logical_writes,
			min_logical_writes,
			max_logical_writes,
			total_logical_reads,
			last_logical_reads,
			min_logical_reads,
			max_logical_reads,
			total_clr_time,
			last_clr_time,
			min_clr_time,
			max_clr_time,
			total_elapsed_time,
			last_elapsed_time,
			min_elapsed_time,
			max_elapsed_time,
			query_hash,
			query_plan_hash,
			total_rows, 
			last_rows,
			min_rows,
			max_rows,
			bucketid,
			refcounts, 
			usecounts,
			size_in_bytes,
			cacheobjtype,
			objtype,
			pool_id,
			parent_plan_handle
		)
		SELECT 
			qs.[sql_handle],
			qs.statement_start_offset,
			qs.statement_end_offset,
			qs.plan_generation_num,
			qs.plan_handle,
			qs.creation_time,
			qs.last_execution_time,
			qs.execution_count,
			qs.total_worker_time,
			qs.last_worker_time,
			qs.min_worker_time,
			qs.max_worker_time,
			qs.total_physical_reads,
			qs.last_physical_reads,
			qs.min_physical_reads,
			qs.max_physical_reads,
			qs.total_logical_writes,
			qs.last_logical_writes,
			qs.min_logical_writes,
			qs.max_logical_writes,
			qs.total_logical_reads,
			qs.last_logical_reads,
			qs.min_logical_reads,
			qs.max_logical_reads,
			qs.total_clr_time,
			qs.last_clr_time,
			qs.min_clr_time,
			qs.max_clr_time,
			qs.total_elapsed_time,
			qs.last_elapsed_time,
			qs.min_elapsed_time,
			qs.max_elapsed_time,
			qs.query_hash,
			qs.query_plan_hash,
			qs.total_rows, 
			qs.last_rows,
			qs.min_rows,
			qs.max_rows,
			cp.bucketid,
			cp.refcounts, 
			cp.usecounts,
			cp.size_in_bytes,
			cp.cacheobjtype,
			cp.objtype,
			cp.pool_id,
			cp.parent_plan_handle
		FROM sys.dm_exec_query_stats qs
			left outer join sys.dm_exec_cached_plans cp
				on qs.plan_handle = cp.plan_handle
		;

		SET @errorloc = N'A querypattern insert';
		INSERT INTO ServerEye.QueryPatternStats__A (
			cacheobjtype, 
			objtype, 
			query_hash, 
			NumEntries,
			size_in_bytes, 
			total_rows, 
			plan_generation_num, 
			refcounts, 
			usecounts, 
			execution_count, 
			total_worker_time, 
			total_physical_reads, 
			total_logical_writes, 
			total_logical_reads, 
			total_clr_time, 
			total_elapsed_time
		)
		SELECT 
			a.cacheobjtype,
			a.objtype,
			a.query_hash,
			[NumEntries] = SUM(1),
			SUM(a.size_in_bytes),
			SUM(a.total_rows),
			SUM(a.plan_generation_num),
			SUM(a.refcounts),
			SUM(a.usecounts),
			SUM(a.execution_count),
			SUM(a.total_worker_time),
			SUM(a.total_physical_reads),
			SUM(a.total_logical_writes),
			SUM(a.total_logical_reads),
			SUM(a.total_clr_time),
			SUM(a.total_elapsed_time)
		FROM ServerEye.dm_exec_query_stats__A a
		WHERE ISNULL(a.objtype,N'') NOT IN (N'Proc', N'Trigger')
		GROUP BY a.cacheobjtype, 
			a.objtype,
			a.query_hash
		;

		SET @errorloc = N'A #QueryPattern insert';
		INSERT INTO #QueryPatternDelta (
			[cacheobjtype],		--1
			[objtype],
			[query_hash],
			[NumEntries],
			[NumEntries_delta],		--5
			[size_in_bytes],
			[size_in_bytes_delta],
			[total_rows],
			[total_rows_delta],
			[plan_generation_num],		--10
			[plan_generation_num_delta],
			[refcounts],
			[usecounts],
			[usecounts_delta],
			[execution_count],			--15
			[execution_count_delta],
			[total_worker_time],
			[total_worker_time_delta],
			[total_physical_reads],
			[total_physical_reads_delta],	--20
			[total_logical_writes],
			[total_logical_writes_delta],
			[total_logical_reads],
			[total_logical_reads_delta],
			[total_clr_time],				--25
			[total_clr_time_delta],
			[total_elapsed_time],
			[total_elapsed_time_delta]	--28
		)
		SELECT 
			a.cacheobjtype,		--1
			a.objtype,
			a.query_hash,
			a.NumEntries,
			NumEntries_delta = a.NumEntries - ISNULL(b.NumEntries,0),	--5
			a.size_in_bytes,
			size_in_bytes_delta = a.size_in_bytes - ISNULL(b.size_in_bytes,0), 
			a.total_rows, 
			total_rows_delta = CASE WHEN a.total_rows - ISNULL(b.total_rows,0) < 0 THEN a.total_rows 
									ELSE a.total_rows - ISNULL(b.total_rows,0) END,
			a.plan_generation_num,		--10
			plan_generation_num_delta = a.plan_generation_num - ISNULL(b.plan_generation_num,0),
			a.refcounts,
			a.usecounts,
			usecounts_delta = CASE WHEN a.usecounts - ISNULL(b.usecounts,0) < 0 THEN a.usecounts
									ELSE a.usecounts - ISNULL(b.usecounts,0) END,
			a.execution_count,			--15
			execution_count_delta = CASE WHEN a.execution_count - ISNULL(b.execution_count,0) < 0 THEN a.execution_count
									ELSE a.execution_count - ISNULL(b.execution_count,0) END ,
			a.total_worker_time,
			total_worker_time_delta = CASE WHEN a.total_worker_time - ISNULL(b.total_worker_time,0) < 0 THEN a.total_worker_time
										ELSE a.total_worker_time - ISNULL(b.total_worker_time,0) END,
			a.total_physical_reads, 
			total_physical_reads_delta = CASE WHEN a.total_physical_reads - ISNULL(b.total_physical_reads,0) < 0 THEN a.total_physical_reads
											ELSE a.total_physical_reads - ISNULL(b.total_physical_reads,0) END,		--20
			a.total_logical_writes,
			total_logical_writes_delta = CASE WHEN a.total_logical_writes - ISNULL(b.total_logical_writes,0) < 0 THEN a.total_logical_writes
											ELSE a.total_logical_writes - ISNULL(b.total_logical_writes,0) END,
			a.total_logical_reads,
			total_logical_reads_delta = CASE WHEN a.total_logical_reads - ISNULL(b.total_logical_reads,0) < 0 THEN a.total_logical_reads
											ELSE a.total_logical_reads - ISNULL(b.total_logical_reads,0) END,
			a.total_clr_time,				--25
			total_clr_time_delta = CASE WHEN a.total_clr_time - ISNULL(b.total_clr_time,0) < 0 THEN a.total_clr_time
										ELSE a.total_clr_time - ISNULL(b.total_clr_time,0) END,
			a.total_elapsed_time,
			total_elapsed_time_delta = CASE WHEN a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) < 0 THEN a.total_elapsed_time
										ELSE a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) END
		FROM ServerEye.QueryPatternStats__A a
			LEFT OUTER JOIN ServerEye.QueryPatternStats__B b
				ON a.cacheobjtype = b.cacheobjtype
				AND a.objtype = b.objtype
				AND a.query_hash = b.query_hash
		;

	END	--end of first half of massive A/B block
	ELSE
	BEGIN
		--beginning of second-half of massive A/B block
		--just for safety (these tables should have been truncated at the end of the previous run:
		IF EXISTS (SELECT * FROM ServerEye.dm_exec_object_stats__B)
		BEGIN
			TRUNCATE TABLE ServerEye.dm_exec_object_stats__B;
		END

		IF EXISTS (SELECT * FROM ServerEye.dm_exec_query_stats__B)
		BEGIN
			TRUNCATE TABLE ServerEye.dm_exec_query_stats__B;
		END

		IF EXISTS (SELECT * FROM ServerEye.QueryPatternStats__B)
		BEGIN
			TRUNCATE TABLE ServerEye.QueryPatternStats__B;
		END

		SET @errorloc = N'B objstats insert';
		INSERT INTO ServerEye.dm_exec_object_stats__B (
			database_id, 
			object_id, 
			type, 
			sql_handle, 
			plan_handle, 
			cached_time, 
			last_execution_time, 
			execution_count, 
			total_worker_time, 
			last_worker_time, 
			min_worker_time, 
			max_worker_time, 
			total_physical_reads, 
			last_physical_reads, 
			min_physical_reads, 
			max_physical_reads, 
			total_logical_writes, 
			last_logical_writes, 
			min_logical_writes, 
			max_logical_writes, 
			total_logical_reads, 
			last_logical_reads, 
			min_logical_reads, 
			max_logical_reads, 
			total_elapsed_time, 
			last_elapsed_time, 
			min_elapsed_time, 
			max_elapsed_time
		)
		SELECT 
			ps.database_id, 
			ps.object_id, 
			ps.type, 
			ps.sql_handle, 
			ps.plan_handle, 
			ps.cached_time, 
			ps.last_execution_time,
			ps.execution_count,
			ps.total_worker_time,
			ps.last_worker_time,
			ps.min_worker_time,
			ps.max_worker_time,
			ps.total_physical_reads,
			ps.last_physical_reads,
			ps.min_physical_reads,
			ps.max_physical_reads,
			ps.total_logical_writes,
			ps.last_logical_writes,
			ps.min_logical_writes,
			ps.max_logical_writes,
			ps.total_logical_reads,
			ps.last_logical_reads,
			ps.min_logical_reads,
			ps.max_logical_reads,
			ps.total_elapsed_time,
			ps.last_elapsed_time,
			ps.min_elapsed_time,
			ps.max_elapsed_time
		FROM sys.dm_exec_procedure_stats ps
			UNION ALL 
		SELECT 
			ts.database_id, 
			ts.object_id, 
			ts.type, 
			ts.sql_handle, 
			ts.plan_handle, 
			ts.cached_time, 
			ts.last_execution_time,
			ts.execution_count,
			ts.total_worker_time,
			ts.last_worker_time,
			ts.min_worker_time,
			ts.max_worker_time,
			ts.total_physical_reads,
			ts.last_physical_reads,
			ts.min_physical_reads,
			ts.max_physical_reads,
			ts.total_logical_writes,
			ts.last_logical_writes,
			ts.min_logical_writes,
			ts.max_logical_writes,
			ts.total_logical_reads,
			ts.last_logical_reads,
			ts.min_logical_reads,
			ts.max_logical_reads,
			ts.total_elapsed_time,
			ts.last_elapsed_time,
			ts.min_elapsed_time,
			ts.max_elapsed_time
		FROM sys.dm_exec_trigger_stats ts
		;

		SET @errorloc = N'B #ObjectStatDelta insert';
		INSERT INTO #ObjectStatDelta (
			[database_id],
			[object_id],
			[type],
			[sql_handle],
			[plan_handle],
			[cached_time],
			[last_execution_time],
			[execution_count],
			[execution_count_delta],
			[total_worker_time],
			[total_worker_time_delta],
			[last_worker_time],
			[min_worker_time],
			[max_worker_time],
			[total_physical_reads],
			[total_physical_reads_delta],
			[last_physical_reads],
			[min_physical_reads],
			[max_physical_reads],
			[total_logical_writes],
			[total_logical_writes_delta],
			[last_logical_writes],
			[min_logical_writes],
			[max_logical_writes],
			[total_logical_reads],
			[total_logical_reads_delta],
			[last_logical_reads],
			[min_logical_reads],
			[max_logical_reads],
			[total_elapsed_time],
			[total_elapsed_time_delta],
			[last_elapsed_time],
			[min_elapsed_time],
			[max_elapsed_time]
		)
		SELECT 
			a.database_id, 
			a.object_id,
			a.type, 
			a.sql_handle,
			a.plan_handle,
			a.cached_time,
			a.last_execution_time,
			a.execution_count,
			[execution_count_delta] = CASE WHEN a.execution_count - ISNULL(b.execution_count,0) < 0 THEN a.execution_count
										ELSE a.execution_count - ISNULL(b.execution_count,0) END,
			a.total_worker_time,
			total_worker_time_delta = CASE WHEN a.total_worker_time - ISNULL(b.total_worker_time,0) < 0 THEN a.total_worker_time
										ELSE a.total_worker_time - ISNULL(b.total_worker_time,0) END,
			a.last_worker_time,
			[min_worker_time] = CASE WHEN ISNULL(b.min_worker_time,a.min_worker_time) < a.min_worker_time 
									THEN b.min_worker_time
									ELSE a.min_worker_time
									END,
			[max_worker_time] = CASE WHEN ISNULL(b.max_worker_time,a.max_worker_time) > a.max_worker_time
									THEN b.max_worker_time
									ELSE a.max_worker_time
									END,
			a.total_physical_reads,
			[total_physical_reads_delta] = CASE WHEN a.total_physical_reads - ISNULL(b.total_physical_reads,0) < 0 THEN a.total_physical_reads
												ELSE a.total_physical_reads - ISNULL(b.total_physical_reads,0) END,
			a.last_physical_reads,
			[min_physical_reads] = CASE WHEN ISNULL(b.min_physical_reads,a.min_physical_reads) < a.min_physical_reads
									THEN b.min_physical_reads
									ELSE a.min_physical_reads
									END,
			[max_physical_reads] = CASE WHEN ISNULL(b.max_physical_reads,a.max_physical_reads) > a.max_physical_reads
									THEN b.max_physical_reads
									ELSE a.max_physical_reads
									END,
			a.total_logical_writes,
			[total_logical_writes_delta] = CASE WHEN a.total_logical_writes - ISNULL(b.total_logical_writes,0) < 0 THEN a.total_logical_writes
											ELSE a.total_logical_writes - ISNULL(b.total_logical_writes,0) END,
			a.last_logical_writes,
			[min_logical_writes] = CASE WHEN ISNULL(b.min_logical_writes,a.min_logical_writes) < a.min_logical_writes
									THEN b.min_logical_writes
									ELSE a.min_logical_writes
									END,
			[max_logical_writes] = CASE WHEN ISNULL(b.max_logical_writes,a.max_logical_writes) > a.max_logical_writes
									THEN b.max_logical_writes
									ELSE a.max_logical_writes
									END,
			a.total_logical_reads,
			[total_logical_reads_delta] = CASE WHEN a.total_logical_reads - ISNULL(b.total_logical_reads,0) < 0 THEN a.total_logical_reads
											ELSE a.total_logical_reads - ISNULL(b.total_logical_reads,0) END,
			a.last_logical_reads,
			[min_logical_reads] = CASE WHEN ISNULL(b.min_logical_reads,a.min_logical_reads) < a.min_logical_reads
									THEN b.min_logical_reads
									ELSE a.min_logical_reads
									END,
			[max_logical_reads] = CASE WHEN ISNULL(b.max_logical_reads,a.max_logical_reads) > a.max_logical_reads
									THEN b.max_logical_reads
									ELSE a.max_logical_reads
									END,
			a.total_elapsed_time,
			[total_elapsed_time_delta] = CASE WHEN a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) < 0 THEN a.total_elapsed_time
											ELSE a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) END,
			a.last_elapsed_time,
			[min_elapsed_time] = CASE WHEN ISNULL(b.min_elapsed_time,a.min_elapsed_time) < a.min_elapsed_time
									THEN b.min_elapsed_time
									ELSE a.min_elapsed_time
									END,
			[max_elapsed_time] = CASE WHEN ISNULL(b.max_elapsed_time,a.max_elapsed_time) > a.max_elapsed_time
									THEN b.max_elapsed_time
									ELSE a.max_elapsed_time
									END

			--the FROM has been copied from the first half of the A/B block, with the A & B tables reversed relative
			-- to the LEFT OUTER JOIN. However, the aliases have been switched (such that "a" remains on the LEFT-hand side)
			-- so that all of the above column logic can stay as-is.
		FROM ServerEye.dm_exec_object_stats__B a
			LEFT OUTER JOIN ServerEye.dm_exec_object_stats__A b
				ON a.database_id = b.database_id
				AND a.object_id = b.object_id
				AND a.type = b.type
		;

		SET @errorloc = N'B querystats insert';
		INSERT INTO ServerEye.dm_exec_query_stats__B (
			[sql_handle],
			statement_start_offset,
			statement_end_offset,
			plan_generation_num,
			plan_handle,
			creation_time,
			last_execution_time,
			execution_count,
			total_worker_time,
			last_worker_time,
			min_worker_time,
			max_worker_time,
			total_physical_reads,
			last_physical_reads,
			min_physical_reads,
			max_physical_reads,
			total_logical_writes,
			last_logical_writes,
			min_logical_writes,
			max_logical_writes,
			total_logical_reads,
			last_logical_reads,
			min_logical_reads,
			max_logical_reads,
			total_clr_time,
			last_clr_time,
			min_clr_time,
			max_clr_time,
			total_elapsed_time,
			last_elapsed_time,
			min_elapsed_time,
			max_elapsed_time,
			query_hash,
			query_plan_hash,
			total_rows, 
			last_rows,
			min_rows,
			max_rows,
			bucketid,
			refcounts, 
			usecounts,
			size_in_bytes,
			cacheobjtype,
			objtype,
			pool_id,
			parent_plan_handle
		)
		SELECT 
			qs.[sql_handle],
			qs.statement_start_offset,
			qs.statement_end_offset,
			qs.plan_generation_num,
			qs.plan_handle,
			qs.creation_time,
			qs.last_execution_time,
			qs.execution_count,
			qs.total_worker_time,
			qs.last_worker_time,
			qs.min_worker_time,
			qs.max_worker_time,
			qs.total_physical_reads,
			qs.last_physical_reads,
			qs.min_physical_reads,
			qs.max_physical_reads,
			qs.total_logical_writes,
			qs.last_logical_writes,
			qs.min_logical_writes,
			qs.max_logical_writes,
			qs.total_logical_reads,
			qs.last_logical_reads,
			qs.min_logical_reads,
			qs.max_logical_reads,
			qs.total_clr_time,
			qs.last_clr_time,
			qs.min_clr_time,
			qs.max_clr_time,
			qs.total_elapsed_time,
			qs.last_elapsed_time,
			qs.min_elapsed_time,
			qs.max_elapsed_time,
			qs.query_hash,
			qs.query_plan_hash,
			qs.total_rows, 
			qs.last_rows,
			qs.min_rows,
			qs.max_rows,
			cp.bucketid,
			cp.refcounts, 
			cp.usecounts,
			cp.size_in_bytes,
			cp.cacheobjtype,
			cp.objtype,
			cp.pool_id,
			cp.parent_plan_handle
		FROM sys.dm_exec_query_stats qs
			left outer join sys.dm_exec_cached_plans cp
				on qs.plan_handle = cp.plan_handle
		;

		SET @errorloc = N'B querypattern insert';
		INSERT INTO ServerEye.QueryPatternStats__B (
			cacheobjtype, 
			objtype, 
			query_hash, 
			NumEntries,
			size_in_bytes, 
			total_rows, 
			plan_generation_num, 
			refcounts, 
			usecounts, 
			execution_count, 
			total_worker_time, 
			total_physical_reads, 
			total_logical_writes, 
			total_logical_reads, 
			total_clr_time, 
			total_elapsed_time
		)
		SELECT 
			a.cacheobjtype,
			a.objtype,
			a.query_hash,
			[NumEntries] = SUM(1),
			SUM(a.size_in_bytes),
			SUM(a.total_rows),
			SUM(a.plan_generation_num),
			SUM(a.refcounts),
			SUM(a.usecounts),
			SUM(a.execution_count),
			SUM(a.total_worker_time),
			SUM(a.total_physical_reads),
			SUM(a.total_logical_writes),
			SUM(a.total_logical_reads),
			SUM(a.total_clr_time),
			SUM(a.total_elapsed_time)
		FROM ServerEye.dm_exec_query_stats__B a
		WHERE ISNULL(a.objtype,N'') NOT IN (N'Proc', N'Trigger')
		GROUP BY a.cacheobjtype, 
			a.objtype,
			a.query_hash
		;

		SET @errorloc = N'B #QueryPattern insert';
		INSERT INTO #QueryPatternDelta (
			[cacheobjtype],		--1
			[objtype],
			[query_hash],
			[NumEntries],
			[NumEntries_delta],		--5
			[size_in_bytes],
			[size_in_bytes_delta],
			[total_rows],
			[total_rows_delta],
			[plan_generation_num],		--10
			[plan_generation_num_delta],
			[refcounts],
			[usecounts],
			[usecounts_delta],
			[execution_count],			--15
			[execution_count_delta],
			[total_worker_time],
			[total_worker_time_delta],
			[total_physical_reads],
			[total_physical_reads_delta],	--20
			[total_logical_writes],
			[total_logical_writes_delta],
			[total_logical_reads],
			[total_logical_reads_delta],
			[total_clr_time],				--25
			[total_clr_time_delta],
			[total_elapsed_time],
			[total_elapsed_time_delta]	--28
		)
		SELECT 
			a.cacheobjtype,		--1
			a.objtype,
			a.query_hash,
			a.NumEntries,
			NumEntries_delta = a.NumEntries - ISNULL(b.NumEntries,0),	--5
			a.size_in_bytes,
			size_in_bytes_delta = a.size_in_bytes - ISNULL(b.size_in_bytes,0), 
			a.total_rows, 
			total_rows_delta = CASE WHEN a.total_rows - ISNULL(b.total_rows,0) < 0 THEN a.total_rows 
									ELSE a.total_rows - ISNULL(b.total_rows,0) END,
			a.plan_generation_num,		--10
			plan_generation_num_delta = a.plan_generation_num - ISNULL(b.plan_generation_num,0),
			a.refcounts,
			a.usecounts,
			usecounts_delta = CASE WHEN a.usecounts - ISNULL(b.usecounts,0) < 0 THEN a.usecounts
									ELSE a.usecounts - ISNULL(b.usecounts,0) END,
			a.execution_count,			--15
			execution_count_delta = CASE WHEN a.execution_count - ISNULL(b.execution_count,0) < 0 THEN a.execution_count
									ELSE a.execution_count - ISNULL(b.execution_count,0) END ,
			a.total_worker_time,
			total_worker_time_delta = CASE WHEN a.total_worker_time - ISNULL(b.total_worker_time,0) < 0 THEN a.total_worker_time
										ELSE a.total_worker_time - ISNULL(b.total_worker_time,0) END,
			a.total_physical_reads, 
			total_physical_reads_delta = CASE WHEN a.total_physical_reads - ISNULL(b.total_physical_reads,0) < 0 THEN a.total_physical_reads
											ELSE a.total_physical_reads - ISNULL(b.total_physical_reads,0) END,		--20
			a.total_logical_writes,
			total_logical_writes_delta = CASE WHEN a.total_logical_writes - ISNULL(b.total_logical_writes,0) < 0 THEN a.total_logical_writes
											ELSE a.total_logical_writes - ISNULL(b.total_logical_writes,0) END,
			a.total_logical_reads,
			total_logical_reads_delta = CASE WHEN a.total_logical_reads - ISNULL(b.total_logical_reads,0) < 0 THEN a.total_logical_reads
											ELSE a.total_logical_reads - ISNULL(b.total_logical_reads,0) END,
			a.total_clr_time,				--25
			total_clr_time_delta = CASE WHEN a.total_clr_time - ISNULL(b.total_clr_time,0) < 0 THEN a.total_clr_time
										ELSE a.total_clr_time - ISNULL(b.total_clr_time,0) END,
			a.total_elapsed_time,
			total_elapsed_time_delta = CASE WHEN a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) < 0 THEN a.total_elapsed_time
										ELSE a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) END

			--the FROM has been copied from the first half of the A/B block, with the A & B tables reversed relative
			-- to the LEFT OUTER JOIN. However, the aliases have been switched (such that "a" remains on the LEFT-hand side)
			-- so that all of the above column logic can stay as-is.
		FROM ServerEye.QueryPatternStats__B a
			LEFT OUTER JOIN ServerEye.QueryPatternStats__A b
				ON a.cacheobjtype = b.cacheobjtype
				AND a.objtype = b.objtype
				AND a.query_hash = b.query_hash
		;

	END		--end of massive A/B block

	DECLARE @SumWorkerTime BIGINT, 
			@SumPhysReads BIGINT,
			@SumLogicalReads BIGINT,
			@SumLogicalWrites BIGINT,
			@SumElapsedTime BIGINT,
			@SumNumEntries BIGINT;

	--Persist to variables so we can get the aggregates in just one pass of the data.
	SET @errorloc = N'SUM var #QueryPattern';
	SELECT @SumWorkerTime = SUM(total_worker_time_delta),
			@SumPhysReads = SUM(total_physical_reads_delta),
			@SumLogicalReads = SUM(total_logical_reads_delta),
			@SumLogicalWrites = SUM(total_logical_writes_delta),
			@SumElapsedTime = SUM(total_elapsed_time_delta),
			@SumNumEntries = SUM(NumEntries_delta)
	FROM #QueryPatternDelta t
	;
	--TODO: what if there are 0 rows in #QueryPatternDelta? Or if any of the variables are 0 or NULL?

	--Let's find the rank and the percentage of the query patterns
	SET @errorloc = N'Query Pattern Rank/Pct';
	UPDATE targ 
	SET rnk_total_rows = r.rnk_total_rows,
		rnk_worker_time = r.rnk_worker_time,
		pctallplans_worker_time = r.pctallplans_worker_time,
		rnk_physical_reads = r.rnk_physical_reads,
		pctallplans_phys_reads = r.pctallplans_physical_reads,
		rnk_logical_writes = r.rnk_logical_writes,
		pctallplans_logical_writes = r.rnk_logical_writes,
		rnk_logical_reads = r.rnk_logical_reads,
		pctallplans_logical_reads = r.pctallplans_logical_reads,
		rnk_elapsed_time = r.rnk_elapsed_time,
		pctallplans_elapsed_time = r.pctallplans_elapsed_time,
		rnk_NumEntries = r.rnk_NumEntries,
		pctallplans_NumEntries = r.pctallplans_NumEntries
	FROM #QueryPatternDelta targ 
		INNER JOIN (
			SELECT t.cacheobjtype, 
				t.objtype, 
				t.query_hash,
				pctallplans_worker_time = CONVERT(DECIMAL(5,2),
												( 1.0 * t.total_worker_time_delta) / (1.*@SumWorkerTime)
												),
				pctallplans_physical_reads = CONVERT(DECIMAL(5,2),
												( 1.0 * t.total_physical_reads_delta) / (1.*@SumPhysReads)
												),
				pctallplans_logical_writes = CONVERT(DECIMAL(5,2),
												( 1.0 * t.total_logical_writes_delta) / (1.*@SumLogicalWrites)
												),
				pctallplans_logical_reads = CONVERT(DECIMAL(5,2),
												( 1.0 * t.total_logical_reads_delta) / (1.*@SumLogicalReads)
												),
				pctallplans_elapsed_time = CONVERT(DECIMAL(5,2),
												( 1.0 * t.total_elapsed_time_delta) / 
												(1.*@SumElapsedTime)
												),
				pctallplans_NumEntries = CONVERT(DECIMAL(5,2),
												( 1.0 * t.NumEntries_delta) / (1.*@SumNumEntries)
												),

				rnk_total_rows = RANK() OVER (ORDER BY total_rows_delta desc), 
				rnk_worker_time = RANK() OVER (ORDER BY total_worker_time_delta desc), 
				rnk_physical_reads = RANK() OVER (ORDER BY total_physical_reads_delta desc), 
				rnk_logical_writes = RANK() OVER (ORDER BY total_logical_writes_delta desc), 
				rnk_logical_reads = RANK() OVER (ORDER BY total_logical_reads_delta desc), 
				rnk_elapsed_time = RANK() OVER (ORDER BY total_elapsed_time_delta desc),
				rnk_NumEntries = RANK() OVER (ORDER BY NumEntries_Delta desc)
			FROM #QueryPatternDelta t
		) r
			ON r.cacheobjtype = targ.cacheobjtype
			AND r.objtype = targ.objtype
			AND r.query_hash = targ.query_hash
		;

	SET @errorloc = N'QueryPattern final persist';
	INSERT INTO ServerEye.QueryPatternStats (
		CollectionTime,			--1
		cacheobjtype, 
		objtype, 
		query_hash, 
		NumEntries,				--5
		NumEntries_delta, 
		size_in_bytes, 
		size_in_bytes_delta, 
		total_rows, 
		total_rows_delta,			--10
		plan_generation_num, 
		plan_generation_num_delta, 
		refcounts, 
		usecounts, 
		usecounts_delta,			--15
		execution_count, 
		execution_count_delta, 
		total_worker_time, 
		total_worker_time_delta, 
		total_physical_reads,		--20
		total_physical_reads_delta, 
		total_logical_writes, 
		total_logical_writes_delta, 
		total_logical_reads, 
		total_logical_reads_delta,	--25
		total_clr_time, 
		total_clr_time_delta, 
		total_elapsed_time, 
		total_elapsed_time_delta, 
		rank_total_rows,			--30
		rank_worker_time, 
		[pctall_worker_time],
		rank_physical_reads, 
		[pctall_physical_reads],
		rank_logical_writes,		--35
		[pctall_logical_writes],
		rank_logical_reads, 
		[pctall_logical_reads],
		rank_elapsed_time,
		[pctall_elapsed_time],		--40
		rank_NumEntries,
		[pctall_NumEntries]			--42
	)
	SELECT 
		@CollectionTime,			--1
		cacheobjtype,
		objtype,
		query_hash,
		NumEntries,					--5
		NumEntries_delta,
		size_in_bytes,
		size_in_bytes_delta,
		total_rows,
		total_rows_delta,			--10
		plan_generation_num,
		plan_generation_num_delta,
		refcounts,
		usecounts,
		usecounts_delta,			--15
		execution_count,
		execution_count_delta,
		total_worker_time,
		total_worker_time_delta,
		total_physical_reads,		--20
		total_physical_reads_delta,
		total_logical_writes,
		total_logical_writes_delta,
		total_logical_reads,
		total_logical_reads_delta,	--25
		total_clr_time,
		total_clr_time_delta,
		total_elapsed_time,
		total_elapsed_time_delta,
		rnk_total_rows,				--30
		t.rnk_worker_time,
		t.pctallplans_worker_time,
		rnk_physical_reads,
		t.pctallplans_phys_reads,
		rnk_logical_writes,			--35
		t.pctallplans_logical_writes,
		rnk_logical_reads,
		t.pctallplans_logical_reads,
		rnk_elapsed_time,
		t.pctallplans_elapsed_time,	--40
		rnk_NumEntries,
		t.pctallplans_NumEntries
	FROM #QueryPatternDelta t
	WHERE
		t.rnk_total_rows <= 12
	OR t.rnk_worker_time <= 12
	OR t.rnk_physical_reads <= 12
	OR t.rnk_logical_writes <= 12
	OR t.rnk_logical_reads <= 12
	OR t.rnk_elapsed_time <= 12
	OR t.rnk_NumEntries <= 12
	;


	--Persist to variables so we can get the aggregates in just one pass of the data.
	SET @SumWorkerTime = NULL; 
	SET @SumPhysReads = NULL; 
	SET @SumLogicalWrites = NULL; 
	SET @SumLogicalReads = NULL; 
	SET @SumElapsedTime = NULL; 
	SET @SumNumEntries = NULL; 

	SET @errorloc = N'SUM var #ObjectStat';
	SELECT @SumWorkerTime = SUM(total_worker_time_delta),
			@SumPhysReads = SUM(total_physical_reads_delta),
			@SumLogicalReads = SUM(total_logical_reads_delta),
			@SumLogicalWrites = SUM(total_logical_writes_delta),
			@SumElapsedTime = SUM(total_elapsed_time_delta)
	FROM #ObjectStatDelta t
	;

	--Let's find the rank and the percentage of the object stats
	SET @errorloc = N'Object stat Rank/Pct';
	UPDATE targ 
	SET rnk_total_rows = r.rnk_total_rows,
		rnk_worker_time = r.rnk_worker_time,
		pctallplans_worker_time = r.pctallplans_worker_time,
		rnk_physical_reads = r.rnk_physical_reads,
		pctallplans_phys_reads = r.pctallplans_physical_reads,
		rnk_logical_writes = r.rnk_logical_writes,
		pctallplans_logical_writes = r.rnk_logical_writes,
		rnk_logical_reads = r.rnk_logical_reads,
		pctallplans_logical_reads = r.pctallplans_logical_reads,
		rnk_elapsed_time = r.rnk_elapsed_time,
		pctallplans_elapsed_time = r.pctallplans_elapsed_time
	FROM #ObjectStatDelta targ 
		INNER JOIN (
			SELECT t.database_id, 
				t.object_id, 
				t.[type],
				pctallplans_worker_time = CONVERT(DECIMAL(5,2),
												( 1.0 * t.total_worker_time_delta) / (1.*@SumWorkerTime)
												),
				pctallplans_physical_reads = CONVERT(DECIMAL(5,2),
												( 1.0 * t.total_physical_reads_delta) / (1.*@SumPhysReads)
												),
				pctallplans_logical_writes = CONVERT(DECIMAL(5,2),
												( 1.0 * t.total_logical_writes_delta) / (1.*@SumLogicalWrites)
												),
				pctallplans_logical_reads = CONVERT(DECIMAL(5,2),
												( 1.0 * t.total_logical_reads_delta) / (1.*@SumLogicalReads)
												),
				pctallplans_elapsed_time = CONVERT(DECIMAL(5,2),
												( 1.0 * t.total_elapsed_time_delta) / 
												(1.*@SumElapsedTime)
												),

				rnk_total_rows = RANK() OVER (ORDER BY total_rows_delta desc), 
				rnk_worker_time = RANK() OVER (ORDER BY total_worker_time_delta desc), 
				rnk_physical_reads = RANK() OVER (ORDER BY total_physical_reads_delta desc), 
				rnk_logical_writes = RANK() OVER (ORDER BY total_logical_writes_delta desc), 
				rnk_logical_reads = RANK() OVER (ORDER BY total_logical_reads_delta desc), 
				rnk_elapsed_time = RANK() OVER (ORDER BY total_elapsed_time_delta desc)
			FROM #ObjectStatDelta t
		) r
			ON r.database_id = targ.database_id
			AND r.object_id = targ.object_id
			AND r.[type] = targ.[type]
		;

	SET @errorloc = N'ObjectStat final persist';
	INSERT INTO ServerEye.ObjectStats (
		CollectionTime, 
		database_id, 
		object_id, 
		type, 
		sql_handle, 
		plan_handle, 
		cached_time, 
		last_execution_time, 
		execution_count, 
		execution_count_delta, 
		total_worker_time, 
		total_worker_time_delta, 
		last_worker_time, 
		min_worker_time, 
		max_worker_time, 
		total_physical_reads, 
		total_physical_reads_delta, 
		last_physical_reads, 
		min_physical_reads, 
		max_physical_reads, 
		total_logical_writes, 
		total_logical_writes_delta, 
		last_logical_writes, 
		min_logical_writes, 
		max_logical_writes, 
		total_logical_reads, 
		total_logical_reads_delta, 
		last_logical_reads, 
		min_logical_reads, 
		max_logical_reads, 
		total_elapsed_time, 
		total_elapsed_time_delta, 
		last_elapsed_time, 
		min_elapsed_time, 
		max_elapsed_time, 
		rank_worker_time, 
		[pctallplans_worker_time],
		rank_physical_reads, 
		[pctallplans_physical_reads],
		rank_logical_writes, 
		[pctallplans_logical_writes],
		rank_logical_reads, 
		[pctallplans_logical_reads],
		rank_elapsed_time,
		[pctallplans_elapsed_time]
	)
	SELECT 	
		@CollectionTime,
		[database_id],
		[object_id],
		[type],
		[sql_handle],
		[plan_handle],
		[cached_time],
		[last_execution_time],
		[execution_count],
		[execution_count_delta],
		[total_worker_time],
		[total_worker_time_delta],
		[last_worker_time],
		[min_worker_time],
		[max_worker_time],
		[total_physical_reads],
		[total_physical_reads_delta],
		[last_physical_reads],
		[min_physical_reads],
		[max_physical_reads],
		[total_logical_writes],
		[total_logical_writes_delta],
		[last_logical_writes],
		[min_logical_writes],
		[max_logical_writes],
		[total_logical_reads],
		[total_logical_reads_delta],
		[last_logical_reads],
		[min_logical_reads],
		[max_logical_reads],
		[total_elapsed_time],
		[total_elapsed_time_delta],
		[last_elapsed_time],
		[min_elapsed_time],
		[max_elapsed_time],
		rnk_worker_time,
		t.pctallplans_worker_time,
		rnk_physical_reads,
		t.pctallplans_phys_reads,
		rnk_logical_writes,
		t.pctallplans_logical_writes,
		rnk_logical_reads,
		t.pctallplans_logical_reads,
		rnk_elapsed_time,
		t.pctallplans_elapsed_time
	FROM #ObjectStatDelta t
	WHERE t.rnk_worker_time <= 12
	OR t.rnk_physical_reads <= 12
	OR t.rnk_logical_writes <= 12
	OR t.rnk_logical_reads <= 12
	OR t.rnk_elapsed_time <= 12
	;

	--Ok, so at this point we have
	--	1. The "top X" objects for our various categories, saved off to the ObjectStats table.
	--		We also have their DBID and OBJECTID values so we can resolve to strings later. 

	--		We *WANT* to grab data & sql text for all significant statements in query_stats & their plans, 
	--		and save off to a separate table. 

	--	2. The "top X" non-proc/non-trigger query patterns, saved off to the QueryPatterns table.
	--		We *WANT* to capture some number of "representative" entries (5?) from query_stats for the pattern,
	--		and save its sql text and query plan info




	IF @lv__CurrentTable = N'A'
	BEGIN
		--beginning of first half of smaller A/B block
		SET @errorloc = N'A #TopObjects insert';
		INSERT INTO #TopObjects_StmtStats (
			[database_id],					--1
			[object_id],
			[type],
			[sql_handle],
			[plan_handle],					--5
			[statement_start_offset],
			[statement_end_offset],
			[plan_generation_num],
			[plan_generation_num_delta],
			[creation_time],				--10
			[last_execution_time],
			[execution_count],
			[execution_count_delta],
			[total_worker_time],
			[total_worker_time_delta],		--15
			[last_worker_time],
			[min_worker_time],
			[max_worker_time],
			[total_physical_reads],
			[total_physical_reads_delta],	--20
			[last_physical_reads],
			[min_physical_reads],
			[max_physical_reads],
			[total_logical_writes],
			[total_logical_writes_delta],	--25
			[last_logical_writes],
			[min_logical_writes],
			[max_logical_writes],
			[total_logical_reads],
			[total_logical_reads_delta],	--30
			[last_logical_reads],
			[min_logical_reads],
			[max_logical_reads],
			[total_clr_time],
			[total_clr_time_delta],			--35
			[last_clr_time],
			[min_clr_time],
			[max_clr_time],
			[total_elapsed_time],
			[total_elapsed_time_delta],		--40
			[last_elapsed_time],
			[min_elapsed_time],
			[max_elapsed_time],
			[query_hash],
			[query_plan_hash],				--45
			[total_rows],
			[total_rows_delta],
			[last_rows],
			[min_rows],
			[max_rows]						--50
		)
		SELECT 
			os1.database_id,				--1
			os1.object_id,
			os1.type,
			os1.sql_handle,
			os1.plan_handle,				--5
			a.statement_start_offset,
			a.statement_end_offset,
			a.plan_generation_num, 
			plan_generation_num_delta = CASE WHEN a.plan_generation_num - ISNULL(b.plan_generation_num,0) < 0 THEN a.plan_generation_num
											ELSE a.plan_generation_num - ISNULL(b.plan_generation_num,0) END,
			a.creation_time,				--10
			a.last_execution_time,
			a.execution_count, 
			execution_count_delta = CASE WHEN a.execution_count - ISNULL(b.execution_count,0) < 0 THEN a.execution_count
										ELSE a.execution_count - ISNULL(b.execution_count,0) END,
			a.total_worker_time,
			total_worker_time_delta = CASE WHEN a.total_worker_time - ISNULL(b.total_worker_time,0) < 0 THEN a.total_worker_time
										ELSE a.total_worker_time - ISNULL(b.total_worker_time,0) END,	--15
			a.last_worker_time, 
			a.min_worker_time, 
			a.max_worker_time, 
			a.total_physical_reads, 
			total_physical_reads_delta = CASE WHEN a.total_physical_reads - ISNULL(b.total_physical_reads,0) < 0 THEN a.total_physical_reads
											ELSE a.total_physical_reads - ISNULL(b.total_physical_reads,0) END,	--20
			a.last_physical_reads, 
			a.min_physical_reads, 
			a.max_physical_reads, 
			a.total_logical_writes,
			total_logical_writes_delta = CASE WHEN a.total_logical_writes - ISNULL(b.total_logical_writes,0) < 0 THEN a.total_logical_writes
											ELSE a.total_logical_writes - ISNULL(b.total_logical_writes,0) END,		--25 
			a.last_logical_writes, 
			a.min_logical_writes, 
			a.max_logical_writes, 
			a.total_logical_reads, 
			total_logical_reads_delta = CASE WHEN a.total_logical_reads - ISNULL(b.total_logical_reads,0) < 0 THEN a.total_logical_reads
											ELSE a.total_logical_reads - ISNULL(b.total_logical_reads,0) END,		--30
			a.last_logical_reads,
			a.min_logical_reads, 
			a.max_logical_reads, 
			a.total_clr_time, 
			total_clr_time_delta = CASE WHEN a.total_clr_time - ISNULL(b.total_clr_time,0) < 0 THEN a.total_clr_time
										ELSE a.total_clr_time - ISNULL(b.total_clr_time,0) END,			--35
			a.last_clr_time, 
			a.min_clr_time, 
			a.max_clr_time, 
			a.total_elapsed_time, 
			total_elapsed_time_delta = CASE WHEN a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) < 0 THEN a.total_elapsed_time
											ELSE a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) END,		--40
			a.last_elapsed_time, 
			a.min_elapsed_time, 
			a.max_elapsed_time, 
			a.query_hash, 
			a.query_plan_hash,					--45
			a.total_rows, 
			total_rows_delta = CASE WHEN a.total_rows - ISNULL(b.total_rows,0) < 0 THEN a.total_rows
									ELSE a.total_rows - ISNULL(b.total_rows,0) END, 
			a.last_rows, 
			a.min_rows, 
			a.max_rows							--50
		FROM ServerEye.ObjectStats os1 
			INNER JOIN ServerEye.dm_exec_query_stats__A a
				ON os1.sql_handle = a.sql_handle
			LEFT OUTER JOIN ServerEye.dm_exec_query_stats__B b
				ON os1.sql_handle = b.sql_handle
				AND a.statement_start_offset = b.statement_start_offset
				AND a.statement_end_offset = b.statement_end_offset

		WHERE os1.CollectionTime = @CollectionTime
		AND os1.type IN ('P', 'TR')
		;
	END		--end of first half of smaller A/B block
	ELSE
	BEGIN
		--beginning of 2nd half of smaller A/B block
		SET @errorloc = N'B #TopObjects insert';
		INSERT INTO #TopObjects_StmtStats (
			[database_id],					--1
			[object_id],
			[type],
			[sql_handle],
			[plan_handle],					--5
			[statement_start_offset],
			[statement_end_offset],
			[plan_generation_num],
			[plan_generation_num_delta],
			[creation_time],				--10
			[last_execution_time],
			[execution_count],
			[execution_count_delta],
			[total_worker_time],
			[total_worker_time_delta],		--15
			[last_worker_time],
			[min_worker_time],
			[max_worker_time],
			[total_physical_reads],
			[total_physical_reads_delta],	--20
			[last_physical_reads],
			[min_physical_reads],
			[max_physical_reads],
			[total_logical_writes],
			[total_logical_writes_delta],	--25
			[last_logical_writes],
			[min_logical_writes],
			[max_logical_writes],
			[total_logical_reads],
			[total_logical_reads_delta],	--30
			[last_logical_reads],
			[min_logical_reads],
			[max_logical_reads],
			[total_clr_time],
			[total_clr_time_delta],			--35
			[last_clr_time],
			[min_clr_time],
			[max_clr_time],
			[total_elapsed_time],
			[total_elapsed_time_delta],		--40
			[last_elapsed_time],
			[min_elapsed_time],
			[max_elapsed_time],
			[query_hash],
			[query_plan_hash],				--45
			[total_rows],
			[total_rows_delta],
			[last_rows],
			[min_rows],
			[max_rows]						--50
		)
		SELECT 
			os1.database_id,				--1
			os1.object_id,
			os1.type,
			os1.sql_handle,
			os1.plan_handle,				--5
			a.statement_start_offset,
			a.statement_end_offset,
			a.plan_generation_num, 
			plan_generation_num_delta = CASE WHEN a.plan_generation_num - ISNULL(b.plan_generation_num,0) < 0 THEN a.plan_generation_num
											ELSE a.plan_generation_num - ISNULL(b.plan_generation_num,0) END,
			a.creation_time,				--10
			a.last_execution_time,
			a.execution_count, 
			execution_count_delta = CASE WHEN a.execution_count - ISNULL(b.execution_count,0) < 0 THEN a.execution_count
										ELSE a.execution_count - ISNULL(b.execution_count,0) END,
			a.total_worker_time,
			total_worker_time_delta = CASE WHEN a.total_worker_time - ISNULL(b.total_worker_time,0) < 0 THEN a.total_worker_time
										ELSE a.total_worker_time - ISNULL(b.total_worker_time,0) END,	--15
			a.last_worker_time, 
			a.min_worker_time, 
			a.max_worker_time, 
			a.total_physical_reads, 
			total_physical_reads_delta = CASE WHEN a.total_physical_reads - ISNULL(b.total_physical_reads,0) < 0 THEN a.total_physical_reads
											ELSE a.total_physical_reads - ISNULL(b.total_physical_reads,0) END,	--20
			a.last_physical_reads, 
			a.min_physical_reads, 
			a.max_physical_reads, 
			a.total_logical_writes,
			total_logical_writes_delta = CASE WHEN a.total_logical_writes - ISNULL(b.total_logical_writes,0) < 0 THEN a.total_logical_writes
											ELSE a.total_logical_writes - ISNULL(b.total_logical_writes,0) END,		--25 
			a.last_logical_writes, 
			a.min_logical_writes, 
			a.max_logical_writes, 
			a.total_logical_reads, 
			total_logical_reads_delta = CASE WHEN a.total_logical_reads - ISNULL(b.total_logical_reads,0) < 0 THEN a.total_logical_reads
											ELSE a.total_logical_reads - ISNULL(b.total_logical_reads,0) END,		--30
			a.last_logical_reads,
			a.min_logical_reads, 
			a.max_logical_reads, 
			a.total_clr_time, 
			total_clr_time_delta = CASE WHEN a.total_clr_time - ISNULL(b.total_clr_time,0) < 0 THEN a.total_clr_time
										ELSE a.total_clr_time - ISNULL(b.total_clr_time,0) END,			--35
			a.last_clr_time, 
			a.min_clr_time, 
			a.max_clr_time, 
			a.total_elapsed_time, 
			total_elapsed_time_delta = CASE WHEN a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) < 0 THEN a.total_elapsed_time
											ELSE a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) END,		--40
			a.last_elapsed_time, 
			a.min_elapsed_time, 
			a.max_elapsed_time, 
			a.query_hash, 
			a.query_plan_hash,					--45
			a.total_rows, 
			total_rows_delta = CASE WHEN a.total_rows - ISNULL(b.total_rows,0) < 0 THEN a.total_rows
									ELSE a.total_rows - ISNULL(b.total_rows,0) END, 
			a.last_rows, 
			a.min_rows, 
			a.max_rows							--50
		--like above, we switch the table position but not the alias position
		FROM ServerEye.ObjectStats os1 
			INNER JOIN ServerEye.dm_exec_query_stats__B a
				ON os1.sql_handle = a.sql_handle
			LEFT OUTER JOIN ServerEye.dm_exec_query_stats__A b			
				ON os1.sql_handle = b.sql_handle
				AND a.statement_start_offset = b.statement_start_offset
				AND a.statement_end_offset = b.statement_end_offset
		WHERE os1.CollectionTime = @CollectionTime
		AND os1.type IN ('P', 'TR')
		;
	END		--end of smaller A/B block


	--update the pct_  fields and for "significant" things (by default, 5% or more), grab the pointer to the SQL text
	-- from the SQL stmt store, if it exists there.
	-- The passthru logic syntax is used since we expect most procs to have just a few significant statements.
	SET @errorloc = N'#TopObjects Pct';
	UPDATE targ 
	SET pct_worker_time = ss2.pct_worker_time,
		pct_phys_reads = ss2.pct_phys_reads,
		pct_logical_writes = ss2.pct_logical_writes,
		pct_logical_reads = ss2.pct_logical_reads,
		pct_elapsed_time = ss2.pct_elapsed_time,

		FKSQLStmtStoreID = CASE WHEN ss2.pct_worker_time < 5.0 AND ss2.pct_phys_reads < 5.0
									AND ss2.pct_logical_writes < 5.0 AND ss2.pct_logical_reads < 5.0
									AND ss2.pct_elapsed_time < 5.0 
								THEN NULL 
								ELSE (
									SELECT sss.PKSQLStmtStoreID
									FROM DMViewerCore.SQLStmtStore sss
									WHERE sss.sql_handle = ss2.sql_handle
									AND sss.statement_start_offset = ss2.statement_start_offset
									AND sss.statement_end_offset = ss2.statement_end_offset
									--if the dbid/objectid don't match, then we let this entry fall through to
									-- the loop below. However, in practice I don't think there can/will be mismatches?
									AND sss.dbid = ss2.database_id
									AND sss.objectid = ss2.object_id
									AND sss.fail_to_obtain = CONVERT(BIT,0)
								)
								END
	FROM #TopObjects_StmtStats targ 
		INNER JOIN (
			SELECT
				[database_id],
				[object_id],
				[sql_handle],
				[statement_start_offset],
				[statement_end_offset],

				pct_worker_time = CASE WHEN sum_worker_time > 0 
									THEN CONVERT(DECIMAL(5,2),(1.*total_worker_time_delta) / (1.*sum_worker_time))
									ELSE -1 END,
				pct_phys_reads = CASE WHEN sum_phys_reads > 0 
									THEN CONVERT(DECIMAL(5,2),(1.*total_physical_reads_delta) / (1.*sum_phys_reads))
									ELSE -1 END,
				pct_logical_writes = CASE WHEN sum_logical_writes > 0 
									THEN CONVERT(DECIMAL(5,2),(1.*total_logical_writes_delta) / (1.*sum_logical_writes)) 
									ELSE -1 END,
				pct_logical_reads = CASE WHEN sum_logical_reads > 0 
									THEN CONVERT(DECIMAL(5,2),(1.*total_logical_reads_delta) / (1.*sum_logical_reads)) 
									ELSE -1 END,
				pct_elapsed_time = CASE WHEN sum_elapsed_time > 0 
									THEN CONVERT(DECIMAL(5,2),(1.*total_elapsed_time_delta) / (1.*sum_elapsed_time)) 
									ELSE -1 END
			FROM (
				SELECT 
					[database_id],
					[object_id],
					[sql_handle],
					[statement_start_offset],
					[statement_end_offset],
					[total_worker_time_delta],
					[total_physical_reads_delta],
					[total_logical_writes_delta],
					[total_logical_reads_delta],
					[total_clr_time_delta],
					[total_elapsed_time_delta],
					[total_rows_delta],

					sum_worker_time = SUM(total_worker_time_delta) OVER (PARTITION BY database_id, object_id),
					sum_phys_reads = SUM(total_physical_reads_delta) OVER (PARTITION BY database_id, object_id),
					sum_logical_reads = SUM(total_logical_reads_delta) OVER (PARTITION BY database_id, object_id),
					sum_logical_writes = SUM(total_logical_writes_delta) OVER (PARTITION BY database_id, object_id), 
					sum_clr_time = SUM(total_clr_time_delta) OVER (PARTITION BY database_id, object_id),
					sum_elapsed_time = SUM(total_elapsed_time_delta) OVER (PARTITION BY database_id, object_id),
					sum_rows = SUM(total_rows_delta) OVER (PARTITION BY database_id, object_id)
				FROM #TopObjects_StmtStats t
			) ss
		) ss2
			ON targ.database_id = ss2.database_id
			AND targ.object_id = ss2.object_id
	;


	DECLARE @lv__database_id INT,
		@lv__object_id INT,
		@lv__StatementsPulled	BIT,
		@lv__PlansPulled	BIT,
		@lv__curHandle VARBINARY(64),
		@lv__curStatementOffsetStart	INT,
		@lv__curStatementOffsetEnd		INT,
		@lv__usedStartOffset			INT,
		@lv__usedEndOffset				INT,
		@lv__nullstring						NVARCHAR(8),
		@lv__nullint						INT,
		@lv__nullsmallint					SMALLINT,
		@lv__nulldatetime					DATETIME;

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value
	SET @lv__nulldatetime = '2005-01-01';
	SET @lv__StatementsPulled = 0;
	SET @lv__PlansPulled = 0;

	SET @errorloc = N'#TopObjects StmtText';
	SET LOCK_TIMEOUT 20;

	DECLARE iterateTopObjects_StmtText CURSOR STATIC LOCAL FORWARD_ONLY FOR 
	SELECT DISTINCT targ.database_id, 
		targ.object_id, 
		targ.sql_handle,
		targ.statement_start_offset,
		targ.statement_end_offset
	FROM #TopObjects_StmtStats targ 
	WHERE (targ.pct_elapsed_time >= 5.0
	OR targ.pct_logical_reads >= 5.0
	OR targ.pct_logical_writes >= 5.0
	OR targ.pct_phys_reads >= 5.0
	OR targ.pct_worker_time >= 5.0
	)
	AND targ.FKSQLStmtStoreID IS NULL
	AND targ.sql_handle IS NOT NULL
	AND targ.sql_handle <> 0x00
	;

	OPEN iterateTopObjects_StmtText;
	FETCH iterateTopObjects_StmtText INTO @lv__database_id, @lv__object_id, @lv__curHandle, @lv__curStatementOffsetStart, @lv__curStatementOffsetEnd;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @lv__StatementsPulled = CONVERT(BIT,1);

		BEGIN TRY
			INSERT INTO #t__stmt (
				sql_handle, 
				statement_start_offset, 
				statement_end_offset, 
				dbid, 
				objectid, 
				fail_to_obtain, 
				datalen_batch,
				stmt_text
			)
			SELECT curhandle,
				curoffsetstart,
				curoffsetend,
				dbid, 
				objectid,
				fail_to_obtain,
				datalen_batch,
				stmt_text
			FROM (
				SELECT curhandle = @lv__curHandle, 
					curoffsetstart = @lv__curStatementOffsetStart, 
					curoffsetend = @lv__curStatementOffsetEnd, 
					ss.dbid, 
					ss.objectid, 
					ss.fail_to_obtain,
					datalen_batch,
					[stmt_text] = CASE WHEN ss.fail_to_obtain = 1 THEN ss.stmt_text		--in failure cases, ss.stmt_text contains the reason why
						ELSE (
							CASE WHEN @lv__curStatementOffsetStart = 0 THEN 
									CASE 
										WHEN @lv__curStatementOffsetEnd IN (0,-1) 
											THEN ss.stmt_text 
										ELSE SUBSTRING(ss.stmt_text, 1, @lv__curStatementOffsetEnd/2 + 1) 
									END 
								WHEN datalen_batch = 0 THEN SUBSTRING(ss.stmt_text, (@lv__curStatementOffsetStart/2)+1, 4000)
								WHEN datalen_batch <= @lv__curStatementOffsetStart 
									THEN SUBSTRING(ss.stmt_text, 1, 4000)
								WHEN datalen_batch < @lv__curStatementOffsetEnd 
									THEN SUBSTRING(ss.stmt_text, 
												1,
												(CASE @lv__curStatementOffsetEnd
													WHEN -1 THEN datalen_batch 
													ELSE @lv__curStatementOffsetEnd
													END - @lv__curStatementOffsetStart
												)/2 + 1
											) 
								ELSE SUBSTRING(ss.stmt_text, 
												(@lv__curStatementOffsetStart/2)+1, 
												(CASE @lv__curStatementOffsetEnd
													WHEN -1 THEN datalen_batch 
													ELSE @lv__curStatementOffsetEnd
													END - @lv__curStatementOffsetStart
												)/2 + 1
											) 
									END 
							) END
				FROM 
				(SELECT [dbid] = ISNULL(txt.dbid,@lv__nullsmallint), 
						[objectid] = ISNULL(txt.objectid,@lv__nullint), 
						[stmt_text] = ISNULL(txt.text, 'SQL batch info was NULL'), 
						[fail_to_obtain] = CASE WHEN txt.text IS NULL THEN 1 ELSE 0 END,
						[datalen_batch] = DATALENGTH(txt.text)
				FROM sys.dm_exec_sql_text(@lv__curHandle) txt) ss
			) outerquery
			;
		END TRY
		BEGIN CATCH
			INSERT INTO #t__stmt (
				sql_handle, 
				statement_start_offset, 
				statement_end_offset, 
				dbid, 
				objectid, 
				fail_to_obtain, 
				datalen_batch,
				stmt_text
			)	
			SELECT @lv__curHandle, 
				@lv__curStatementOffsetStart, 
				@lv__curStatementOffsetEnd, 
				@lv__nullsmallint, 
				@lv__nullint, 
				1, 
				0, 
				'Error getting SQL statement text: ' + CONVERT(VARCHAR(20),ERROR_NUMBER()) + '; ' + ERROR_MESSAGE()
		END CATCH

		FETCH iterateTopObjects_StmtText INTO @lv__database_id, @lv__object_id, @lv__curHandle, @lv__curStatementOffsetStart, @lv__curStatementOffsetEnd;
	END

	CLOSE iterateTopObjects_StmtText;
	DEALLOCATE iterateTopObjects_StmtText;



	--Now go get stmt query plans. Because we need to hash every query plan, we have to obtain the plan for every
	-- statement that was significant, and compare it to the hash in the store.
	SET @errorloc = N'#TopObjects StmtPlan';
	DECLARE iterateTopObjects_StmtPlan CURSOR STATIC LOCAL FORWARD_ONLY FOR 
	SELECT DISTINCT targ.database_id, 
		targ.object_id, 
		targ.plan_handle,
		targ.statement_start_offset,
		targ.statement_end_offset
	FROM #TopObjects_StmtStats targ 
	WHERE (targ.pct_elapsed_time >= 5.0
	OR targ.pct_logical_reads >= 5.0
	OR targ.pct_logical_writes >= 5.0
	OR targ.pct_phys_reads >= 5.0
	OR targ.pct_worker_time >= 5.0
	)
	AND targ.FKQueryPlanStmtStoreID IS NULL
	AND targ.plan_handle IS NOT NULL
	AND targ.plan_handle <> 0x00
	;

	OPEN iterateTopObjects_StmtPlan;
	FETCH iterateTopObjects_StmtPlan INTO @lv__database_id, @lv__object_id, @lv__curHandle, @lv__curStatementOffsetStart, @lv__curStatementOffsetEnd;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @lv__PlansPulled = 1;

		--since I've seen cases where the statement_start_offset and statement_end_offset are a little funky, 
		-- do a bit of edge-case handling (where we'll just get the full plan instead)
		IF @lv__curStatementOffsetStart = 0 
		BEGIN
			IF @lv__curStatementOffsetEnd IN (0,-1)
			BEGIN
				SET @lv__usedStartOffset = 0;
				SET @lv__usedEndOffset = -1;
			END
			ELSE
			BEGIN
				SET @lv__usedStartOffset = 0;
				SET @lv__usedEndOffset = @lv__curStatementOffsetEnd;
			END
		END
		ELSE
		BEGIN
			SET @lv__usedStartOffset = @lv__curStatementOffsetStart;
			SET @lv__usedEndOffset = @lv__curStatementOffsetEnd;
		END

		BEGIN TRY
			INSERT INTO #t__stmtqp (
				[plan_handle],
				[statement_start_offset],
				[statement_end_offset],
				[dbid],
				[objectid],
				[fail_to_obtain],
				[query_plan],
				[aw_stmtplan_hash]
			)
			SELECT 
				curHandle,
				curstartoffset,
				curendoffset,
				[dbid], 
				objectid,
				fail_to_obtain,
				query_plan,
				aw_stmtplan_hash = HASHBYTES('MD5',
					(SUBSTRING(query_plan,1,3940) +
					CONVERT(nvarchar(40),CHECKSUM(query_plan)))
					)
			FROM (
				SELECT 
					curHandle = @lv__curHandle, 
					--Note that we store the offsets we were given, not the ones we actually used
					-- (@lv__usedStartOffset/EndOffset). This makes troubleshooting this code & resulting plans easier
					curstartoffset = @lv__curStatementOffsetStart, 
					curendoffset = @lv__curStatementOffsetEnd,
					[dbid] = ISNULL(dbid,@lv__nullsmallint),
					[objectid] = ISNULL(objectid,@lv__nullint),
					[fail_to_obtain] = CASE WHEN query_plan IS NULL THEN 1 ELSE 0 END, 
					[query_plan] = 
						CASE 
							WHEN s2.row_exists IS NULL 
								THEN '<?PlanError -- ' + CHAR(13) + CHAR(10) + 'Query Plan DMV did not return a row' + CHAR(13) + CHAR(10) + '-- ?>'
							WHEN s2.row_exists IS NOT NULL AND s2.query_plan IS NULL 
								THEN '<?PlanError -- ' + CHAR(13) + CHAR(10) + 'Statement Query Plan is NULL' + CHAR(13) + CHAR(10) + '-- ?>'
							ELSE s2.query_plan
						END
				FROM
					(SELECT 0 as col1) s
					LEFT OUTER JOIN 
					(SELECT 1 as row_exists, t.dbid, t.objectid, t.query_plan
						FROM sys.dm_exec_text_query_plan(@lv__curHandle, @lv__usedStartOffset, @lv__usedEndOffset) t) s2
						ON 1=1
			) s3;
		END TRY
		BEGIN CATCH
			INSERT INTO #t__stmtqp (
				[plan_handle],
				[statement_start_offset],
				[statement_end_offset],
				[dbid],
				[objectid],
				[fail_to_obtain],
				[query_plan],
				[aw_stmtplan_hash]
			)
			SELECT curHandle = @lv__curHandle,
				curstartoffset = @lv__curStatementOffsetStart,
				curendoffset = @lv__curStatementOffsetEnd,
				@lv__nullsmallint,
				@lv__nullint,
				1 as fail_to_obtain, 
				--'Error obtaining Statement Query Plan: ' + CONVERT(VARCHAR(20),ERROR_NUMBER()) + '; ' + ERROR_MESSAGE()
				N'<?PlanError -- ' + NCHAR(13) + NCHAR(10) + N'Error obtaining Statement Query Plan: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + N'; ' + ISNULL(ERROR_MESSAGE(),N'<null>') + NCHAR(13) + NCHAR(10) + N'-- ?>',
				HASHBYTES('MD5', 
					N'<?PlanError -- ' + NCHAR(13) + NCHAR(10) + N'Error obtaining Statement Query Plan: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + N'; ' + ISNULL(ERROR_MESSAGE(),N'<null>') + NCHAR(13) + NCHAR(10) + N'-- ?>'
					);
		END CATCH 

		FETCH iterateTopObjects_StmtPlan INTO @lv__database_id, @lv__object_id, @lv__curHandle, @lv__curStatementOffsetStart, @lv__curStatementOffsetEnd;
	END

	CLOSE iterateTopObjects_StmtPlan;
	DEALLOCATE iterateTopObjects_StmtPlan;

	SET LOCK_TIMEOUT -1;

	IF @lv__StatementsPulled = CONVERT(BIT,1)
	BEGIN
		SET @errorloc = N'#TopObjects Stmt Persist';
		MERGE DMViewerCore.SQLStmtStore perm
			USING #t__stmt t
				ON perm.sql_handle = t.sql_handle
				AND perm.statement_start_offset = t.statement_start_offset
				AND perm.statement_end_offset = t.statement_end_offset
		WHEN MATCHED THEN UPDATE 
			-- #t__stmt has a match in the store. If this occurred, it means
			-- that the initial population (above) of #TopObjects_StmtStats did not find
			-- a match, and thus fail_to_obtain must have been 1 (or the DBID/OBJECT
			-- ID values didn't match, but we don't expect that to occur). 
			-- Update all of the attributes of the existing entry in the store.
			SET perm.dbid = t.dbid, 
				perm.objectid = t.objectid,
				perm.fail_to_obtain = t.fail_to_obtain,
				perm.datalen_batch = t.datalen_batch,
				perm.stmt_text = t.stmt_text,
				perm.LastTouchedBy_SPIDCaptureTime = @CollectionTime
		WHEN NOT MATCHED BY TARGET THEN 
			--new entry
			INSERT (sql_handle, statement_start_offset, statement_end_offset, 
				dbid, objectid, fail_to_obtain, datalen_batch, stmt_text, 
				Insertedby_SPIDCaptureTime, LastTouchedBy_SPIDCaptureTime)
			VALUES (t.sql_handle, t.statement_start_offset, t.statement_end_offset,
				t.dbid, t.objectid, t.fail_to_obtain, t.datalen_batch, t.stmt_text, 
				@CollectionTime, @CollectionTime)
		;

		SET @errorloc = N'#TopObjects Stmt PK';
		UPDATE targ 
		SET targ.FKSQLStmtStoreID = sss.PKSQLStmtStoreID
		FROM #TopObjects_StmtStats targ 
			INNER hash JOIN DMViewerCore.SQLStmtStore sss
				ON sss.sql_handle = targ.sql_handle
				AND sss.statement_start_offset = targ.statement_start_offset
				AND sss.statement_end_offset = targ.statement_end_offset
				AND sss.dbid = targ.database_id
				AND sss.objectid = targ.object_id
		WHERE targ.FKSQLStmtStoreID IS NULL
			--this lets us IxSeek quickly to the rows in SQLStmtStore that we just updated/inserted
		AND sss.LastTouchedBy_SPIDCaptureTime = @CollectionTime
		OPTION(FORCE ORDER, MAXDOP 1);
	END

	IF @lv__PlansPulled = CONVERT(BIT,1)
	BEGIN
		--the unique key here is the hash + the handle + the offsets. (but not dbid & objectid)
		SET @errorloc = N'#TopObjects Plan Persist';
		MERGE DMViewerCore.QueryPlanStmtStore perm
			USING #t__stmtqp t
				ON perm.AWStmtPlanHash = t.aw_stmtplan_hash
				AND perm.plan_handle = t.plan_handle
				AND perm.statement_start_offset = t.statement_start_offset
				AND perm.statement_end_offset = t.statement_end_offset
		WHEN MATCHED THEN UPDATE
			--We overwrite dbid/objectid b/c I don't have 100% certainty that a plan_handle/offset combo, with
			-- the plan hash, will ALWAYS have the same dbid/objectid (though it seems mathematically almost 
			-- impossible to have a collision here).
			SET perm.LastTouchedBy_SPIDCaptureTime = @CollectionTime, 
				perm.dbid = t.dbid, 
				perm.objectid = t.objectid
		WHEN NOT MATCHED THEN
			INSERT (AWStmtPlanHash, plan_handle, statement_start_offset, statement_end_offset, 
				dbid, objectid, fail_to_obtain, query_plan, 
				Insertedby_SPIDCaptureTime, LastTouchedBy_SPIDCaptureTime)
			VALUES (t.aw_stmtplan_hash, t.plan_handle, t.statement_start_offset, t.statement_end_offset,
				t.dbid, t.objectid, t.fail_to_obtain, t.query_plan, 
				@CollectionTime, @CollectionTime)
		;

		SET @errorloc = N'#TopObjects Plan PK';
		UPDATE targ
		SET targ.FKQueryPlanStmtStoreID = qps.PKQueryPlanStmtStoreID
		FROM #TopObjects_StmtStats targ 
			INNER JOIN DMViewerCore.QueryPlanStmtStore qps
				ON qps.plan_handle = targ.plan_handle
				AND qps.statement_start_offset = targ.statement_start_offset
				AND qps.statement_end_offset = targ.statement_end_offset
		WHERE qps.LastTouchedBy_SPIDCaptureTime = @CollectionTime
		;
	END

	--Ok, we've obtained info we want for our "Top Objects". Persist to a permanent table
	SET @errorloc = N'TopObjects Stmt Final Persist';
	INSERT INTO [ServerEye].[ObjectStats_StmtStats] (
		CollectionTime, 
		database_id, 
		object_id, 
		type, 
		sql_handle, 
		plan_handle, 
		statement_start_offset, 
		statement_end_offset, 
		plan_generation_num, 
		plan_generation_num_delta, 
		creation_time, 
		last_execution_time, 
		execution_count, 
		execution_count_delta, 
		total_worker_time, 
		total_worker_time_delta, 
		last_worker_time, 
		min_worker_time, 
		max_worker_time, 
		total_physical_reads, 
		total_physical_reads_delta, 
		last_physical_reads, 
		min_physical_reads, 
		max_physical_reads, 
		total_logical_writes, 
		total_logical_writes_delta, 
		last_logical_writes, 
		min_logical_writes, 
		max_logical_writes, 
		total_logical_reads, 
		total_logical_reads_delta, 
		last_logical_reads, 
		min_logical_reads, 
		max_logical_reads, 
		total_clr_time, 
		total_clr_time_delta, 
		last_clr_time, 
		min_clr_time, 
		max_clr_time, 
		total_elapsed_time, 
		total_elapsed_time_delta, 
		last_elapsed_time, 
		min_elapsed_time, 
		max_elapsed_time, 
		query_hash, 
		query_plan_hash, 
		total_rows, 
		total_rows_delta,
		last_rows, 
		min_rows, 
		max_rows, 
		pct_worker_time, 
		pct_phys_reads, 
		pct_logical_writes, 
		pct_logical_reads, 
		pct_elapsed_time, 
		FKSQLStmtStoreID, 
		FKQueryPlanStmtStoreID
	)
	SELECT 
		@CollectionTime,
		database_id,
		object_id,
		type,
		sql_handle,
		plan_handle,
		statement_start_offset, 
		statement_end_offset, 
		plan_generation_num, 
		plan_generation_num_delta, 
		creation_time, 
		last_execution_time, 
		execution_count, 
		execution_count_delta, 
		total_worker_time, 
		total_worker_time_delta, 
		last_worker_time, 
		min_worker_time, 
		max_worker_time, 
		total_physical_reads, 
		total_physical_reads_delta, 
		last_physical_reads, 
		min_physical_reads, 
		max_physical_reads, 
		total_logical_writes, 
		total_logical_writes_delta, 
		last_logical_writes, 
		min_logical_writes, 
		max_logical_writes, 
		total_logical_reads, 
		total_logical_reads_delta, 
		last_logical_reads, 
		min_logical_reads, 
		max_logical_reads, 
		total_clr_time, 
		total_clr_time_delta, 
		last_clr_time, 
		min_clr_time, 
		max_clr_time, 
		total_elapsed_time, 
		total_elapsed_time_delta, 
		last_elapsed_time, 
		min_elapsed_time, 
		max_elapsed_time, 
		query_hash, 
		query_plan_hash, 
		total_rows, 
		total_rows_delta,
		last_rows, 
		min_rows, 
		max_rows, 
		pct_worker_time, 
		pct_phys_reads, 
		pct_logical_writes, 
		pct_logical_reads, 
		pct_elapsed_time, 
		FKSQLStmtStoreID, 
		FKQueryPlanStmtStoreID
	FROM #TopObjects_StmtStats t
	--TODO: we'll come back and evaluate whether we should just persist the "significant" statements
	-- (those 5% or more of any of the resources), but for now will try persisting everything.
	-- May make this configurable
	;


	IF @lv__CurrentTable = N'A'
	BEGIN
		--Beginning of the first half of another big A/B block
		SET @errorloc = N'A #TopPatterns RepStmt';
		INSERT INTO #TopPatterns_RepresentativeStmts (
			[cacheobjtype],					--1
			[objtype],
			[query_hash],
			[sql_handle],
			[statement_start_offset],		--5
			[statement_end_offset],
			[plan_handle],
			[plan_generation_num],
			[plan_generation_num_delta],
			[creation_time],				--10
			[last_execution_time],
			[execution_count],
			[execution_count_delta],
			[total_worker_time],
			[total_worker_time_delta],		--15
			[last_worker_time],
			[min_worker_time],
			[max_worker_time],
			[total_physical_reads],
			[total_physical_reads_delta],	--20
			[last_physical_reads],
			[min_physical_reads],
			[max_physical_reads],
			[total_logical_writes],
			[total_logical_writes_delta],	--25
			[last_logical_writes],
			[min_logical_writes],
			[max_logical_writes],
			[total_logical_reads],
			[total_logical_reads_delta],	--30
			[last_logical_reads],
			[min_logical_reads],
			[max_logical_reads],
			[total_clr_time],
			[total_clr_time_delta],			--35
			[last_clr_time],
			[min_clr_time],
			[max_clr_time],
			[total_elapsed_time],
			[total_elapsed_time_delta],		--40
			[last_elapsed_time],
			[min_elapsed_time],
			[max_elapsed_time],
			[query_plan_hash],
			[total_rows],					--45
			[total_rows_delta],
			[last_rows],
			[min_rows],
			[max_rows],
			[refcounts],					--50
			[usecounts],
			[size_in_bytes],
			[pool_id],
			[parent_plan_handle],
			FKSQLStmtStoreID				--55
		)
		SELECT 
			qps.cacheobjtype,				--1
			qps.objtype,
			qps.query_hash,
			xapp1.sql_handle, 
			xapp1.statement_start_offset,	--5
			xapp1.statement_end_offset,
			xapp1.plan_handle,
			xapp1.plan_generation_num,
			xapp1.plan_generation_num_delta,
			xapp1.creation_time,			--10
			xapp1.last_execution_time,
			xapp1.execution_count,
			xapp1.execution_count_delta,
			xapp1.total_worker_time,
			xapp1.total_worker_time_delta,	--15
			xapp1.last_worker_time,
			xapp1.min_worker_time,
			xapp1.max_worker_time, 
			xapp1.total_physical_reads,
			xapp1.total_physical_reads_delta,		--20
			xapp1.last_physical_reads,
			xapp1.min_physical_reads,
			xapp1.max_physical_reads,
			xapp1.total_logical_writes,
			xapp1.total_logical_writes_delta,		--25
			xapp1.last_logical_writes,
			xapp1.min_logical_writes,
			xapp1.max_logical_writes,
			xapp1.total_logical_reads,
			xapp1.total_logical_reads_delta,		--30
			xapp1.last_logical_reads,
			xapp1.min_logical_reads,
			xapp1.max_logical_reads,
			xapp1.total_clr_time, 
			xapp1.total_clr_time_delta,				--35
			xapp1.last_clr_time,
			xapp1.min_clr_time,
			xapp1.max_clr_time,
			xapp1.total_elapsed_time,
			xapp1.total_elapsed_time_delta,			--40
			xapp1.last_elapsed_time,
			xapp1.min_elapsed_time,
			xapp1.max_elapsed_time,
			xapp1.query_plan_hash,
			xapp1.total_rows,						--45
			xapp1.total_rows_delta,
			xapp1.last_rows,
			xapp1.min_rows,
			xapp1.max_rows,
			xapp1.refcounts,					--50
			xapp1.usecounts,
			xapp1.size_in_bytes,
			xapp1.pool_id,
			xapp1.parent_plan_handle,
			stmtapp.PKSQLStmtStoreID			--55
		FROM ServerEye.QueryPatternStats qps
			CROSS APPLY (
				SELECT TOP 5 
					a.sql_handle, 
					a.statement_start_offset,
					a.statement_end_offset,
					a.plan_handle,
					a.plan_generation_num,
					plan_generation_num_delta = CASE WHEN a.plan_generation_num - ISNULL(b.plan_generation_num,0) < 0 THEN a.plan_generation_num
													ELSE a.plan_generation_num - ISNULL(b.plan_generation_num,0) END,
					a.creation_time,
					a.last_execution_time,
					a.execution_count,
					execution_count_delta = CASE WHEN a.execution_count - ISNULL(b.execution_count,0) < 0 THEN a.execution_count
												ELSE a.execution_count - ISNULL(b.execution_count,0) END,
					a.total_worker_time,
					total_worker_time_delta = CASE WHEN a.total_worker_time - ISNULL(b.total_worker_time,0) < 0 THEN a.total_worker_time
												ELSE a.total_worker_time - ISNULL(b.total_worker_time,0) END,
					a.last_worker_time,
					a.min_worker_time,
					a.max_worker_time, 
					a.total_physical_reads,
					total_physical_reads_delta = CASE WHEN a.total_physical_reads - ISNULL(b.total_physical_reads,0) < 0 THEN a.total_physical_reads
													ELSE a.total_physical_reads - ISNULL(b.total_physical_reads,0) END,
					a.last_physical_reads,
					a.min_physical_reads,
					a.max_physical_reads,
					a.total_logical_writes,
					total_logical_writes_delta = CASE WHEN a.total_logical_writes - ISNULL(b.total_logical_writes,0) < 0 THEN a.total_logical_writes
													ELSE a.total_logical_writes - ISNULL(b.total_logical_writes,0) END,
					a.last_logical_writes,
					a.min_logical_writes,
					a.max_logical_writes,
					a.total_logical_reads,
					total_logical_reads_delta = CASE WHEN a.total_logical_reads - ISNULL(b.total_logical_reads,0) < 0 THEN a.total_logical_reads
													ELSE a.total_logical_reads - ISNULL(b.total_logical_reads,0) END,
					a.last_logical_reads,
					a.min_logical_reads,
					a.max_logical_reads,
					a.total_clr_time, 
					total_clr_time_delta = CASE WHEN a.total_clr_time - ISNULL(b.total_clr_time,0) < 0 THEN a.total_clr_time
												ELSE a.total_clr_time - ISNULL(b.total_clr_time,0) END,
					a.last_clr_time,
					a.min_clr_time,
					a.max_clr_time,
					a.total_elapsed_time,
					total_elapsed_time_delta = CASE WHEN a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) < 0 THEN a.total_elapsed_time
												ELSE a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) END,
					a.last_elapsed_time,
					a.min_elapsed_time,
					a.max_elapsed_time,
					a.query_plan_hash,
					a.total_rows,
					total_rows_delta = CASE WHEN a.total_rows - ISNULL(b.total_rows,0) < 0 THEN a.total_rows
											ELSE a.total_rows - ISNULL(b.total_rows,0) END,
					a.last_rows,
					a.min_rows,
					a.max_rows,
					a.refcounts,
					a.usecounts,
					a.size_in_bytes,
					a.pool_id,
					a.parent_plan_handle
				FROM ServerEye.dm_exec_query_stats__A a 
					LEFT OUTER JOIN ServerEye.dm_exec_query_stats__B b 
						ON a.sql_handle = b.sql_handle
						AND a.statement_start_offset = b.statement_start_offset
						AND a.statement_end_offset = b.statement_end_offset
				WHERE qps.query_hash = a.query_hash
				ORDER BY CASE  WHEN a.execution_count - ISNULL(b.execution_count,0) < 0 THEN a.execution_count
								ELSE a.execution_count - ISNULL(b.execution_count,0) END DESC
					--TODO: do we want to make the order by conditional/configurable here?
			) xapp1 
			OUTER APPLY (
				SELECT sss.PKSQLStmtStoreID
				FROM DMViewerCore.SQLStmtStore sss
				WHERE sss.sql_handle = xapp1.sql_handle
				AND sss.statement_start_offset = xapp1.statement_start_offset
				AND sss.statement_end_offset = xapp1.statement_end_offset
				AND sss.fail_to_obtain = CONVERT(BIT,0)
			) stmtapp
		WHERE qps.CollectionTime = @CollectionTime
		;

		--End of the first half of another big A/B block
	END
	ELSE
	BEGIN
		--Beginning of the 2nd half of another big A/B block
		SET @errorloc = N'B #TopPatterns RepStmt';
		INSERT INTO #TopPatterns_RepresentativeStmts (
			[cacheobjtype],					--1
			[objtype],
			[query_hash],
			[sql_handle],
			[statement_start_offset],		--5
			[statement_end_offset],
			[plan_handle],
			[plan_generation_num],
			[plan_generation_num_delta],
			[creation_time],				--10
			[last_execution_time],
			[execution_count],
			[execution_count_delta],
			[total_worker_time],
			[total_worker_time_delta],		--15
			[last_worker_time],
			[min_worker_time],
			[max_worker_time],
			[total_physical_reads],
			[total_physical_reads_delta],	--20
			[last_physical_reads],
			[min_physical_reads],
			[max_physical_reads],
			[total_logical_writes],
			[total_logical_writes_delta],	--25
			[last_logical_writes],
			[min_logical_writes],
			[max_logical_writes],
			[total_logical_reads],
			[total_logical_reads_delta],	--30
			[last_logical_reads],
			[min_logical_reads],
			[max_logical_reads],
			[total_clr_time],
			[total_clr_time_delta],			--35
			[last_clr_time],
			[min_clr_time],
			[max_clr_time],
			[total_elapsed_time],
			[total_elapsed_time_delta],		--40
			[last_elapsed_time],
			[min_elapsed_time],
			[max_elapsed_time],
			[query_plan_hash],
			[total_rows],					--45
			[total_rows_delta],
			[last_rows],
			[min_rows],
			[max_rows],
			[refcounts],					--50
			[usecounts],
			[size_in_bytes],
			[pool_id],
			[parent_plan_handle],
			FKSQLStmtStoreID				--55
		)
		SELECT 
			qps.cacheobjtype,				--1
			qps.objtype,
			qps.query_hash,
			xapp1.sql_handle, 
			xapp1.statement_start_offset,	--5
			xapp1.statement_end_offset,
			xapp1.plan_handle,
			xapp1.plan_generation_num,
			xapp1.plan_generation_num_delta,
			xapp1.creation_time,			--10
			xapp1.last_execution_time,
			xapp1.execution_count,
			xapp1.execution_count_delta,
			xapp1.total_worker_time,
			xapp1.total_worker_time_delta,	--15
			xapp1.last_worker_time,
			xapp1.min_worker_time,
			xapp1.max_worker_time, 
			xapp1.total_physical_reads,
			xapp1.total_physical_reads_delta,		--20
			xapp1.last_physical_reads,
			xapp1.min_physical_reads,
			xapp1.max_physical_reads,
			xapp1.total_logical_writes,
			xapp1.total_logical_writes_delta,		--25
			xapp1.last_logical_writes,
			xapp1.min_logical_writes,
			xapp1.max_logical_writes,
			xapp1.total_logical_reads,
			xapp1.total_logical_reads_delta,		--30
			xapp1.last_logical_reads,
			xapp1.min_logical_reads,
			xapp1.max_logical_reads,
			xapp1.total_clr_time, 
			xapp1.total_clr_time_delta,				--35
			xapp1.last_clr_time,
			xapp1.min_clr_time,
			xapp1.max_clr_time,
			xapp1.total_elapsed_time,
			xapp1.total_elapsed_time_delta,			--40
			xapp1.last_elapsed_time,
			xapp1.min_elapsed_time,
			xapp1.max_elapsed_time,
			xapp1.query_plan_hash,
			xapp1.total_rows,						--45
			xapp1.total_rows_delta,
			xapp1.last_rows,
			xapp1.min_rows,
			xapp1.max_rows,
			xapp1.refcounts,					--50
			xapp1.usecounts,
			xapp1.size_in_bytes,
			xapp1.pool_id,
			xapp1.parent_plan_handle,
			stmtapp.PKSQLStmtStoreID			--55
		FROM ServerEye.QueryPatternStats qps
			CROSS APPLY (
				SELECT TOP 5		--TODO: make this configurable
					a.sql_handle, 
					a.statement_start_offset,
					a.statement_end_offset,
					a.plan_handle,
					a.plan_generation_num,
					plan_generation_num_delta = CASE WHEN a.plan_generation_num - ISNULL(b.plan_generation_num,0) < 0 THEN a.plan_generation_num
													ELSE a.plan_generation_num - ISNULL(b.plan_generation_num,0) END,
					a.creation_time,
					a.last_execution_time,
					a.execution_count,
					execution_count_delta = CASE WHEN a.execution_count - ISNULL(b.execution_count,0) < 0 THEN a.execution_count
												ELSE a.execution_count - ISNULL(b.execution_count,0) END,
					a.total_worker_time,
					total_worker_time_delta = CASE WHEN a.total_worker_time - ISNULL(b.total_worker_time,0) < 0 THEN a.total_worker_time
												ELSE a.total_worker_time - ISNULL(b.total_worker_time,0) END,
					a.last_worker_time,
					a.min_worker_time,
					a.max_worker_time, 
					a.total_physical_reads,
					total_physical_reads_delta = CASE WHEN a.total_physical_reads - ISNULL(b.total_physical_reads,0) < 0 THEN a.total_physical_reads
													ELSE a.total_physical_reads - ISNULL(b.total_physical_reads,0) END,
					a.last_physical_reads,
					a.min_physical_reads,
					a.max_physical_reads,
					a.total_logical_writes,
					total_logical_writes_delta = CASE WHEN a.total_logical_writes - ISNULL(b.total_logical_writes,0) < 0 THEN a.total_logical_writes
													ELSE a.total_logical_writes - ISNULL(b.total_logical_writes,0) END,
					a.last_logical_writes,
					a.min_logical_writes,
					a.max_logical_writes,
					a.total_logical_reads,
					total_logical_reads_delta = CASE WHEN a.total_logical_reads - ISNULL(b.total_logical_reads,0) < 0 THEN a.total_logical_reads
													ELSE a.total_logical_reads - ISNULL(b.total_logical_reads,0) END,
					a.last_logical_reads,
					a.min_logical_reads,
					a.max_logical_reads,
					a.total_clr_time, 
					total_clr_time_delta = CASE WHEN a.total_clr_time - ISNULL(b.total_clr_time,0) < 0 THEN a.total_clr_time
												ELSE a.total_clr_time - ISNULL(b.total_clr_time,0) END,
					a.last_clr_time,
					a.min_clr_time,
					a.max_clr_time,
					a.total_elapsed_time,
					total_elapsed_time_delta = CASE WHEN a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) < 0 THEN a.total_elapsed_time
												ELSE a.total_elapsed_time - ISNULL(b.total_elapsed_time,0) END,
					a.last_elapsed_time,
					a.min_elapsed_time,
					a.max_elapsed_time,
					a.query_plan_hash,
					a.total_rows,
					total_rows_delta = CASE WHEN a.total_rows - ISNULL(b.total_rows,0) < 0 THEN a.total_rows
											ELSE a.total_rows - ISNULL(b.total_rows,0) END,
					a.last_rows,
					a.min_rows,
					a.max_rows,
					a.refcounts,
					a.usecounts,
					a.size_in_bytes,
					a.pool_id,
					a.parent_plan_handle
				--Another place where we copy the query from above, reverse the B and A tables,
				-- but leave the aliases in place so we minimize the # of changes required
				FROM ServerEye.dm_exec_query_stats__B a 
					LEFT OUTER JOIN ServerEye.dm_exec_query_stats__A b
						ON a.sql_handle = b.sql_handle
						AND a.statement_start_offset = b.statement_start_offset
						AND a.statement_end_offset = b.statement_end_offset
				WHERE qps.query_hash = a.query_hash
				ORDER BY CASE WHEN a.execution_count - ISNULL(b.execution_count,0) < 0 THEN a.execution_count
								ELSE a.execution_count - ISNULL(b.execution_count,0) END DESC
					--TODO: do we want to make the order by conditional/configurable here?
			) xapp1 
			OUTER APPLY (
				SELECT sss.PKSQLStmtStoreID
				FROM DMViewerCore.SQLStmtStore sss
				WHERE sss.sql_handle = xapp1.sql_handle
				AND sss.statement_start_offset = xapp1.statement_start_offset
				AND sss.statement_end_offset = xapp1.statement_end_offset
				AND sss.fail_to_obtain = CONVERT(BIT,0)
			) stmtapp
		WHERE qps.CollectionTime = @CollectionTime
		;

	END	--End of another big A/B block

	--OK, now #TopPatterns_RepresentativeStmts has 5 "representative queries" for each pattern. We have the stats,
	-- but we need the SQL text and the query plan.
	TRUNCATE TABLE #t__stmt;
	TRUNCATE TABLE #t__stmtqp;
	SET @lv__StatementsPulled = 0;
	SET @lv__PlansPulled = 0;

	SET @errorloc = N'TopPatterns StmtText';
	SET LOCK_TIMEOUT 20;

	DECLARE iterateTopPatterns_StmtText CURSOR STATIC LOCAL FORWARD_ONLY FOR 
	SELECT DISTINCT 
		t.sql_handle,
		t.statement_start_offset,
		t.statement_end_offset
	FROM #TopPatterns_RepresentativeStmts t
	WHERE t.FKSQLStmtStoreID IS NULL
	AND t.sql_handle IS NOT NULL
	AND t.sql_handle <> 0x00
	;

	OPEN iterateTopPatterns_StmtText;
	FETCH iterateTopPatterns_StmtText INTO 
		@lv__curHandle, @lv__curStatementOffsetStart, @lv__curStatementOffsetEnd;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @lv__StatementsPulled = CONVERT(BIT,1);

		BEGIN TRY
			INSERT INTO #t__stmt (
				sql_handle, 
				statement_start_offset, 
				statement_end_offset, 
				dbid, 
				objectid, 
				fail_to_obtain, 
				datalen_batch,
				stmt_text
			)
			SELECT 
				curhandle,
				curoffsetstart,
				curoffsetend,
				dbid, 
				objectid,
				fail_to_obtain,
				datalen_batch,
				stmt_text
			FROM (
				SELECT
					curhandle = @lv__curHandle, 
					curoffsetstart = @lv__curStatementOffsetStart, 
					curoffsetend = @lv__curStatementOffsetEnd, 
					ss.dbid, 
					ss.objectid, 
					ss.fail_to_obtain,
					datalen_batch,
					[stmt_text] = CASE WHEN ss.fail_to_obtain = 1 THEN ss.stmt_text		--in failure cases, ss.stmt_text contains the reason why
						ELSE (
							CASE WHEN @lv__curStatementOffsetStart = 0 THEN 
									CASE 
										WHEN @lv__curStatementOffsetEnd IN (0,-1) 
											THEN ss.stmt_text 
										ELSE SUBSTRING(ss.stmt_text, 1, @lv__curStatementOffsetEnd/2 + 1) 
									END 
								WHEN datalen_batch = 0 THEN SUBSTRING(ss.stmt_text, (@lv__curStatementOffsetStart/2)+1, 4000)
								WHEN datalen_batch <= @lv__curStatementOffsetStart 
									THEN SUBSTRING(ss.stmt_text, 1, 4000)
								WHEN datalen_batch < @lv__curStatementOffsetEnd 
									THEN SUBSTRING(ss.stmt_text, 
												1,
												(CASE @lv__curStatementOffsetEnd
													WHEN -1 THEN datalen_batch 
													ELSE @lv__curStatementOffsetEnd
													END - @lv__curStatementOffsetStart
												)/2 + 1
											) 
								ELSE SUBSTRING(ss.stmt_text, 
												(@lv__curStatementOffsetStart/2)+1, 
												(CASE @lv__curStatementOffsetEnd
													WHEN -1 THEN datalen_batch 
													ELSE @lv__curStatementOffsetEnd
													END - @lv__curStatementOffsetStart
												)/2 + 1
											) 
									END 
							) END
				FROM 
				(SELECT [dbid] = ISNULL(txt.dbid,@lv__nullsmallint), 
						[objectid] = ISNULL(txt.objectid,@lv__nullint), 
						[stmt_text] = ISNULL(txt.text, 'SQL batch info was NULL'), 
						[fail_to_obtain] = CASE WHEN txt.text IS NULL THEN 1 ELSE 0 END,
						[datalen_batch] = DATALENGTH(txt.text)
				FROM sys.dm_exec_sql_text(@lv__curHandle) txt) ss
			) outerquery
			;
		END TRY
		BEGIN CATCH
			INSERT INTO #t__stmt (
				sql_handle, 
				statement_start_offset, 
				statement_end_offset, 
				dbid, 
				objectid, 
				fail_to_obtain, 
				datalen_batch,
				stmt_text
			)	
			SELECT 
				@lv__curHandle, 
				@lv__curStatementOffsetStart, 
				@lv__curStatementOffsetEnd, 
				@lv__nullsmallint, 
				@lv__nullint, 
				1, 
				0, 
				'Error getting SQL statement text: ' + CONVERT(VARCHAR(20),ERROR_NUMBER()) + '; ' + ERROR_MESSAGE()
		END CATCH

		FETCH iterateTopPatterns_StmtText INTO 
			@lv__curHandle, @lv__curStatementOffsetStart, @lv__curStatementOffsetEnd;
	END

	CLOSE iterateTopPatterns_StmtText;
	DEALLOCATE iterateTopPatterns_StmtText;



	SET @errorloc = N'TopPatterns StmtPlan';
	DECLARE iterateTopPatterns_QueryPlan CURSOR STATIC LOCAL FORWARD_ONLY FOR 
	SELECT DISTINCT 
		t.plan_handle,
		t.statement_start_offset,
		t.statement_end_offset
	FROM #TopPatterns_RepresentativeStmts t
	WHERE t.plan_handle IS NOT NULL
	AND t.plan_handle <> 0x00
	;

	OPEN iterateTopPatterns_QueryPlan;
	FETCH iterateTopPatterns_QueryPlan INTO 
		@lv__curHandle, 
		@lv__curStatementOffsetStart,
		@lv__curStatementOffsetEnd;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @lv__PlansPulled = 1;

		--since I've seen cases where the statement_start_offset and statement_end_offset are a little funky, 
		-- do a bit of edge-case handling (where we'll just get the full plan instead)
		IF @lv__curStatementOffsetStart = 0 
		BEGIN
			IF @lv__curStatementOffsetEnd IN (0,-1)
			BEGIN
				SET @lv__usedStartOffset = 0;
				SET @lv__usedEndOffset = -1;
			END
			ELSE
			BEGIN
				SET @lv__usedStartOffset = 0;
				SET @lv__usedEndOffset = @lv__curStatementOffsetEnd;
			END
		END
		ELSE
		BEGIN
			SET @lv__usedStartOffset = @lv__curStatementOffsetStart;
			SET @lv__usedEndOffset = @lv__curStatementOffsetEnd;
		END

		BEGIN TRY
			INSERT INTO #t__stmtqp (
				[plan_handle],
				[statement_start_offset],
				[statement_end_offset],
				[dbid],
				[objectid],
				[fail_to_obtain],
				[query_plan],
				[aw_stmtplan_hash]
			)
			SELECT 
				curHandle,
				curstartoffset,
				curendoffset,
				[dbid], 
				objectid,
				fail_to_obtain,
				query_plan,
				aw_stmtplan_hash = HASHBYTES('MD5',
					(SUBSTRING(query_plan,1,3940) +
					CONVERT(nvarchar(40),CHECKSUM(query_plan)))
					)
			FROM (
				SELECT 
					curHandle = @lv__curHandle, 
					--Note that we store the offsets we were given, not the ones we actually used
					-- (@lv__usedStartOffset/EndOffset). This makes troubleshooting this code & resulting plans easier
					curstartoffset = @lv__curStatementOffsetStart, 
					curendoffset = @lv__curStatementOffsetEnd,
					[dbid] = ISNULL(dbid,@lv__nullsmallint),
					[objectid] = ISNULL(objectid,@lv__nullint),
					[fail_to_obtain] = CASE WHEN query_plan IS NULL THEN 1 ELSE 0 END, 
					[query_plan] = 
						CASE 
							WHEN s2.row_exists IS NULL 
								THEN '<?PlanError -- ' + CHAR(13) + CHAR(10) + 'Query Plan DMV did not return a row' + CHAR(13) + CHAR(10) + '-- ?>'
							WHEN s2.row_exists IS NOT NULL AND s2.query_plan IS NULL 
								THEN '<?PlanError -- ' + CHAR(13) + CHAR(10) + 'Statement Query Plan is NULL' + CHAR(13) + CHAR(10) + '-- ?>'
							ELSE s2.query_plan
						END
				FROM
					(SELECT 0 as col1) s
					LEFT OUTER JOIN 
					(SELECT 1 as row_exists, t.dbid, t.objectid, t.query_plan
						FROM sys.dm_exec_text_query_plan(@lv__curHandle, @lv__usedStartOffset, @lv__usedEndOffset) t) s2
						ON 1=1
			) s3;
		END TRY
		BEGIN CATCH
			INSERT INTO #t__stmtqp (
				[plan_handle],
				[statement_start_offset],
				[statement_end_offset],
				[dbid],
				[objectid],
				[fail_to_obtain],
				[query_plan],
				[aw_stmtplan_hash]
			)
			SELECT curHandle = @lv__curHandle,
				curstartoffset = @lv__curStatementOffsetStart,
				curendoffset = @lv__curStatementOffsetEnd,
				@lv__nullsmallint,
				@lv__nullint,
				1 as fail_to_obtain, 
				--'Error obtaining Statement Query Plan: ' + CONVERT(VARCHAR(20),ERROR_NUMBER()) + '; ' + ERROR_MESSAGE()
				N'<?PlanError -- ' + NCHAR(13) + NCHAR(10) + N'Error obtaining Statement Query Plan: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + N'; ' + ISNULL(ERROR_MESSAGE(),N'<null>') + NCHAR(13) + NCHAR(10) + N'-- ?>',
				HASHBYTES('MD5', 
					N'<?PlanError -- ' + NCHAR(13) + NCHAR(10) + N'Error obtaining Statement Query Plan: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + N'; ' + ISNULL(ERROR_MESSAGE(),N'<null>') + NCHAR(13) + NCHAR(10) + N'-- ?>'
					);
		END CATCH 

		FETCH iterateTopPatterns_QueryPlan INTO 
			@lv__curHandle, 
			@lv__curStatementOffsetStart,
			@lv__curStatementOffsetEnd;
	END

	CLOSE iterateTopPatterns_QueryPlan;
	DEALLOCATE iterateTopPatterns_QueryPlan;

	SET LOCK_TIMEOUT -1;

	IF @lv__StatementsPulled = CONVERT(BIT,1)
	BEGIN
		SET @errorloc = N'TopPatterns Stmt Persist';
		MERGE DMViewerCore.SQLStmtStore perm
			USING #t__stmt t
				ON perm.sql_handle = t.sql_handle
				AND perm.statement_start_offset = t.statement_start_offset
				AND perm.statement_end_offset = t.statement_end_offset
		WHEN MATCHED THEN UPDATE 
			-- #t__stmt has a match in the store. If this occurred, it means
			-- that the initial population (above) of #TopObjects_StmtStats did not find
			-- a match, and thus fail_to_obtain must have been 1 (or the DBID/OBJECT
			-- ID values didn't match, but we don't expect that to occur). 
			-- Update all of the attributes of the existing entry in the store.
			SET perm.dbid = t.dbid, 
				perm.objectid = t.objectid,
				perm.fail_to_obtain = t.fail_to_obtain,
				perm.datalen_batch = t.datalen_batch,
				perm.stmt_text = t.stmt_text,
				perm.LastTouchedBy_SPIDCaptureTime = @CollectionTime
		WHEN NOT MATCHED BY TARGET THEN 
			--new entry
			INSERT (sql_handle, statement_start_offset, statement_end_offset, 
				dbid, objectid, fail_to_obtain, datalen_batch, stmt_text, 
				Insertedby_SPIDCaptureTime, LastTouchedBy_SPIDCaptureTime)
			VALUES (t.sql_handle, t.statement_start_offset, t.statement_end_offset,
				t.dbid, t.objectid, t.fail_to_obtain, t.datalen_batch, t.stmt_text, 
				@CollectionTime, @CollectionTime)
		;

		SET @errorloc = N'TopPatterns Stmt PK';
		UPDATE targ 
		SET targ.FKSQLStmtStoreID = sss.PKSQLStmtStoreID
		FROM #TopPatterns_RepresentativeStmts targ 
			INNER hash JOIN DMViewerCore.SQLStmtStore sss
				ON sss.sql_handle = targ.sql_handle
				AND sss.statement_start_offset = targ.statement_start_offset
				AND sss.statement_end_offset = targ.statement_end_offset
				--AND sss.dbid = targ.database_id
				--AND sss.objectid = targ.object_id
		WHERE targ.FKSQLStmtStoreID IS NULL
			--this lets us IxSeek quickly to the rows in SQLStmtStore that we just updated/inserted
		AND sss.LastTouchedBy_SPIDCaptureTime = @CollectionTime
		OPTION(FORCE ORDER, MAXDOP 1);
	END


	IF @lv__PlansPulled = CONVERT(BIT,1)
	BEGIN
		--the unique key here is the hash + the handle + the offsets. (but not dbid & objectid)
		SET @errorloc = N'TopPatterns Plan Persist';
		MERGE DMViewerCore.QueryPlanStmtStore perm
			USING #t__stmtqp t
				ON perm.AWStmtPlanHash = t.aw_stmtplan_hash
				AND perm.plan_handle = t.plan_handle
				AND perm.statement_start_offset = t.statement_start_offset
				AND perm.statement_end_offset = t.statement_end_offset
		WHEN MATCHED THEN UPDATE
			--We overwrite dbid/objectid b/c I don't have 100% certainty that a plan_handle/offset combo, with
			-- the plan hash, will ALWAYS have the same dbid/objectid (though it seems mathematically almost 
			-- impossible to have a collision here).
			SET perm.LastTouchedBy_SPIDCaptureTime = @CollectionTime, 
				perm.dbid = t.dbid, 
				perm.objectid = t.objectid
		WHEN NOT MATCHED THEN
			INSERT (AWStmtPlanHash, plan_handle, statement_start_offset, statement_end_offset, 
				dbid, objectid, fail_to_obtain, query_plan, 
				Insertedby_SPIDCaptureTime, LastTouchedBy_SPIDCaptureTime)
			VALUES (t.aw_stmtplan_hash, t.plan_handle, t.statement_start_offset, t.statement_end_offset,
				t.dbid, t.objectid, t.fail_to_obtain, t.query_plan, 
				@CollectionTime, @CollectionTime)
		;

		SET @errorloc = N'TopPatterns Plan PK';
		UPDATE targ
		SET targ.FKQueryPlanStmtStoreID = qps.PKQueryPlanStmtStoreID
		FROM #TopPatterns_RepresentativeStmts targ 
			INNER JOIN DMViewerCore.QueryPlanStmtStore qps
				ON qps.plan_handle = targ.plan_handle
				AND qps.statement_start_offset = targ.statement_start_offset
				AND qps.statement_end_offset = targ.statement_end_offset
		WHERE qps.LastTouchedBy_SPIDCaptureTime = @CollectionTime
		;
	END

	SET @errorloc = N'QueryPattern RepStmt Final Persist';
	INSERT INTO [ServerEye].[QueryPatterns_RepresentativeStmts] (
		CollectionTime,						--1
		cacheobjtype, 
		objtype, 
		query_hash, 
		sql_handle,							--5
		statement_start_offset, 
		statement_end_offset, 
		plan_handle, 
		plan_generation_num, 
		plan_generation_num_delta,			--10
		creation_time, 
		last_execution_time, 
		execution_count, 
		execution_count_delta, 
		total_worker_time,					--15
		total_worker_time_delta, 
		last_worker_time, 
		min_worker_time, 
		max_worker_time, 
		total_physical_reads,				--20
		total_physical_reads_delta, 
		last_physical_reads, 
		min_physical_reads, 
		max_physical_reads, 
		total_logical_writes,				--25
		total_logical_writes_delta, 
		last_logical_writes, 
		min_logical_writes, 
		max_logical_writes, 
		total_logical_reads,				--30
		total_logical_reads_delta, 
		last_logical_reads, 
		min_logical_reads, 
		max_logical_reads, 
		total_clr_time,						--35
		total_clr_time_delta, 
		last_clr_time, 
		min_clr_time, 
		max_clr_time, 
		total_elapsed_time,					--40
		total_elapsed_time_delta, 
		last_elapsed_time, 
		min_elapsed_time, 
		max_elapsed_time, 
		query_plan_hash,				--45
		total_rows, 
		total_rows_delta, 
		last_rows, 
		min_rows, 
		max_rows,					--50
		refcounts, 
		usecounts, 
		size_in_bytes, 
		pool_id, 
		parent_plan_handle,			--55
		FKSQLStmtStoreID, 
		FKQueryPlanStmtStoreID
	)
	SELECT @CollectionTime,				--1
		[cacheobjtype],
		[objtype],
		[query_hash],
		[sql_handle],					--5
		[statement_start_offset],
		[statement_end_offset],
		[plan_handle],
		[plan_generation_num],
		[plan_generation_num_delta],	--10
		[creation_time],
		[last_execution_time],
		[execution_count],
		[execution_count_delta],
		[total_worker_time],			--15
		[total_worker_time_delta],
		[last_worker_time],
		[min_worker_time],
		[max_worker_time],
		[total_physical_reads],			--20
		[total_physical_reads_delta],
		[last_physical_reads],
		[min_physical_reads],
		[max_physical_reads],
		[total_logical_writes],			--25
		[total_logical_writes_delta],
		[last_logical_writes],
		[min_logical_writes],
		[max_logical_writes],
		[total_logical_reads],			--30
		[total_logical_reads_delta],
		[last_logical_reads],
		[min_logical_reads],
		[max_logical_reads],
		[total_clr_time],				--35
		[total_clr_time_delta],
		[last_clr_time],
		[min_clr_time],
		[max_clr_time],
		[total_elapsed_time],			--40
		[total_elapsed_time_delta],
		[last_elapsed_time],
		[min_elapsed_time],
		[max_elapsed_time],
		[query_plan_hash],			--45
		[total_rows],
		[total_rows_delta],
		[last_rows],
		[min_rows],
		[max_rows],					--50
		[refcounts],
		[usecounts],
		[size_in_bytes],
		[pool_id],
		[parent_plan_handle],		--55
		FKSQLStmtStoreID,
		FKQueryPlanStmtStoreID
	FROM #TopPatterns_RepresentativeStmts
	;


	--Ok, let's switch our table identifier and truncate the old table
	IF @lv__CurrentTable = N'A'
	BEGIN
		--We just populated A and B was the "previous run". Thus we truncate B
		SET @errorloc = N'B TRUNCATE';
		TRUNCATE TABLE ServerEye.QueryPatternStats__B;
		TRUNCATE TABLE ServerEye.dm_exec_query_stats__B;
		TRUNCATE TABLE ServerEye.dm_exec_object_stats__B;
	END
	ELSE
	BEGIN
		--We just populated B and A was the "previous run". Thus we truncate A
		SET @errorloc = N'A TRUNCATE';
		TRUNCATE TABLE ServerEye.QueryPatternStats__A;
		TRUNCATE TABLE ServerEye.dm_exec_query_stats__A;
		TRUNCATE TABLE ServerEye.dm_exec_object_stats__A;
	END

	SET @errorloc = N'Table Switch';
	UPDATE targ 
	SET CurrentTable = CASE WHEN @lv__CurrentTable = N'A' THEN N'B' ELSE N'A' END
	FROM ServerEye.TableSwitcher targ WITH (FORCESEEK)
	WHERE targ.SwitchName = @const__SwitchName;

	COMMIT TRANSACTION
	

	RETURN 0;
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK;

	SET @errorsev = ERROR_SEVERITY();
	SET @errorstate = ERROR_STATE();

	SET @errormsg = N'Exception occurred in IntervalMetrics_TopQueries at location "' + ISNULL(@errorloc,N'<null>') + N'". 
		Error #: ' + CONVERT(NVARCHAR(20),ERROR_NUMBER()) + 
		N'; Severity: ' + CONVERT(NVARCHAR(20),@errorsev) + 
		N'; State: ' + CONVERT(NVARCHAR(20),@errorstate) + 
		N'; Message: ' + ERROR_MESSAGE()
		;

	RAISERROR(@errormsg, @errorsev, @errorstate);
	RETURN -1;
END CATCH

END