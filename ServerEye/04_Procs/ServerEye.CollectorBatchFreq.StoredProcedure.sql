SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [ServerEye].[CollectorBatchFreq]
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

	FILE NAME: ServerEye.CollectorBatchFreq.StoredProcedure.sql

	PROCEDURE NAME: CollectorBatchFreq.Collector

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Runs on a schedule (or initiated by a user via a viewer proc) and calls various sub-procs to gather miscellaneous 
		server-level DMV data. Collects data for metrics that do not need to be captured very frequently (by default every 30 minutes)

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

	--Probably need a way to turn this off for larger systems.
	INSERT INTO [ServerEye].[BufDescriptors] (
		[UTCCaptureTime],
		[database_id],
		[file_id],
		[allocation_unit_id],
		[page_type],
		[numa_node],
		[NumModified],
		[SumRowCount],
		[SumFreeSpaceInBytes],
		[NumRows]
	)
	SELECT 
		@UTCCaptureTime
		database_id,
		file_id,
		allocation_unit_id,
		page_type,
		numa_node,
		NumModified = SUM(CASE WHEN is_modified = 1 THEN CONVERT(INT,1) ELSE CONVERT(INT,0) END),
		SumRowCount = SUM(CONVERT(BIGINT,row_count)),
		SumFreeSpaceInBytes = SUM(CONVERT(BIGINT,free_space_in_bytes)),
		NumRows = COUNT(*)
	FROM (
		SELECT 
			database_id = ISNULL(buf.database_id,-1),
			file_id = ISNULL(buf.file_id,-1),
			allocation_unit_id = CASE WHEN buf.database_id = 2 THEN -5 ELSE ISNULL(buf.allocation_unit_id,-1) END,
			page_type = ISNULL(buf.page_type,''),
			numa_node = ISNULL(buf.numa_node,-1),
			buf.is_modified,
			buf.row_count,
			buf.free_space_in_bytes
		FROM sys.dm_os_buffer_descriptors buf
		WHERE buf.database_id NOT IN (1, 3, 4)
		AND buf.page_type NOT IN ('BOOT_PAGE','FILEHEADER_PAGE','SYSCONFIG_PAGE')
	) ss
	GROUP BY database_id,
		file_id,
		allocation_unit_id,
		page_type,
		numa_node
	HAVING COUNT(*) > 10*128		--Num MB * 128 (pages) to filter down to just alloc units that are larger memory hogs. This should be a config option



	RETURN 0;
END
GO