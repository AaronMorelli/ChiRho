SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [ServerEye].[PrePopulateDimensions]
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

	FILE NAME: ServerEye.PrePopulateDimensions.StoredProcedure.sql

	PROCEDURE NAME: ServerEye.PrePopulateDimensions

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Adds new rows (if any are found) to various dimension tables so that those values are present when
		the various ServerEye collection queries run.

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
EXEC ServerEye.PrePopulateDimensions

*/
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE
		@lv__nullstring						NVARCHAR(8),
		@lv__nullint						INT,
		@lv__nullsmallint					SMALLINT;

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" 
											-- would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value

	IF OBJECT_ID('tempdb..#DistinctWaitTypes1') IS NOT NULL
	BEGIN
		DROP TABLE #DistinctWaitTypes1;
	END
	SELECT w.wait_type
	INTO #DistinctWaitTypes1
	FROM sys.dm_os_wait_stats w;

	INSERT INTO ServerEye.DimWaitType (
		wait_type, 
		isBenign
	)
	SELECT DISTINCT 
		w.wait_type, 
		[isBenign] = 0	--TODO: set this logic appropriately
	FROM #DistinctWaitTypes1 w
	WHERE NOT EXISTS (
		SELECT * FROM ServerEye.DimWaitType dwt
		WHERE dwt.wait_type = w.wait_type
	);

	INSERT INTO ServerEye.DimLatchClass (
		latch_class,
		[IsBenign]
	)
	SELECT DISTINCT 
		ls.latch_class, 
		0
	FROM sys.dm_os_latch_stats ls
	WHERE NOT EXISTS (
		SELECT * FROM ServerEye.DimLatchClass l
		WHERE l.latch_class = ls.latch_class
	);

	INSERT INTO [ServerEye].[DimSpinlock] (
		[SpinlockName],
		[IsBenign]
	)
	SELECT DISTINCT
		s.name,
		0
	FROM sys.dm_os_spinlock_stats s
	WHERE NOT EXISTS (
		SELECT * FROM ServerEye.DimSpinlock dsl
		WHERE dsl.SpinlockName = s.name
	);

	INSERT INTO [ServerEye].[DimDBVolume](
		[volume_id],
		[volume_mount_point],
		[logical_volume_name],
		[file_system_type],
		[supports_compression],
		[supports_alternate_streams],
		[supports_sparse_files],
		[is_read_only],
		[is_compressed]
	)
	SELECT 
		ss.volume_id,
		ss.volume_mount_point,
		ss.logical_volume_name,
		ss.file_system_type,
		ss.supports_compression,
		ss.supports_alternate_streams,
		ss.supports_sparse_files,
		ss.is_read_only,
		ss.is_compressed
	FROM (
		SELECT DISTINCT
			[volume_id] = ISNULL(vs.volume_id,N'<null>'),
			[volume_mount_point] = ISNULL(vs.volume_mount_point,N'<null>'),
			[logical_volume_name] = ISNULL(vs.logical_volume_name,N'<null>'),
			[file_system_type] = ISNULL(vs.file_system_type,N'<null>'),
			[supports_compression] = ISNULL(vs.supports_compression,255),
			[supports_alternate_streams] = ISNULL(vs.supports_alternate_streams,255),
			[supports_sparse_files] = ISNULL(vs.supports_sparse_files,255),
			[is_read_only] = ISNULL(vs.is_read_only,255),
			[is_compressed] = ISNULL(vs.is_compressed,255)
		FROM sys.master_files mf
			CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) vs
	) ss
	WHERE NOT EXISTS (
		SELECT * 
		FROM ServerEye.DimDBVolume dbv
		WHERE ss.volume_id = dbv.volume_id
		AND ss.volume_mount_point = dbv.volume_mount_point
		AND ss.logical_volume_name = dbv.logical_volume_name
		AND ss.file_system_type = dbv.file_system_type
		AND ss.supports_compression = dbv.supports_compression
		AND ss.supports_alternate_streams = dbv.supports_alternate_streams
		AND ss.supports_sparse_files = dbv.supports_sparse_files
		AND ss.is_read_only = dbv.is_read_only
		AND ss.is_compressed = dbv.is_compressed
	);

	RETURN 0;
END
GO
