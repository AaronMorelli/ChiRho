SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[DataByTime] 
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

	FILE NAME: AutoWho.DataByTime.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.DataByTime

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Just dumps out data for each table organized by time. Mainly for quick data collection review during development.

To Execute
------------------------
EXEC AutoWho.DataByTime
*/
AS
BEGIN
	SET NOCOUNT ON;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.SignalTable' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT * 
		FROM [AutoWho].[SignalTable]
		) d
		ON 1=1;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.ThresholdFilterSpids' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT *
		FROM [AutoWho].[ThresholdFilterSpids]
		) d
		ON 1=1;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.Log' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT CONVERT(DATE, LogDT) as LogDT, TraceID, COUNT(*) as NumRows
			FROM [AutoWho].[Log]
			GROUP BY CONVERT(DATE, LogDT), TraceID
		) d
		ON 1=1
	ORDER BY d.LogDT ASC, d.TraceID;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'CoreXR.Traces' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT *
		FROM [CoreXR].[Traces]
		WHERE Utility = N'AutoWho'
		) d
		ON 1=1
	ORDER BY d.TraceID;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'CoreXR.CaptureOrdinalCache' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT DISTINCT t.StartTime, t.EndTime
			FROM [CoreXR].[CaptureOrdinalCache] t
			WHERE t.Utility = N'AutoWho'
		) d
		ON 1=1
	ORDER BY d.StartTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.CaptureSummary' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT CONVERT(DATE,cs.SPIDCaptureTime) as CaptureDT
			FROM [AutoWho].[CaptureSummary] cs
		) d
		ON 1=1
	ORDER BY d.CaptureDT;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.CaptureTimes' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT CONVERT(DATE,ct.SPIDCaptureTime) as CaptureDT
			FROM [AutoWho].[CaptureTimes] ct
		) d
		ON 1=1
	ORDER BY d.CaptureDT;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.LightweightSessions' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT l.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[LightweightSessions] l
			GROUP BY l.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.LightweightTasks' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT l.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[LightweightTasks] l
			GROUP BY l.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.LightweightTrans' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT l.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[LightweightTrans] l
			GROUP BY l.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.BlockingGraphs' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[BlockingGraphs] bg
			GROUP BY SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.LockDetails' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[LockDetails]
			GROUP BY SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.TransactionDetails' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT t.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[TransactionDetails] t
			GROUP BY t.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.SessionsAndRequests' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT sar.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[SessionsAndRequests] sar
			GROUP BY sar.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.TasksAndWaits' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT taw.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[TasksAndWaits] taw
			GROUP BY taw.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.InsertHour, COUNT(InsertHour) as NumRows
	FROM
	(SELECT 'CoreXR.InputBufferStore' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT InsertHour = CONVERT(varchar(30),CONVERT(DATE, InsertedBy_UTCCaptureTime)) + 
							' ' + CONVERT(varchar(30),DATEPART(HOUR, InsertedBy_UTCCaptureTime)) + ':00'
			FROM [CoreXR].[InputBufferStore]
		) d
		ON 1=1
	GROUP BY tb.TableName, d.InsertHour
	ORDER BY d.InsertHour;

	SELECT tb.TableName, d.InsertHour, COUNT(InsertHour) as NumRows
	FROM
	(SELECT 'CoreXR.QueryPlanBatchStore' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT InsertHour = CONVERT(varchar(30),CONVERT(DATE, InsertedBy_UTCCaptureTime)) + 
						' ' + CONVERT(varchar(30),DATEPART(HOUR, InsertedBy_UTCCaptureTime)) + ':00'
		FROM [CoreXR].[QueryPlanBatchStore]
		) d
		ON 1=1
	GROUP BY tb.TableName, d.InsertHour
	ORDER BY d.InsertHour;

	SELECT tb.TableName, d.InsertHour, COUNT(InsertHour) as NumRows
	FROM
	(SELECT 'CoreXR.QueryPlanStmtStore' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT InsertHour = CONVERT(varchar(30),CONVERT(DATE, InsertedBy_UTCCaptureTime)) + 
						' ' + CONVERT(varchar(30),DATEPART(HOUR, InsertedBy_UTCCaptureTime)) + ':00'
		FROM [CoreXR].[QueryPlanStmtStore]
		) d
		ON 1=1
	GROUP BY tb.TableName, d.InsertHour
	ORDER BY d.InsertHour;

	SELECT tb.TableName, d.InsertHour, COUNT(InsertHour) as NumRows
	FROM
	(SELECT 'CoreXR.SQLBatchStore' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT InsertHour = CONVERT(varchar(30),CONVERT(DATE, InsertedBy_UTCCaptureTime)) + 
						' ' + CONVERT(varchar(30),DATEPART(HOUR, InsertedBy_UTCCaptureTime)) + ':00'
		FROM [CoreXR].[SQLBatchStore]
		) d
		ON 1=1
	GROUP BY tb.TableName, d.InsertHour
	ORDER BY d.InsertHour;

	SELECT tb.TableName, d.InsertHour, COUNT(InsertHour) as NumRows
	FROM
	(SELECT 'CoreXR.SQLStmtStore' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT InsertHour = CONVERT(varchar(30),CONVERT(DATE, InsertedBy_UTCCaptureTime)) + 
						' ' + CONVERT(varchar(30),DATEPART(HOUR, InsertedBy_UTCCaptureTime)) + ':00'
		FROM [CoreXR].[SQLStmtStore]
		) d
		ON 1=1
	GROUP BY tb.TableName, d.InsertHour
	ORDER BY d.InsertHour;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimCommand' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimCommand]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimConnectionAttribute' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimConnectionAttribute]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimLoginName' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimLoginName]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimNetAddress' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimNetAddress]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimSessionAttribute' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimSessionAttribute]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimWaitType' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimWaitType]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	RETURN 0;
END
GO
