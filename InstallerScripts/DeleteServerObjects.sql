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

	FILE NAME: DeleteServerObjects.sql

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Deletes server objects when called by the Powershell uninstaller script.
*/
DECLARE @DBN_input NVARCHAR(256),
		@AutoWhoTraceJobName NVARCHAR(256),
		@XRMasterJobName NVARCHAR(256),
		@ExceptionMessage NVARCHAR(4000),
		@jid uniqueidentifier,
		@DynSQL NVARCHAR(4000);
SET @DBN_input = N'$(DBName)';
SET @AutoWhoTraceJobName = @DBN_input +  N' - AlwaysDisabled - AutoWho Trace';
SET @XRMasterJobName = @DBN_input +  N' - Every 15 Min - Daily - ChiRho Master';

IF @DBN_input IS NULL
BEGIN
	RAISERROR('Parameter "Database" cannot be null.', 16,1);
END
ELSE
BEGIN
	SET @jid = (SELECT j.job_id FROM msdb.dbo.sysjobs j WHERE j.name = @AutoWhoTraceJobName);
	IF @jid IS NOT NULL
	BEGIN
		EXEC msdb.dbo.sp_delete_job @job_id=@jid, @delete_unused_schedule=1
	END

	SET @jid = (SELECT j.job_id FROM msdb.dbo.sysjobs j WHERE j.name = @XRMasterJobName);
	IF @jid IS NOT NULL
	BEGIN
		EXEC msdb.dbo.sp_delete_job @job_id=@jid, @delete_unused_schedule=1
	END
END
GO

IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_XR_JobMatrix')
BEGIN
	DROP PROCEDURE dbo.sp_XR_JobMatrix;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_XR_LongRequests')
BEGIN
	DROP PROCEDURE dbo.sp_XR_LongRequests;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_XR_FileUsage')
BEGIN
	DROP PROCEDURE dbo.sp_XR_FileUsage;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_XR_QueryCamera')
BEGIN
	DROP PROCEDURE dbo.sp_XR_QueryCamera;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_XR_QueryProgress')
BEGIN
	DROP PROCEDURE dbo.sp_XR_QueryProgress;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_XR_SessionSummary')
BEGIN
	DROP PROCEDURE dbo.sp_XR_SessionSummary;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_XR_SessionViewer')
BEGIN
	DROP PROCEDURE dbo.sp_XR_SessionViewer;
END