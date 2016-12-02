SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ResetAutoWhoData]
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

	FILE NAME: AutoWho.ResetAutoWhoData.StoredProcedure.sql

	PROCEDURE NAME: AutoWho.ResetAutoWhoData

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: Clear out/reset all "collected" data in the AutoWho tables so that we can start testing
			over again. This proc is primarily aimed at development/testing

	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
exec AutoWho.ResetAutoWhoData @DeleteConfig=N'N'
*/
(
	@DeleteConfig NCHAR(1)=N'N'
)
AS
BEGIN
	SET NOCOUNT ON;

	IF @DeleteConfig IS NULL OR UPPER(@DeleteConfig) NOT IN (N'N', N'Y')
	BEGIN
		SET @DeleteConfig = N'N';
	END

	TRUNCATE TABLE [AutoWho].[LightweightSessions];
	TRUNCATE TABLE [AutoWho].[LightweightTasks];
	TRUNCATE TABLE [AutoWho].[LightweightTrans];

	TRUNCATE TABLE [AutoWho].[LockDetails];
	TRUNCATE TABLE [AutoWho].[TransactionDetails];
	TRUNCATE TABLE [AutoWho].[TasksAndWaits];
	TRUNCATE TABLE [AutoWho].[SessionsAndRequests];
	TRUNCATE TABLE [AutoWho].[BlockingGraphs];
	TRUNCATE TABLE [AutoWho].[ThresholdFilterSpids];
	TRUNCATE TABLE [AutoWho].[SARException];
	TRUNCATE TABLE [AutoWho].[TAWException];
	TRUNCATE TABLE [AutoWho].[SignalTable];

	--We have pre-reserved certain ID values for certain dimension members, so we need to keep those.
	DELETE FROM [AutoWho].[DimCommand] WHERE DimCommandID > 3;
	DELETE FROM [AutoWho].[DimConnectionAttribute] WHERE DimConnectionAttributeID > 1;
	DELETE FROM [AutoWho].[DimLoginName] WHERE DimLoginNameID > 2;
	DELETE FROM [AutoWho].[DimNetAddress] WHERE DimNetAddressID > 2;
	DELETE FROM [AutoWho].[DimSessionAttribute] WHERE DimSessionAttributeID > 1;
	DELETE [AutoWho].[DimWaitType] WHERE DimWaitTypeID > 2;

	DELETE FROM [CoreXR].[OrdinalCachePosition] WHERE Utility IN (N'AutoWho',N'SessionViewer',N'QueryProgress');
	DELETE FROM [CoreXR].[CaptureOrdinalCache] WHERE Utility IN (N'AutoWho', N'SessionViewer', N'QueryProgress');
	DELETE FROM [CoreXR].[Traces] WHERE Utility = N'AutoWho';

	TRUNCATE TABLE [AutoWho].[CaptureSummary];
	TRUNCATE TABLE [AutoWho].[CaptureTimes];
	TRUNCATE TABLE [AutoWho].[Log];

	IF @DeleteConfig = N'Y'
	BEGIN
		TRUNCATE TABLE [AutoWho].[CollectorOptFakeout];
		TRUNCATE TABLE [AutoWho].[Options];
		TRUNCATE TABLE [AutoWho].[Options_History];
		TRUNCATE TABLE [CoreXR].[Version];
		TRUNCATE TABLE [CoreXR].[Version_History];

		TRUNCATE TABLE [AutoWho].[DimCommand];
		TRUNCATE TABLE [AutoWho].[DimConnectionAttribute];
		TRUNCATE TABLE [AutoWho].[DimLoginName];
		TRUNCATE TABLE [AutoWho].[DimNetAddress];
		TRUNCATE TABLE [AutoWho].[DimSessionAttribute];
		TRUNCATE TABLE [AutoWho].[DimWaitType];
	END

	RETURN 0;
END
GO
