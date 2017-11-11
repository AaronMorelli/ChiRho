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
	@UTCCaptureTime		DATETIME
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	



	RETURN 0;
END
GO