USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_XR_QueryCamera]
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

	FILE NAME: sp_XR_QueryCamera__TEMPLATE.sql

	PROCEDURE NAME: sp_XR_QueryCamera

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE:

	FUTURE ENHANCEMENTS: 
		
To Execute
------------------------

*/
(
	@spid				INT,
	@request			INT=0,
	@frequency			INT=2,			-- every X seconds the capture runs
	@captures			INT=NULL,		-- if NULL or 0, run until the query ends; if > 0, run that many iterations
	@wait				INT=10,
	@allcaptures		NCHAR(1)=N'N',	-- if code is likely to enter sub-calls and then exit back to the main query
										-- (e.g. scalar functions), then specifying Y here tells the code that once
										-- it sees the query, it will execute @capture number of captures regardless
										-- of what it finds.
	@PKSQLStmtStoreID	BIGINT=NULL
)
AS
BEGIN
	SET NOCOUNT ON;

	IF ISNULL(@spid,-1) <= 0 
	BEGIN
		RAISERROR('Parameter @spid must be > 0.', 16, 1);
		RETURN -1;
	END

	IF ISNULL(@request,-1) <= 0 
	BEGIN
		RAISERROR('Parameter @request must be > 0.', 16, 1);
		RETURN -1;
	END

	IF NOT(ISNULL(@frequency,-1) BETWEEN 1 AND 60)
	BEGIN
		RAISERROR('Parameter @frequency must be between 1 and 60.', 16, 1);
		RETURN -1;
	END

	IF @captures IS NULL
	BEGIN
		SET @captures = 0;
	END
	ELSE
	BEGIN
		IF @captures < 0 OR @captures > 1000
		BEGIN
			RAISERROR('Parameter @captures cannot be < 0. Valid values are NULL, 0, or a positive number <= 1000.', 16, 1);
			RETURN -1;
		END
	END

	IF @wait IS NULL
	BEGIN
		SET @wait = 0;
	END
	ELSE
	BEGIN
		IF @wait < 0
		BEGIN
			RAISERROR('Parameter @wait must be NULL or 0 (no wait) or > 0 (wait # of seconds).', 16, 1);
			RETURN -1;
		END
	END

	IF @allcaptures IS NULL
	BEGIN
		RAISERROR('Parameter @allcaptures cannot be NULL.', 16, 1);
		RETURN -1;
	END

	SET @allcaptures = UPPER(@allcaptures);

	IF @allcaptures NOT IN (N'N', N'Y')
	BEGIN
		RAISERROR('Parameter @allcaptures must be either Y or N.', 16, 1);
		RETURN -1;
	END

	IF @allcaptures = N'Y' AND @captures = 0
	BEGIN
		RAISERROR('A positive number for the @captures parameter must be specified if @allcaptures is set to Y.', 16, 1);
		RETURN -1;
	END

	IF @PKSQLStmtStoreID <= 0
	BEGIN
		RAISERROR('Parameter @PKSQLStmtStoreID must be > 0, and should be a valid entry in the AutoWho statement store.', 16, 1);
		RETURN -1;
	END


	EXEC @@XRDATABASENAME@@.AutoWho.QueryCamera @spid=@spid, @request=@request, @frequency=@frequency,
									@captures=@captures, @wait=@wait, 
									@allcaptures = @allcaptures, @PKSQLStmtStoreID = @PKSQLStmtStoreID; 


	RETURN 0;
END
GO
