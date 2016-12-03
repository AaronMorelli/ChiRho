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

	FILE NAME: DBExistenceCheck.sql

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: The installer Powershell scripts outsource the DB existence checking
	to this script.
*/
DECLARE @DBN_input NVARCHAR(256),
		@DBExists NVARCHAR(20),
		@ExceptionMessage NVARCHAR(4000),
		@DynSQL NVARCHAR(4000);
SET @DBN_input = N'$(DBName)';
SET @DBExists = N'$(DBExists)'

IF @DBN_input IS NULL
BEGIN
	RAISERROR('Script input variable DBName cannot be null.', 16,1)
END
ELSE
BEGIN
	IF EXISTS (SELECT * FROM sys.databases d
					WHERE d.name = @DBN_input)
	BEGIN
		IF @DBExists = N'N'
		BEGIN
			--We were told that it doesn't.
			SET @ExceptionMessage = N'Database "' + @DBN_input + N'" already exists but -DBExists was set to N (or defaulted to N)'
			RAISERROR(@ExceptionMessage, 16, 1);
		END
		--else, exit quietly
	END
	ELSE
	BEGIN
		IF @DBExists = N'Y'
		BEGIN
			--We were told that it does.
			SET @ExceptionMessage = N'Database "' + @DBN_input + N'" does not exist but -DBExists was set to Y'
			RAISERROR(@ExceptionMessage, 16, 1);
		END
		--else, exit quietly
	END
END
