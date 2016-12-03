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

	FILE NAME: CreateXRDatabase.sql

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com

	PURPOSE: Create the database with a name passed in by the installation scripts
*/
DECLARE @DBN_input NVARCHAR(256),
		@ExceptionMessage NVARCHAR(4000),
		@DynSQL NVARCHAR(4000);
SET @DBN_input = '$(DBName)';

IF @DBN_input IS NULL
BEGIN
	RAISERROR('Parameter "Database" cannot be null.', 16,1)
END
ELSE
BEGIN
	IF EXISTS (SELECT * FROM sys.databases d
					WHERE d.name = @DBN_input)
	BEGIN
		SET @ExceptionMessage = N'Database "' + @DBN_input + N'" already exists.'
		RAISERROR(@ExceptionMessage, 16, 1);
	END
	ELSE
	BEGIN
		SET @DynSQL = N'CREATE DATABASE ' + @DBN_input;
		EXEC (@DynSQL);
	END
END
