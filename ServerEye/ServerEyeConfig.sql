DECLARE @DaysToKeep_str NVARCHAR(256);
DECLARE @DaysToKeep INT;

SET @DaysToKeep_str = '30';
SET @DaysToKeep = CONVERT(INT, @DaysToKeep_str); 

EXEC ServerEye.InsertConfigData @DaysToKeep = @DaysToKeep;
