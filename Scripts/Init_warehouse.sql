/*
Create Database and Schemas
=============================================================
Script Function:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    Additionally, the script sets up three schemas for each layer within the database: 'Bronze', 'Silver', and 'Gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

--Creating the Data warehouse
CREATE DATABASE DataWarehouse;
GO

--Creating the Schemas for the layers
CREATE SCHEMA Bronze
GO

CREATE SCHEMA Silver
GO

CREATE SCHEMA Gold
GO
