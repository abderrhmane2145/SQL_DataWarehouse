/*
===================================================
Create Database & Schemas
===================================================

Script Purposes : 
  By Running This Script You Will Create A New Database 'DataWarehouse' After Drop Database Called 'DataWarehouse' If It Exists.
  Additionally You Will Create These Schemas Within Database: bronez, silver and gold
Warning :
  Running This Scripts Will Drop Entire Database 'DataWarehouse' If It Exists.
  All Data In Database 'DataWarehouse' Will Be Deleted Permanently.
  So Ensure You Have A Propre Backups Before Running This Scirpt.
*/
USE master;
GO 

-- DROP Database 'DataWarehouse' If It Exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
   ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
   DROP DATABASE DataWarehouse;
END
GO 

-- Create Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create The Schemas 
CREATE SCHEMA bronze;
GO 
        
CREATE SCHEMA silver;
GO 
        
CREATE SCHEMA gold;
GO  
