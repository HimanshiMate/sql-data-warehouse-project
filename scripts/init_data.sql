/*=========================================================
  Don't create same name of database. 
  Database name should be unique.if you have same name of database 
  then you will drop the old database and recreate the new database 
========================================================*/

--------------create database-------
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO
---------create Schemas------------
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
