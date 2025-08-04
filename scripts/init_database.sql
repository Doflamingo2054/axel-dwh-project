/* 
This script create a new Database named 'DataWarehouse' 
and sets up three schemas within the database : 'bronze' , 'silver' , 'gold'
*/

--- Create the Database 'DataWarehouse' 
CREATE DATABASE DataWarehouse;

--- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

