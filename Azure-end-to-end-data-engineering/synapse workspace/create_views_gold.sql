------------------------
-- CREATE VIEW: CALENDAR
------------------------
CREATE VIEW gold.calendar AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adventureworksstorage.blob.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) AS rows;
GO

------------------------
-- CREATE VIEW: CUSTOMERS
------------------------
CREATE VIEW gold.customers AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adventureworksstorage.blob.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
) AS rows;
GO

------------------------
-- CREATE VIEW: PRODUCTS
------------------------
CREATE VIEW gold.products AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adventureworksstorage.blob.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT = 'PARQUET'
) AS rows;
GO

------------------------
-- CREATE VIEW: RETURNS
------------------------
CREATE VIEW gold.returns AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adventureworksstorage.blob.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) AS rows;
GO

------------------------
-- CREATE VIEW: SALES
------------------------
CREATE VIEW gold.sales AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adventureworksstorage.blob.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) AS rows;
GO

------------------------
-- CREATE VIEW: SUBCATEGORIES
------------------------
CREATE VIEW gold.subcat AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adventureworksstorage.blob.core.windows.net/silver/AdventureWorks_SubCategories/',
    FORMAT = 'PARQUET'
) AS rows;
GO

------------------------
-- CREATE VIEW: TERRITORIES
------------------------
CREATE VIEW gold.territories AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://adventureworksstorage.blob.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) AS rows;
GO
