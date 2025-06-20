
CREATE SCHEMA gold;

CREATE DATABASE SCOPED CREDENTIAL credentials
WITH IDENTITY = 'Managed Identity';
GO

-- Step 2: Create external data sources pointing to silver and gold layers
CREATE EXTERNAL DATA SOURCE source_silver
WITH (
    LOCATION = 'https://adventureworksstorage.blob.core.windows.net/silver',
    CREDENTIAL = credentials
);
GO

CREATE EXTERNAL DATA SOURCE source_gold
WITH (
    LOCATION = 'https://adventureworksstorage.blob.core.windows.net/gold',
    CREDENTIAL = credentials
);
GO

-- Step 3: Create external file format for Parquet files using Snappy compression
CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

CREATE EXTERNAL TABLE gold.ext_sales
WITH (
    LOCATION = 'ext_sales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales;
GO
 