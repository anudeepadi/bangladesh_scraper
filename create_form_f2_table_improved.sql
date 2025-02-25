-- Create an improved Form_F2_Data table with all needed columns in proper order
USE [Bangladesh]
GO

-- Drop the table if it already exists to recreate with proper schema
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Form_F2_Data]') AND type in (N'U'))
BEGIN
    DROP TABLE [dbo].[Form_F2_Data]
    PRINT 'Dropped existing Form_F2_Data table'
END

-- Create the table with columns in proper order matching the display
CREATE TABLE [dbo].[Form_F2_Data] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [sl_number] INT NULL,                         -- SL#
    [name_of_fwa] NVARCHAR(255),                  -- Name of FWA
    [opening_balance] NVARCHAR(50),               -- Opening Balance
    [received_this_month] NVARCHAR(50),           -- Received
    [balance_this_month] NVARCHAR(50),            -- Total
    [adjustment_plus] NVARCHAR(50),               -- Adj. (+)
    [adjustment_minus] NVARCHAR(50),              -- Adj. (-)
    [total_this_month] NVARCHAR(50),              -- Grand Total
    [distribution_this_month] NVARCHAR(50),       -- Distribution
    [closing_balance_this_month] NVARCHAR(50),    -- Closing Balance
    [stock_out_reason_code] NVARCHAR(255),        -- Stock Out Reason
    [days_stock_out] NVARCHAR(50),                -- Days Stock Out
    [eligible] BIT,                               -- Eligible
    -- Additional metadata fields
    [product] NVARCHAR(255),                      -- Product name
    [warehouse] NVARCHAR(255),                    -- Warehouse name
    [district] NVARCHAR(255),                     -- District
    [upazila] NVARCHAR(255),                      -- Upazila
    [union_name] NVARCHAR(255),                   -- Union name
    [union_code] NVARCHAR(50),                    -- Union code
    [sdp] NVARCHAR(255),                          -- SDP (Service Delivery Point)
    [month] NVARCHAR(2),                          -- Month
    [year] NVARCHAR(4),                           -- Year
    [file_name] NVARCHAR(255),                    -- Source file name
    [created_at] DATETIME DEFAULT GETDATE()       -- Record creation timestamp
)

-- Add indexes for better performance
CREATE INDEX IX_Form_F2_Data_Location ON [dbo].[Form_F2_Data] 
(
    [year], 
    [month], 
    [warehouse], 
    [district],
    [upazila], 
    [union_name],
    [product]
)

-- Create a view to present the data in a nicely ordered format
CREATE OR ALTER VIEW [dbo].[vw_Form_F2_Data] AS
SELECT 
    [sl_number] AS [SL#],
    [name_of_fwa] AS [Name of FWA],
    [opening_balance] AS [Opening Balance],
    [received_this_month] AS [Received],
    [balance_this_month] AS [Total],
    [adjustment_plus] AS [Adj. (+)],
    [adjustment_minus] AS [Adj. (-)],
    [total_this_month] AS [Grand Total],
    [distribution_this_month] AS [Distribution],
    [closing_balance_this_month] AS [Closing Balance],
    [stock_out_reason_code] AS [Stock Out Reason],
    [days_stock_out] AS [Days Stock Out],
    [eligible] AS [Eligible],
    [product] AS [Product],
    [warehouse] AS [Warehouse],
    [district] AS [District],
    [upazila] AS [Upazila],
    [union_name] AS [Union],
    [month] AS [Month],
    [year] AS [Year],
    [created_at] AS [Created At]
FROM [dbo].[Form_F2_Data]

PRINT 'Form_F2_Data table and view created successfully'
GO
