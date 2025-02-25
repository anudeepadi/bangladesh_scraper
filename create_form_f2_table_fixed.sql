-- Create an improved Form_F2_Data table with exact column structure as requested
USE [Bangladesh]
GO

-- Drop the table if it already exists to recreate with proper schema
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Form_F2_Data]') AND type in (N'U'))
BEGIN
    DROP TABLE [dbo].[Form_F2_Data]
    PRINT 'Dropped existing Form_F2_Data table'
END

-- Create the table with EXACTLY the requested columns
CREATE TABLE [dbo].[Form_F2_Data] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [product] NVARCHAR(255),
    [opening_balance] NVARCHAR(50),
    [received_this_month] NVARCHAR(50),
    [balance_this_month] NVARCHAR(50),
    [adjustment_plus] NVARCHAR(50),
    [adjustment_minus] NVARCHAR(50),
    [total_this_month] NVARCHAR(50),
    [distribution_this_month] NVARCHAR(50),
    [closing_balance_this_month] NVARCHAR(50),
    [stock_out_reason_code] NVARCHAR(255),
    [days_stock_out] NVARCHAR(50),
    [eligible] BIT,
    [warehouse] NVARCHAR(255),
    [district] NVARCHAR(255),
    [upazila] NVARCHAR(255),
    [sdp] NVARCHAR(255),          -- This is the "Name of FWA" field
    [month] NVARCHAR(2),
    [year] NVARCHAR(4),
    [file_name] NVARCHAR(255),
    [created_at] DATETIME DEFAULT GETDATE()
)

-- Add indexes for better performance
CREATE INDEX IX_Form_F2_Data_Location ON [dbo].[Form_F2_Data] 
(
    [year], 
    [month], 
    [warehouse], 
    [district],
    [upazila],
    [product]
)

PRINT 'Form_F2_Data table created successfully with exact requested column structure'
GO
