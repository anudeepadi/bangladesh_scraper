-- Create the Form_F3_Data table with all needed columns
USE [Bangladesh]
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Form_F3_Data]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Form_F3_Data] (
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
        [sdp] NVARCHAR(255),
        [month] NVARCHAR(2),
        [year] NVARCHAR(4),
        [file_name] NVARCHAR(255),
        [created_at] DATETIME DEFAULT GETDATE()
    )
    
    PRINT 'Form_F3_Data table created successfully'
    
    -- Add an index to improve query performance
    CREATE INDEX IX_Form_F3_Data_Location ON [dbo].[Form_F3_Data] 
    (
        [year], 
        [month], 
        [warehouse], 
        [upazila], 
        [product]
    )
    
    PRINT 'Added index for faster queries'
END
ELSE
BEGIN
    PRINT 'Form_F3_Data table already exists'
END
GO
