import argparse
import json
import time
import random
import logging
import re
from datetime import datetime
from pathlib import Path
import concurrent.futures
import pyodbc
import os
import pickle
from dotenv import load_dotenv

# Import from existing scraper modules
from scraper import FamilyPlanningDataFetcher
from utils import setup_logging

# Load environment variables from .env file
load_dotenv()

# Database connection settings from environment variables
SERVER = os.getenv('SERVER', 'AC-DAC02')
DATABASE = os.getenv('BANGLADESH_DATABASE', 'Bangladesh')
USERNAME = os.getenv('UID', 'dacweb_dev')
PASSWORD = os.getenv('PID', '')

# Create connection string
CONN_STR = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

# Dictionary to map warehouse names to districts
WAREHOUSE_DISTRICT_MAP = {
    "Bandarban RWH": "Bandarban",
    "Barishal RWH": "Barishal",
    "Bhola RWH": "Bhola",
    "Bogura RWH": "Bogura",
    "Chattogram RWH": "Chattogram",
    "Cox's Bazar RWH": "Cox's Bazar",
    "Cumilla RWH": "Cumilla",
    "Dhaka CWH": "Dhaka",
    "Dinajpur RWH": "Dinajpur",
    "Faridpur RWH": "Faridpur",
    "Jamalpur RWH": "Jamalpur",
    "Jashore RWH": "Jashore",
    "Khulna RWH": "Khulna",
    "Kushtia RWH": "Kushtia",
    "Mymensingh RWH": "Mymensingh",
    "Noakhali RWH": "Noakhali",
    "Pabna RWH": "Pabna",
    "Patuakhali RWH": "Patuakhali",
    "Rajshahi RWH": "Rajshahi",
    "Rangamati RWH": "Rangamati",
    "Rangpur RWH": "Rangpur",
    "Sylhet RWH": "Sylhet",
    "Tangail RWH": "Tangail"
}

def create_database_table():
    """Create the Form_F2_Data table in the database with correct column structure"""
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        # Check if table exists and drop it for recreation
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Form_F2_Data]') AND type in (N'U'))
            BEGIN
                DROP TABLE [dbo].[Form_F2_Data]
                PRINT 'Dropped existing Form_F2_Data table'
            END
        """)
        
        # Create the table with exactly the requested columns
        cursor.execute("""
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
                [sdp] NVARCHAR(255),
                [month] NVARCHAR(2),
                [year] NVARCHAR(4),
                [file_name] NVARCHAR(255),
                [created_at] DATETIME DEFAULT GETDATE()
            )
        """)
        
        # Add index
        cursor.execute("""
            CREATE INDEX IX_Form_F2_Data_Location ON [dbo].[Form_F2_Data] 
            (
                [year], 
                [month], 
                [warehouse], 
                [district],
                [upazila],
                [product]
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Database table setup successful with exact column structure")
        return True
    except Exception as e:
        print(f"Error creating database table: {str(e)}")
        return False

class FinalDatabaseScraper(FamilyPlanningDataFetcher):
    """FamilyPlanningDataFetcher with final optimized configuration for Form_F2_Data"""
    
    def __init__(self, start_date="2017-01", end_date="2025-01", max_workers=4, max_retries=5):
        super().__init__(start_date, end_date, max_workers, max_retries)
        self.db_logger = setup_logging("FinalDBScraper")
        
        # Setup progress tracking with unique machine identifier
        machine_name = os.environ.get('COMPUTERNAME', 'unknown')
        self.progress_file = Path(f"scraper_progress_{machine_name}.pkl")
        self.progress = self._load_progress()
        
        # Create a unique run ID
        self.run_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Test database connection on initialization
        try:
            conn = pyodbc.connect(CONN_STR)
            cursor = conn.cursor()
            
            # Test if our table exists
            try:
                cursor.execute("SELECT TOP 1 * FROM [dbo].[Form_F2_Data]")
                cursor.fetchone()
                self.db_logger.info("Form_F2_Data table exists")
            except Exception:
                self.db_logger.warning("Form_F2_Data table does not exist. Will try to create it.")
                if Path("create_table_flag.txt").exists() or '--create-table' in os.sys.argv:
                    create_database_table()
            
            cursor.close()
            conn.close()
            
            self.db_logger.info("Database connection successful")
        except Exception as e:
            self.db_logger.error(f"Database connection failed: {str(e)}")
            raise
    
    def _load_progress(self):
        """Load progress data from file if exists"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'rb') as f:
                    progress = pickle.load(f)
                self.db_logger.info(f"Loaded progress data from {self.progress_file}")
                return progress
            except Exception as e:
                self.db_logger.error(f"Error loading progress data: {str(e)}")
        
        # Initialize empty progress structure
        return {
            'completed': set(),  # Set of (year, month, warehouse_id, upazila_id, union_code, item_code) tuples
            'failed': set(),     # Same structure for failed items
            'current': None,     # Current processing item
            'last_year': None,
            'last_month': None,
            'last_warehouse': None,
            'stats': {
                'records_inserted': 0,
                'warehouses_processed': 0,
                'upazilas_processed': 0,
                'unions_processed': 0,
                'items_processed': 0
            }
        }
    
    def _save_progress(self):
        """Save progress data to file"""
        try:
            with open(self.progress_file, 'wb') as f:
                pickle.dump(self.progress, f)
            self.db_logger.debug(f"Saved progress data to {self.progress_file}")
        except Exception as e:
            self.db_logger.error(f"Error saving progress data: {str(e)}")
    
    def check_completion_status(self, year, month, warehouse, upazila=None, union=None, item=None):
        """Check if a specific data point has been completed or failed"""
        # Extract IDs
        wh_id = warehouse['whrec_id']
        upz_id = upazila.get('upazila_id') if upazila else None
        union_code = union.get('UnionCode') if union else None
        item_code = item.get('itemCode') if item else None
        
        # Create lookup key
        key = (year, month, wh_id, upz_id, union_code, item_code)
        
        # Check if already processed
        if key in self.progress['completed']:
            return 'completed'
        elif key in self.progress['failed']:
            return 'failed'
        else:
            return None
    
    def update_completion_status(self, year, month, warehouse, upazila=None, union=None, item=None, status='completed', records=0):
        """Update the completion status of a data point"""
        # Extract IDs
        wh_id = warehouse['whrec_id']
        upz_id = upazila.get('upazila_id') if upazila else None
        union_code = union.get('UnionCode') if union else None
        item_code = item.get('itemCode') if item else None
        
        # Create key
        key = (year, month, wh_id, upz_id, union_code, item_code)
        
        # Update status
        if status == 'completed':
            self.progress['completed'].add(key)
            if key in self.progress['failed']:
                self.progress['failed'].remove(key)
        elif status == 'failed':
            self.progress['failed'].add(key)
            if key in self.progress['completed']:
                self.progress['completed'].remove(key)
        
        # Update stats
        self.progress['stats']['records_inserted'] += records
        
        if item is not None:
            self.progress['stats']['items_processed'] += 1
        elif union is not None and item is None:
            self.progress['stats']['unions_processed'] += 1
        elif upazila is not None and union is None:
            self.progress['stats']['upazilas_processed'] += 1
        elif warehouse is not None and upazila is None:
            self.progress['stats']['warehouses_processed'] += 1
        
        # Save current point for resumption
        self.progress['last_year'] = year
        self.progress['last_month'] = month
        self.progress['last_warehouse'] = wh_id
        
        # Save progress periodically
        if records > 0 or self.progress['stats']['items_processed'] % 10 == 0:
            self._save_progress()
    
    def find_resumption_point(self):
        """Find the point to resume scraping from"""
        if not self.progress['last_year'] or not self.progress['last_month'] or not self.progress['last_warehouse']:
            return None
        
        return {
            'year': self.progress['last_year'],
            'month': self.progress['last_month'],
            'warehouse_id': self.progress['last_warehouse']
        }
    
    def _batch_insert_records(self, records):
        """Insert multiple records into the database in a single batch"""
        if not records:
            return 0
        
        try:
            conn = pyodbc.connect(CONN_STR)
            cursor = conn.cursor()
            
            records_inserted = 0
            
            # Use a more efficient batch insert approach
            for record in records:
                sql = """
                INSERT INTO [dbo].[Form_F2_Data] (
                    [product],
                    [opening_balance],
                    [received_this_month],
                    [balance_this_month],
                    [adjustment_plus],
                    [adjustment_minus],
                    [total_this_month],
                    [distribution_this_month],
                    [closing_balance_this_month],
                    [stock_out_reason_code],
                    [days_stock_out],
                    [eligible],
                    [warehouse],
                    [district],
                    [upazila],
                    [sdp],
                    [month],
                    [year],
                    [file_name]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                try:
                    cursor.execute(sql, record)
                    records_inserted += 1
                except Exception as e:
                    self.db_logger.error(f"Error inserting record: {str(e)}")
                    # Continue with next record
            
            # Commit the batch
            conn.commit()
            cursor.close()
            conn.close()
            
            return records_inserted
            
        except Exception as e:
            self.db_logger.error(f"Error in batch insert: {str(e)}")
            return 0
    
    def _process_single_item_to_db(self, year, month, warehouse, upazila, union, item):
        """Process a single item for a union and write directly to database"""
        wh_id = warehouse['whrec_id']
        wh_name = warehouse['wh_name'].replace('&#039;', "'")
        upz_id = upazila.get('upazila_id')
        upz_name = upazila.get('upazila_name')
        union_code = union.get('UnionCode')
        union_name = union.get('UnionName')
        item_code = item.get('itemCode')
        item_name = item.get('itemName')
        
        # Get district from warehouse name
        district = WAREHOUSE_DISTRICT_MAP.get(wh_name, "")
        
        # Check if already processed
        status = self.check_completion_status(year, month, warehouse, upazila, union, item)
        if status == 'completed':
            self.db_logger.info(f"Item {item_name} for {union_name}, {upz_name} already processed. Skipping.")
            return 0
        
        self.db_logger.info(f"Processing item {item_name} for {union_name}, {upz_name}")
        
        try:
            # Get data with retries and fallbacks (using existing methods)
            data = None
            
            # Strategy 1: API method
            data = self.get_item_data(year, month, wh_id, upz_id, union_code, item_code)
            
            # Strategy 2: If first method fails, try direct Excel download
            if not data:
                self.db_logger.info(f"API method failed, trying Excel download")
                excel_data = self.direct_download_excel(year, month, wh_id, upz_id, union_code, item_code)
                if excel_data:
                    # Process Excel data if implemented
                    pass
            
            if data:
                # Generate a unique file name for reference
                filename = f"{upz_id}_{union_code}_{item_code}_{year}_{month}.json"
                
                # Convert raw data to database records
                db_records = []
                
                for record in data:
                    # Ensure all text fields are properly formatted
                    facility = record.get('facility', '')
                    
                    # Ensure stock_out fields are properly formatted
                    stock_out_reason = record.get('stock_out_reason', '') or ''
                    days_stock_out = record.get('stock_out_days', '') or ''
                    
                    # Format numeric fields
                    opening_balance = record.get('opening_balance', '') or '0'
                    received = record.get('received', '') or '0'
                    total = record.get('total', '') or '0'
                    adj_plus = record.get('adj_plus', '') or '0'
                    adj_minus = record.get('adj_minus', '') or '0'
                    grand_total = record.get('grand_total', '') or '0'
                    distribution = record.get('distribution', '') or '0'
                    closing_balance = record.get('closing_balance', '') or '0'
                    
                    # Map the values to database columns - EXACT ORDER AS REQUESTED
                    db_record = (
                        item_name,              # product
                        opening_balance,        # opening_balance
                        received,               # received_this_month
                        total,                  # balance_this_month
                        adj_plus,               # adjustment_plus
                        adj_minus,              # adjustment_minus
                        grand_total,            # total_this_month
                        distribution,           # distribution_this_month
                        closing_balance,        # closing_balance_this_month
                        stock_out_reason,       # stock_out_reason_code
                        days_stock_out,         # days_stock_out
                        1 if record.get('eligible') else 0,  # eligible
                        wh_name,                # warehouse
                        district,               # district
                        upz_name,               # upazila
                        facility,               # sdp (Name of FWA)
                        month,                  # month
                        year,                   # year
                        filename                # file_name
                    )
                    
                    db_records.append(db_record)
                
                # Insert records in a batch
                records_inserted = self._batch_insert_records(db_records)
                
                self.db_logger.info(f"Inserted {records_inserted} records for {item_name} in {union_name}, {upz_name}")
                
                # Update completion status
                self.update_completion_status(year, month, warehouse, upazila, union, item, 'completed', records_inserted)
                
                return records_inserted
            else:
                self.db_logger.warning(f"No data found for {item_name} in {union_name}, {upz_name}")
                
                # Update completion status
                self.update_completion_status(year, month, warehouse, upazila, union, item, 'failed')
                
                return 0
                
        except Exception as e:
            self.db_logger.error(f"Error processing item {item_name}: {str(e)}")
            
            # Update completion status
            self.update_completion_status(year, month, warehouse, upazila, union, item, 'failed')
            
            return 0
    
    def process_union_data_to_db(self, year, month, warehouse, upazila, union):
        """Process data for a single union and write to database"""
        union_code = union.get('UnionCode')
        union_name = union.get('UnionName')
        
        if not union_code or not union_name:
            self.db_logger.warning(f"Invalid union data: {union}")
            return 0
        
        # Check if already processed
        status = self.check_completion_status(year, month, warehouse, upazila, union)
        if status == 'completed':
            self.db_logger.info(f"Union {union_name} already processed. Skipping.")
            return 0
        
        self.db_logger.info(f"Processing union: {union_name}")
        
        records_inserted = 0
        errors = []
        
        # Try to get the available item tabs first
        try:
            self.db_logger.info(f"Getting available item tabs for union {union_name}")
            wh_id = warehouse['whrec_id']
            upz_id = upazila.get('upazila_id')
            item_tabs = self.get_item_tab(year, month, upz_id, wh_id, union_code)
            
            if item_tabs and len(item_tabs) > 0:
                self.db_logger.info(f"Found {len(item_tabs)} item tabs")
                # Process each available item in the tabs
                for item_tab in item_tabs:
                    item_code = item_tab.get('itemCode')
                    item_name = item_tab.get('itemName')
                    
                    if not item_code or not item_name:
                        continue
                    
                    records_inserted += self._process_single_item_to_db(year, month, warehouse, upazila, union, item_tab)
                    
                    # Add a small delay between item requests to avoid overwhelming the server
                    time.sleep(random.uniform(0.5, 1.5))
            else:
                self.db_logger.warning(f"No item tabs found, falling back to predefined items list")
                # Fall back to predefined items
                for item in self.items:
                    records_inserted += self._process_single_item_to_db(year, month, warehouse, upazila, union, item)
                    time.sleep(random.uniform(0.5, 1.5))
            
            # Update completion status for the union
            if records_inserted > 0:
                self.update_completion_status(year, month, warehouse, upazila, union, None, 'completed', 0)
            else:
                self.update_completion_status(year, month, warehouse, upazila, union, None, 'failed', 0)
                
        except Exception as e:
            self.db_logger.error(f"Error getting item tabs: {str(e)}")
            
            # Update completion status as failed
            self.update_completion_status(year, month, warehouse, upazila, union, None, 'failed', 0)
            
            # Fall back to predefined items
            for item in self.items:
                records_inserted += self._process_single_item_to_db(year, month, warehouse, upazila, union, item)
                time.sleep(random.uniform(0.5, 1.5))
        
        return records_inserted
    
    def process_upazila_to_db(self, year, month, warehouse, upazila):
        """Process data for a single upazila and write to database"""
        upz_id = upazila.get('upazila_id')
        upz_name = upazila.get('upazila_name')
        
        if not upz_id or not upz_name:
            self.db_logger.warning(f"Invalid upazila data: {upazila}")
            return {"union_count": 0, "records_inserted": 0, "errors": ["Invalid upazila data"]}
        
        # Check if already processed
        status = self.check_completion_status(year, month, warehouse, upazila)
        if status == 'completed':
            self.db_logger.info(f"Upazila {upz_name} already processed. Skipping.")
            return {"union_count": 0, "records_inserted": 0, "errors": []}
        
        self.db_logger.info(f"Processing upazila: {upz_name}")
        
        # Get unions for this upazila
        unions = self.get_unions(upz_id, year, month)
        self.db_logger.info(f"Found {len(unions)} unions for upazila {upz_name}")
        
        union_results = []
        records_inserted = 0
        errors = []
        
        # Process each union
        for union in unions:
            try:
                union_records = self.process_union_data_to_db(year, month, warehouse, upazila, union)
                records_inserted += union_records
                
                union_results.append({
                    "union_name": union.get('UnionName', 'Unknown'),
                    "union_code": union.get('UnionCode', 'Unknown'),
                    "records_inserted": union_records
                })
                
            except Exception as e:
                error_msg = f"Error processing union {union.get('UnionName', 'Unknown')}: {str(e)}"
                self.db_logger.error(error_msg)
                errors.append(error_msg)
            
            # Add a delay between unions to avoid overwhelming the server
            time.sleep(random.uniform(1.5, 3.0))
        
        # Update completion status for upazila
        if records_inserted > 0 and not errors:
            self.update_completion_status(year, month, warehouse, upazila, None, None, 'completed', 0)
        else:
            self.update_completion_status(year, month, warehouse, upazila, None, None, 'failed', 0)
            
        return {
            "union_count": len(unions),
            "records_inserted": records_inserted,
            "union_results": union_results,
            "errors": errors
        }
    
    def process_warehouse_month_to_db(self, year, month, warehouse):
        """Process data for a single warehouse for a specific month and write to database"""
        wh_id = warehouse['whrec_id']
        wh_name = warehouse['wh_name'].replace('&#039;', "'")
        
        # Check if already processed
        status = self.check_completion_status(year, month, warehouse)
        if status == 'completed':
            self.db_logger.info(f"Warehouse {wh_name} for {year}-{month} already processed. Skipping.")
            return {
                'name': wh_name,
                'id': wh_id,
                'upazila_count': 0,
                'union_count': 0,
                'records_inserted': 0,
                'errors': [],
                'upazila_results': []
            }
        
        self.db_logger.info(f"Processing warehouse: {wh_name} for {year}-{month}")
        
        # Get all upazilas for this warehouse and month
        upazilas = self.get_upazilas(year, month, wh_id)
        self.db_logger.info(f"Found {len(upazilas)} upazilas for warehouse {wh_name}")
        
        warehouse_summary = {
            'name': wh_name,
            'id': wh_id,
            'upazila_count': len(upazilas),
            'union_count': 0,
            'records_inserted': 0,
            'errors': [],
            'upazila_results': []
        }
        
        # Process each upazila
        for upazila in upazilas:
            try:
                upazila_result = self.process_upazila_to_db(year, month, warehouse, upazila)
                
                warehouse_summary['union_count'] += upazila_result['union_count']
                warehouse_summary['records_inserted'] += upazila_result['records_inserted']
                warehouse_summary['errors'].extend(upazila_result['errors'])
                
                warehouse_summary['upazila_results'].append({
                    'upazila_name': upazila.get('upazila_name', 'Unknown'),
                    'upazila_id': upazila.get('upazila_id', 'Unknown'),
                    'union_count': upazila_result['union_count'],
                    'records_inserted': upazila_result['records_inserted'],
                    'union_results': upazila_result.get('union_results', [])
                })
                
            except Exception as e:
                error_msg = f"Error processing upazila {upazila.get('upazila_name', 'Unknown')}: {str(e)}"
                self.db_logger.error(error_msg)
                warehouse_summary['errors'].append(error_msg)
            
            # Add a delay between upazilas to avoid overwhelming the server
            time.sleep(random.uniform(3.0, 5.0))
        
        # Update completion status for warehouse
        if warehouse_summary['records_inserted'] > 0 and not warehouse_summary['errors']:
            self.update_completion_status(year, month, warehouse, None, None, None, 'completed', 0)
        else:
            self.update_completion_status(year, month, warehouse, None, None, None, 'failed', 0)
        
        # Log summary for this warehouse
        self.db_logger.info(f"Warehouse {wh_name} summary: {warehouse_summary['records_inserted']} records inserted from {warehouse_summary['union_count']} unions in {warehouse_summary['upazila_count']} upazilas")
        
        return warehouse_summary
    
    def process_month_to_db(self, year_month_tuple):
        """Process all warehouses for a specific month and write to database"""
        year, month = year_month_tuple
        
        self.db_logger.info(f"\n{'='*50}\nProcessing data for {year}-{month}\n{'='*50}")
        
        monthly_summary = {
            'year': year,
            'month': month,
            'warehouses': []
        }
        
        # Process each warehouse
        for warehouse in self.warehouses:
            try:
                warehouse_summary = self.process_warehouse_month_to_db(year, month, warehouse)
                monthly_summary['warehouses'].append(warehouse_summary)
            except Exception as e:
                error_msg = f"Error processing warehouse {warehouse.get('wh_name', 'Unknown')}: {str(e)}"
                self.db_logger.error(error_msg)
                monthly_summary['warehouses'].append({
                    'name': warehouse.get('wh_name', 'Unknown').replace('&#039;', "'"),
                    'id': warehouse.get('whrec_id', 'Unknown'),
                    'errors': [error_msg]
                })
            
            # Add a delay between warehouses to avoid overwhelming the server
            time.sleep(random.uniform(5.0, 10.0))
        
        self.db_logger.info(f"Completed data collection for {month}/{year}")
        return monthly_summary
    
    def fetch_all_data_to_db(self, resume_from=None, specific_warehouse=None):
        """Fetch all data for specified date range with option to resume, writing directly to database"""
        # Generate date ranges
        date_ranges = self.generate_date_ranges()
        self.db_logger.info(f"Generated {len(date_ranges)} year-month combinations to process")
        
        # Option to resume from a specific date
        if resume_from:
            resume_year, resume_month = resume_from.split('-')
            date_ranges = [d for d in date_ranges if (d[0] > resume_year) or (d[0] == resume_year and d[1] >= resume_month)]
            self.db_logger.info(f"Resuming from {resume_from}, {len(date_ranges)} year-month combinations remaining")
        else:
            # Check if we can auto-resume from local progress
            resume_point = self.find_resumption_point()
            if resume_point:
                resume_year = resume_point['year']
                resume_month = resume_point['month']
                self.db_logger.info(f"Auto-resuming from local progress: {resume_year}-{resume_month}")
                date_ranges = [d for d in date_ranges if (d[0] > resume_year) or (d[0] == resume_year and d[1] >= resume_month)]
                
                # If warehouse is specified in resume point but not in args, use it
                if not specific_warehouse and 'warehouse_id' in resume_point:
                    specific_warehouse = resume_point['warehouse_id']
                    self.db_logger.info(f"Auto-resuming with warehouse: {specific_warehouse}")
        
        # Filter warehouses if a specific one is requested
        if specific_warehouse:
            original_warehouses = self.warehouses.copy()
            
            # Try exact warehouse ID match
            filtered_warehouses = [wh for wh in self.warehouses if 
                                specific_warehouse == wh['whrec_id']]
            
            # Try partial warehouse ID match (e.g., "11" matching "WH-011")
            if not filtered_warehouses:
                filtered_warehouses = [wh for wh in self.warehouses if 
                                    specific_warehouse in wh['whrec_id']]
            
            # Try name-based search
            if not filtered_warehouses:
                filtered_warehouses = [wh for wh in self.warehouses if 
                                    specific_warehouse.lower() in wh['wh_name'].lower()]
            
            if filtered_warehouses:
                self.warehouses = filtered_warehouses
                self.db_logger.info(f"Filtering to process only warehouse: {self.warehouses[0]['wh_name']} (ID: {self.warehouses[0]['whrec_id']})")
                print(f"Processing only warehouse: {self.warehouses[0]['wh_name']} (ID: {self.warehouses[0]['whrec_id']})")
            else:
                self.db_logger.error(f"Warehouse '{specific_warehouse}' not found")
                self.db_logger.info(f"Available warehouses:")
                for wh in self.warehouses:
                    self.db_logger.info(f"  - {wh['wh_name']} (ID: {wh['whrec_id']})")
                print(f"Warehouse '{specific_warehouse}' not found. See logs for available warehouses.")
                return []
        
        # Create a summary log
        summary_log = []
        
        # Process each month - either sequentially or with concurrency
        if self.max_workers > 1:
            self.db_logger.info(f"Using concurrent processing with {self.max_workers} workers")
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_date = {executor.submit(self.process_month_to_db, date_range): date_range for date_range in date_ranges}
                
                for future in concurrent.futures.as_completed(future_to_date):
                    date_range = future_to_date[future]
                    try:
                        monthly_summary = future.result()
                        summary_log.append(monthly_summary)
                        self.db_logger.info(f"Completed processing for {date_range[0]}-{date_range[1]}")
                    except Exception as e:
                        self.db_logger.error(f"Error processing {date_range[0]}-{date_range[1]}: {str(e)}")
        else:
            self.db_logger.info("Using sequential processing")
            for date_range in date_ranges:
                try:
                    monthly_summary = self.process_month_to_db(date_range)
                    summary_log.append(monthly_summary)
                except Exception as e:
                    self.db_logger.error(f"Error processing {date_range[0]}-{date_range[1]}: {str(e)}")
        
        # Save complete summary log to a file
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        with open(log_dir / 'db_fetch_summary.json', 'w', encoding='utf-8') as f:
            json.dump(summary_log, f, indent=2)
        
        # Final save of progress
        self._save_progress()
        
        # Print final statistics
        print("\nScraping Statistics:")
        print(f"Records inserted: {self.progress['stats']['records_inserted']}")
        print(f"Warehouses processed: {self.progress['stats']['warehouses_processed']}")
        print(f"Upazilas processed: {self.progress['stats']['upazilas_processed']}")
        print(f"Unions processed: {self.progress['stats']['unions_processed']}")
        print(f"Items processed: {self.progress['stats']['items_processed']}")
        
        self.db_logger.info("Database data collection complete!")
        return summary_log

def main():
    parser = argparse.ArgumentParser(description="Final Family Planning Database Scraper")
    parser.add_argument('--start', type=str, default="2017-01", help="Start date in YYYY-MM format (default: 2017-01)")
    parser.add_argument('--end', type=str, default="2025-01", help="End date in YYYY-MM format (default: 2025-01)")
    parser.add_argument('--resume', type=str, help="Resume from date in YYYY-MM format")
    parser.add_argument('--workers', type=int, default=4, help="Number of concurrent workers (default: 4)")
    parser.add_argument('--warehouse', type=str, default="", help="Specific warehouse ID or name to process (default: all warehouses)")
    parser.add_argument('--retries', type=int, default=5, help="Maximum number of retries for network requests")
    parser.add_argument('--batch-size', type=int, default=100, help="Number of records to commit in a single batch (default: 100)")
    parser.add_argument('--rate-limit', type=float, default=1.0, help="Base rate limit factor (higher = more delay between requests)")
    parser.add_argument('--reset-progress', action='store_true', help="Reset progress and start fresh")
    parser.add_argument('--create-table', action='store_true', help="Create the database table if it doesn't exist")
    
    args = parser.parse_args()
    
    print(f"Final Family Planning Database Scraper")
    print(f"==============================================")
    print(f"Start date: {args.start}")
    print(f"End date: {args.end}")
    print(f"Workers: {args.workers}")
    print(f"Max retries: {args.retries}")
    print(f"Rate limit factor: {args.rate_limit}")
    if args.resume:
        print(f"Resuming from: {args.resume}")
    if args.warehouse:
        print(f"Processing warehouse: {args.warehouse}")
    else:
        print(f"Processing ALL warehouses")
    if args.reset_progress:
        print(f"Resetting progress tracking")
    if args.create_table:
        print(f"Will create database table if needed")
    print(f"==============================================")
    
    # Ensure required packages are installed
    try:
        import dotenv
    except ImportError:
        print("The python-dotenv package is not installed. Installing it now...")
        import subprocess
        subprocess.check_call(["pip", "install", "python-dotenv"])
        print("Successfully installed python-dotenv")
    
    try:
        import pyodbc
    except ImportError:
        print("The pyodbc package is not installed. Installing it now...")
        import subprocess
        subprocess.check_call(["pip", "install", "pyodbc"])
        print("Successfully installed pyodbc")
    
    # Import sys module for command line args checking
    import sys
    
    # Create database table if requested
    if args.create_table or Path("create_table_flag.txt").exists():
        print("Creating improved database table...")
        create_database_table()
    
    # Reset progress if requested
    if args.reset_progress:
        machine_name = os.environ.get('COMPUTERNAME', 'unknown')
        progress_file = Path(f"scraper_progress_{machine_name}.pkl")
        if progress_file.exists():
            progress_file.unlink()
            print(f"Progress tracking reset for machine: {machine_name}")
    
    try:
        fetcher = FinalDatabaseScraper(
            start_date=args.start,
            end_date=args.end,
            max_workers=args.workers,
            max_retries=args.retries
        )
        
        summary = fetcher.fetch_all_data_to_db(resume_from=args.resume, specific_warehouse=args.warehouse)
        
        print("\nData collection and database insertion complete!")
    except Exception as e:
        print(f"Error initializing or running the fetcher: {str(e)}")
        import traceback
        traceback.print_exc()
        
        print("\nIf the scraper was interrupted, you can resume later using:")
        print(f"python final_db_scraper.py")
        print("The scraper will automatically pick up where it left off.")

if __name__ == "__main__":
    main()
