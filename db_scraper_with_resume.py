import argparse
import json
import time
import random
import logging
from datetime import datetime
from pathlib import Path
import concurrent.futures
import pyodbc
import os
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

class DatabaseFamilyPlanningFetcher(FamilyPlanningDataFetcher):
    """Extended version of FamilyPlanningDataFetcher that writes directly to a database with progress tracking"""
    
    def __init__(self, start_date="2016-12", end_date="2025-01", max_workers=4, max_retries=5, check_resumption=True):
        super().__init__(start_date, end_date, max_workers, max_retries)
        self.db_logger = setup_logging("DatabaseFetcher")
        self.check_resumption = check_resumption
        self.run_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Test database connection on initialization
        try:
            conn = pyodbc.connect(CONN_STR)
            cursor = conn.cursor()
            
            # Test if our tables exist
            try:
                cursor.execute("SELECT TOP 1 * FROM [dbo].[Form_F3_Data]")
                cursor.fetchone()
                self.db_logger.info("Form_F3_Data table exists")
            except Exception:
                self.db_logger.warning("Form_F3_Data table does not exist. Please run the SQL script to create it.")
            
            try:
                cursor.execute("SELECT TOP 1 * FROM [dbo].[ScrapeProgress]")
                cursor.fetchone()
                self.db_logger.info("ScrapeProgress table exists")
            except Exception:
                self.db_logger.warning("ScrapeProgress table does not exist. Please run the SQL script to create it.")
            
            cursor.close()
            conn.close()
            
            self.db_logger.info("Database connection successful")
        except Exception as e:
            self.db_logger.error(f"Database connection failed: {str(e)}")
            raise
        
        # Check for resumption point if requested
        if check_resumption:
            self.check_for_resumption_point()
    
    def check_for_resumption_point(self):
        """Check if there's a point to resume from in the database"""
        try:
            conn = pyodbc.connect(CONN_STR)
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM [dbo].[ScrapeResumptionPoint]")
            row = cursor.fetchone()
            
            if row:
                self.db_logger.info(f"Found resumption point: Year={row.year}, Month={row.month}, Warehouse={row.warehouse_name}")
                
                if row.upazila_id:
                    self.db_logger.info(f"Upazila: {row.upazila_name} ({row.upazila_id})")
                    
                    if row.union_code:
                        self.db_logger.info(f"Union: {row.union_name} ({row.union_code})")
                        
                        if row.item_code:
                            self.db_logger.info(f"Item: {row.item_name} ({row.item_code})")
                
                resume_year = row.year
                resume_month = row.month
                warehouse_id = row.warehouse_id
                
                print(f"\nFound a previous scraping session that was interrupted.")
                print(f"You can resume from: Year={row.year}, Month={row.month}, Warehouse={row.warehouse_name}")
                print(f"To resume, use: --resume {row.year}-{row.month} --warehouse {warehouse_id}")
            else:
                self.db_logger.info("No resumption point found")
            
            cursor.close()
            conn.close()
        except Exception as e:
            self.db_logger.error(f"Error checking for resumption point: {str(e)}")
    
    def update_progress(self, year, month, warehouse, upazila=None, union=None, item=None, status="in_progress", records=0, error=None):
        """Update progress in the database"""
        try:
            conn = pyodbc.connect(CONN_STR)
            cursor = conn.cursor()
            
            # Extract values
            wh_id = warehouse['whrec_id']
            wh_name = warehouse['wh_name'].replace('&#039;', "'")
            
            upz_id = None
            upz_name = None
            if upazila:
                upz_id = upazila.get('upazila_id')
                upz_name = upazila.get('upazila_name')
            
            union_code = None
            union_name = None
            if union:
                union_code = union.get('UnionCode')
                union_name = union.get('UnionName')
            
            item_code = None
            item_name = None
            if item:
                item_code = item.get('itemCode')
                item_name = item.get('itemName')
            
            # Call the stored procedure
            cursor.execute(
                """EXEC [dbo].[UpdateScrapeProgress] 
                   @year=?, @month=?, @warehouse_id=?, @warehouse_name=?,
                   @upazila_id=?, @upazila_name=?, @union_code=?, @union_name=?,
                   @item_code=?, @item_name=?, @status=?, @records_inserted=?, @error_message=?""",
                year, month, wh_id, wh_name, 
                upz_id, upz_name, union_code, union_name,
                item_code, item_name, status, records, error
            )
            
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            self.db_logger.error(f"Error updating progress: {str(e)}")
    
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
        
        # Update progress - starting
        self.update_progress(year, month, warehouse, upazila, union, item, "in_progress")
        
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
                
                # Connect to database with rate limiting
                delay = random.uniform(0.5, 2.0)  # Random delay between 0.5 and 2 seconds
                time.sleep(delay)
                
                conn = pyodbc.connect(CONN_STR)
                cursor = conn.cursor()
                
                # Check for existing records to avoid duplicates
                cursor.execute(
                    """SELECT COUNT(*) FROM [dbo].[Form_F3_Data] 
                       WHERE [year]=? AND [month]=? AND [warehouse]=? AND [upazila]=? 
                       AND [product]=?""",
                    year, month, wh_name, upz_name, item_name
                )
                
                existing_count = cursor.fetchone()[0]
                if existing_count > 0:
                    self.db_logger.info(f"Found {existing_count} existing records for {item_name} in {union_name}, {upz_name}. Skipping.")
                    cursor.close()
                    conn.close()
                    
                    # Update progress - completed with existing records
                    self.update_progress(year, month, warehouse, upazila, union, item, "completed", existing_count)
                    return existing_count
                
                # Insert data into database
                records_inserted = 0
                
                for record in data:
                    # Map the fields from JSON to database columns
                    sql = """
                    INSERT INTO [dbo].[Form_F3_Data] (
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
                    
                    # Map the values
                    values = (
                        item_name,                         # product
                        record.get('opening_balance'),     # opening_balance
                        record.get('received'),            # received_this_month
                        record.get('total'),               # balance_this_month
                        record.get('adj_plus'),            # adjustment_plus
                        record.get('adj_minus'),           # adjustment_minus
                        record.get('grand_total'),         # total_this_month
                        record.get('distribution'),        # distribution_this_month
                        record.get('closing_balance'),     # closing_balance_this_month
                        record.get('stock_out_reason'),    # stock_out_reason_code
                        record.get('stock_out_days'),      # days_stock_out
                        1 if record.get('eligible') else 0,# eligible (convert to bit)
                        wh_name,                           # warehouse
                        '',                                # district (not directly available)
                        upz_name,                          # upazila
                        record.get('facility'),            # sdp (assuming facility is SDP)
                        month,                             # month
                        year,                              # year
                        filename                           # file_name
                    )
                    
                    try:
                        cursor.execute(sql, values)
                        records_inserted += 1
                    except Exception as e:
                        self.db_logger.error(f"Error inserting record: {str(e)}")
                        # Continue with next record instead of failing entire batch
                        continue
                
                # Commit and close
                conn.commit()
                cursor.close()
                conn.close()
                
                self.db_logger.info(f"Inserted {records_inserted} records for {item_name} in {union_name}, {upz_name}")
                
                # Update progress - completed
                self.update_progress(year, month, warehouse, upazila, union, item, "completed", records_inserted)
                
                return records_inserted
            else:
                self.db_logger.warning(f"No data found for {item_name} in {union_name}, {upz_name}")
                
                # Update progress - failed
                self.update_progress(year, month, warehouse, upazila, union, item, "failed", 0, "No data found")
                
                return 0
                
        except Exception as e:
            self.db_logger.error(f"Error processing item {item_name}: {str(e)}")
            
            # Update progress - failed
            self.update_progress(year, month, warehouse, upazila, union, item, "failed", 0, str(e))
            
            return 0
    
    def process_union_data_to_db(self, year, month, warehouse, upazila, union):
        """Process data for a single union and write to database"""
        union_code = union.get('UnionCode')
        union_name = union.get('UnionName')
        
        if not union_code or not union_name:
            self.db_logger.warning(f"Invalid union data: {union}")
            return 0
        
        # Update progress - starting union
        self.update_progress(year, month, warehouse, upazila, union, None, "in_progress")
        
        self.db_logger.debug(f"Processing union: {union_name}")
        
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
            
            # Update progress - completed union
            self.update_progress(year, month, warehouse, upazila, union, None, "completed", records_inserted)
            
        except Exception as e:
            self.db_logger.error(f"Error getting item tabs: {str(e)}")
            
            # Update progress - failed union
            self.update_progress(year, month, warehouse, upazila, union, None, "failed", records_inserted, str(e))
            
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
        
        # Update progress - starting upazila
        self.update_progress(year, month, warehouse, upazila, None, None, "in_progress")
        
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
        
        # Update progress - completed upazila
        self.update_progress(year, month, warehouse, upazila, None, None, "completed", records_inserted)
        
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
        
        # Update progress - starting warehouse
        self.update_progress(year, month, warehouse, None, None, None, "in_progress")
        
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
        
        # Update progress - completed warehouse
        self.update_progress(year, month, warehouse, None, None, None, "completed", warehouse_summary['records_inserted'])
        
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
        
        # Filter warehouses if a specific one is requested
        if specific_warehouse:
            original_warehouses = self.warehouses.copy()
            
            # Try exact warehouse ID match
            filtered_warehouses = [wh for wh in self.warehouses if 
                                specific_warehouse == wh['whrec_id']]
            
            # Try partial warehouse ID match (e.g.,