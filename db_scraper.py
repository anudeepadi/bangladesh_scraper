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
    """Extended version of FamilyPlanningDataFetcher that writes directly to a database"""
    
    def __init__(self, start_date="2016-12", end_date="2025-01", max_workers=4, max_retries=5):
        super().__init__(start_date, end_date, max_workers, max_retries)
        self.db_logger = setup_logging("DatabaseFetcher")
        
        # Test database connection on initialization
        try:
            conn = pyodbc.connect(CONN_STR)
            conn.close()
            self.db_logger.info("Database connection successful")
        except Exception as e:
            self.db_logger.error(f"Database connection failed: {str(e)}")
            raise
    
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
                
                # Use a connection pool or connection context manager for better performance
                conn = pyodbc.connect(CONN_STR)
                cursor = conn.cursor()
                
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
                return records_inserted
            else:
                self.db_logger.warning(f"No data found for {item_name} in {union_name}, {upz_name}")
                return 0
                
        except Exception as e:
            self.db_logger.error(f"Error processing item {item_name}: {str(e)}")
            return 0
    
    def process_union_data_to_db(self, year, month, warehouse, upazila, union):
        """Process data for a single union and write to database"""
        union_code = union.get('UnionCode')
        union_name = union.get('UnionName')
        
        if not union_code or not union_name:
            self.db_logger.warning(f"Invalid union data: {union}")
            return 0
        
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
        except Exception as e:
            self.db_logger.error(f"Error getting item tabs: {str(e)}")
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
        
        self.db_logger.info("Database data collection complete!")
        return summary_log

def main():
    parser = argparse.ArgumentParser(description="Family Planning Data Fetcher - Database Version")
    parser.add_argument('--start', type=str, default="2016-12", help="Start date in YYYY-MM format (default: 2016-12)")
    parser.add_argument('--end', type=str, default="2025-01", help="End date in YYYY-MM format (default: 2025-01)")
    parser.add_argument('--resume', type=str, help="Resume from date in YYYY-MM format")
    parser.add_argument('--workers', type=int, default=4, help="Number of concurrent workers (default: 4)")
    parser.add_argument('--warehouse', type=str, help="Specific warehouse ID or name to process (optional)")
    parser.add_argument('--retries', type=int, default=5, help="Maximum number of retries for network requests")
    parser.add_argument('--batch-size', type=int, default=100, help="Number of records to commit in a single batch (default: 100)")
    parser.add_argument('--rate-limit', type=float, default=1.0, help="Base rate limit factor (higher = more delay between requests)")
    
    args = parser.parse_args()
    
    print(f"Family Planning Data Fetcher - Database Version")
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
    print(f"==============================================")
    
    # Ensure the python-dotenv package is installed
    try:
        import dotenv
    except ImportError:
        print("The python-dotenv package is not installed. Installing it now...")
        import subprocess
        subprocess.check_call(["pip", "install", "python-dotenv"])
        print("Successfully installed python-dotenv")
    
    # Ensure pyodbc is installed
    try:
        import pyodbc
    except ImportError:
        print("The pyodbc package is not installed. Installing it now...")
        import subprocess
        subprocess.check_call(["pip", "install", "pyodbc"])
        print("Successfully installed pyodbc")
    
    try:
        fetcher = DatabaseFamilyPlanningFetcher(
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

if __name__ == "__main__":
    main()
