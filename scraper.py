import requests
import json
import time
import pandas as pd
from datetime import datetime
from pathlib import Path
import re
import logging
import concurrent.futures
import calendar
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from config import ITEMS
from utils import setup_logging

class FamilyPlanningDataFetcher:
    def __init__(self, start_date="2016-12", end_date="2025-01", max_workers=1, max_retries=5):
        self.base_url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/"
        self.scmbd_url = "https://scmpbd.org/scip/"
        self.session = self._create_retry_session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://elmis.dgfp.gov.bd',
            'Referer': 'https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view.php'
        }
        
        # Parse start and end dates
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        
        # Set up concurrent processing
        self.max_workers = max_workers
        self.max_retries = max_retries
        
        # Set up logging
        self.logger = setup_logging("FamilyPlanningScraper")
        
        # Get items from config
        self.items = ITEMS
        
        # Initialize warehouse list
        self.warehouses = []
        self.initialize_session()
        
    def initialize_session(self):
        """Initialize the session by accessing the main page and getting warehouses"""
        try:
            # First access the main page to get cookies
            main_url = f"{self.base_url}sdpdataviewer/form2_view.php"
            self.logger.info(f"Initializing session by accessing {main_url}")
            
            response = self._make_request('GET', main_url)
            if not response:
                self.logger.error("Failed to initialize session")
                return
                
            self.logger.info("Successfully initialized session")
            
            # Now get the warehouses list
            self.warehouses = self.get_warehouses()
            self.logger.info(f"Found {len(self.warehouses)} warehouses")
            
        except Exception as e:
            self.logger.error(f"Error initializing session: {str(e)}")

    def _create_retry_session(self):
        """Create a session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,  # number of retries
            backoff_factor=1,  # wait 1, 2, 4 seconds between retries
            status_forcelist=[500, 502, 503, 504, 404],  # status codes to retry on
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _make_request(self, method: str, url: str, **kwargs):
        """Make a request with error handling and retries"""
        max_retries = self.max_retries
        for attempt in range(max_retries):
            try:
                response = self.session.request(method, url, **kwargs, timeout=30)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt+1}/{max_retries}): {str(e)}")
                if kwargs.get('data'):
                    self.logger.debug(f"Request data: {kwargs['data']}")
                if 'response' in locals():
                    self.logger.debug(f"Response status code: {response.status_code}")
                    if hasattr(response, 'text'):
                        self.logger.debug(f"Response content snippet: {response.text[:200]}...")
                
                if attempt == max_retries - 1:
                    self.logger.error(f"Request failed after {max_retries} attempts")
                    return None
                
                # Exponential backoff with jitter
                wait_time = (2 ** attempt) + (random.random() * attempt)
                self.logger.info(f"Waiting {wait_time:.2f} seconds before retry...")
                time.sleep(wait_time)
        
        return None

    def get_warehouses(self):
        """Get list of all warehouses"""
        self.logger.info("Fetching warehouses list")
        
        # First try the direct approach
        try:
            response = self._make_request('GET', f"{self.base_url}sdpdataviewer/form2_view.php")
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                warehouse_select = soup.find('select', {'id': 'cmbWH'})
                
                if warehouse_select:
                    warehouses = []
                    for option in warehouse_select.find_all('option'):
                        value = option.get('value')
                        if value and value != 'All':
                            warehouses.append({
                                'whrec_id': value,
                                'wh_name': option.text.strip()
                            })
                    
                    if warehouses:
                        self.logger.info(f"Found {len(warehouses)} warehouses from dropdown")
                        return warehouses
        except Exception as e:
            self.logger.warning(f"Error fetching warehouses from dropdown: {str(e)}")
        
        # Fallback to the JavaScript parsing approach
        try:
            response = self._make_request('GET', "https://scmpbd.org/scip/lmis/form2_view.php")
            if not response:
                self.logger.error("Failed to get warehouses list from scmpbd.org")
                return self._fallback_warehouses()
                
            warehouse_match = re.search(r'gWarehouseListAll\s*=\s*JSON\.parse\(\'(.*?)\'\);', response.text)
            if warehouse_match:
                warehouses = json.loads(warehouse_match.group(1))
                self.logger.info(f"Successfully parsed {len(warehouses)} warehouses from JavaScript")
                return warehouses
            
            self.logger.error("Could not find warehouses in the response")
            return self._fallback_warehouses()
            
        except Exception as e:
            self.logger.error(f"Error parsing warehouse data: {str(e)}")
            return self._fallback_warehouses()

    def _fallback_warehouses(self):
        """Return a fallback list of warehouses if we can't get them from the site"""
        self.logger.warning("Using fallback warehouse list")
        warehouses = [
            {"whrec_id": "WH-011", "wh_name": "Bandarban RWH"},
            {"whrec_id": "WH-022", "wh_name": "Barishal RWH"},
            {"whrec_id": "WH-001", "wh_name": "Bhola RWH"},
            {"whrec_id": "WH-018", "wh_name": "Bogura RWH"},
            {"whrec_id": "WH-019", "wh_name": "Chattogram RWH"},
            {"whrec_id": "WH-020", "wh_name": "Cox's Bazar RWH"},
            {"whrec_id": "WH-014", "wh_name": "Cumilla RWH"},
            {"whrec_id": "WH-002", "wh_name": "Dhaka CWH"},
            {"whrec_id": "WH-021", "wh_name": "Dinajpur RWH"},
            {"whrec_id": "WH-003", "wh_name": "Faridpur RWH"},
            {"whrec_id": "WH-004", "wh_name": "Jamalpur RWH"},
            {"whrec_id": "WH-005", "wh_name": "Jashore RWH"},
            {"whrec_id": "WH-006", "wh_name": "Khulna RWH"},
            {"whrec_id": "WH-007", "wh_name": "Kushtia RWH"},
            {"whrec_id": "WH-008", "wh_name": "Mymensingh RWH"},
            {"whrec_id": "WH-009", "wh_name": "Noakhali RWH"},
            {"whrec_id": "WH-010", "wh_name": "Pabna RWH"},
            {"whrec_id": "WH-012", "wh_name": "Patuakhali RWH"},
            {"whrec_id": "WH-013", "wh_name": "Rajshahi RWH"},
            {"whrec_id": "WH-015", "wh_name": "Rangamati RWH"},
            {"whrec_id": "WH-016", "wh_name": "Rangpur RWH"},
            {"whrec_id": "WH-017", "wh_name": "Sylhet RWH"},
            {"whrec_id": "WH-023", "wh_name": "Tangail RWH"}
        ]
        return warehouses

    # Import methods from data_fetcher.py
    from data_fetcher import (
        get_upazilas, get_unions, get_item_tab, get_item_data, scrape_item_data, direct_download_excel
    )
    
    # Import methods from data_processor.py
    from data_processor import (
        process_union_data, _process_single_item, process_upazila, process_warehouse_month, process_month
    )
    
    def generate_date_ranges(self):
        """Generate all year-month combinations in the date range"""
        # Ensure we have a full month by adjusting dates
        start_date = self.start_date.replace(day=1)
        
        # Add 1 day to end date to ensure the last month is included
        if self.end_date.day < 28:  # If it's not the end of month
            end_date = self.end_date.replace(day=28)
        else:
            end_date = self.end_date
        
        # Generate monthly date range
        months = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' is month start frequency
        
        # Convert to year-month tuples
        date_ranges = [(str(date.year), str(date.month).zfill(2)) for date in months]
        
        # Print the date ranges for debugging
        self.logger.info(f"Date ranges: {date_ranges}")
        
        return date_ranges

    def fetch_all_data(self, resume_from=None, specific_warehouse=None):
        """Fetch all data for specified date range with option to resume"""
        # Create output directory
        output_dir = Path("family_planning_data")
        output_dir.mkdir(exist_ok=True)
        
        # Generate date ranges
        date_ranges = self.generate_date_ranges()
        self.logger.info(f"Generated {len(date_ranges)} year-month combinations to process")
        
        # Option to resume from a specific date
        if resume_from:
            resume_year, resume_month = resume_from.split('-')
            date_ranges = [d for d in date_ranges if (d[0] > resume_year) or (d[0] == resume_year and d[1] >= resume_month)]
            self.logger.info(f"Resuming from {resume_from}, {len(date_ranges)} year-month combinations remaining")
        
        # Filter warehouses if a specific one is requested
        if specific_warehouse:
            original_warehouses = self.warehouses.copy()
            
            # Check if specific_warehouse is numeric (ID) or a name
            found = False
            
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
                self.logger.info(f"Filtering to process only warehouse: {self.warehouses[0]['wh_name']} (ID: {self.warehouses[0]['whrec_id']})")
                print(f"Processing only warehouse: {self.warehouses[0]['wh_name']} (ID: {self.warehouses[0]['whrec_id']})")
            else:
                self.logger.error(f"Warehouse '{specific_warehouse}' not found")
                self.logger.info(f"Available warehouses:")
                for wh in self.warehouses:
                    self.logger.info(f"  - {wh['wh_name']} (ID: {wh['whrec_id']})")
                print(f"Warehouse '{specific_warehouse}' not found. See logs for available warehouses.")
                return []
        
        # Create a summary log
        summary_log = []
        
        # Process each month - either sequentially or with concurrency
        if self.max_workers > 1:
            self.logger.info(f"Using concurrent processing with {self.max_workers} workers")
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_date = {executor.submit(self.process_month, date_range, output_dir): date_range for date_range in date_ranges}
                
                for future in concurrent.futures.as_completed(future_to_date):
                    date_range = future_to_date[future]
                    try:
                        monthly_summary = future.result()
                        summary_log.append(monthly_summary)
                        self.logger.info(f"Completed processing for {date_range[0]}-{date_range[1]}")
                    except Exception as e:
                        self.logger.error(f"Error processing {date_range[0]}-{date_range[1]}: {str(e)}")
        else:
            self.logger.info("Using sequential processing")
            for date_range in date_ranges:
                try:
                    monthly_summary = self.process_month(date_range, output_dir)
                    summary_log.append(monthly_summary)
                except Exception as e:
                    self.logger.error(f"Error processing {date_range[0]}-{date_range[1]}: {str(e)}")
        
        # Save complete summary log
        with open(output_dir / 'fetch_summary.json', 'w', encoding='utf-8') as f:
            json.dump(summary_log, f, indent=2)
        
        self.logger.info("Data collection complete!")
        return summary_log

    def generate_stats(self, summary_log):
        """Generate and print statistics about the fetched data"""
        if not summary_log:
            self.logger.warning("No summary log provided for statistics generation")
            return
        
        total_warehouses = 0
        total_upazilas = 0
        total_unions = 0
        total_files = 0
        total_errors = 0
        
        for month in summary_log:
            self.logger.info(f"\nMonth: {month['year']}-{month['month']}")
            
            month_warehouses = len(month['warehouses'])
            month_upazilas = sum(wh.get('upazila_count', 0) for wh in month['warehouses'])
            month_unions = sum(wh.get('union_count', 0) for wh in month['warehouses'])
            month_files = sum(wh.get('data_files', 0) for wh in month['warehouses'])
            month_errors = sum(len(wh.get('errors', [])) for wh in month['warehouses'])
            
            self.logger.info(f"  Warehouses: {month_warehouses}")
            self.logger.info(f"  Upazilas: {month_upazilas}")
            self.logger.info(f"  Unions: {month_unions}")
            self.logger.info(f"  Data files: {month_files}")
            self.logger.info(f"  Errors: {month_errors}")
            
            total_warehouses += month_warehouses
            total_upazilas += month_upazilas
            total_unions += month_unions
            total_files += month_files
            total_errors += month_errors
        
        self.logger.info("\nOverall Summary:")
        self.logger.info(f"  Total months processed: {len(summary_log)}")
        if len(summary_log) > 0:
            self.logger.info(f"  Average warehouses per month: {total_warehouses / len(summary_log):.2f}")
            self.logger.info(f"  Average upazilas per month: {total_upazilas / len(summary_log):.2f}")
            self.logger.info(f"  Average unions per month: {total_unions / len(summary_log):.2f}")
            self.logger.info(f"  Average data files per month: {total_files / len(summary_log):.2f}")
        self.logger.info(f"  Total files collected: {total_files}")
        self.logger.info(f"  Total errors encountered: {total_errors}")
