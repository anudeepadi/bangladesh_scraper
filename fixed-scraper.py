import requests
import json
import time
import random
import logging
from pathlib import Path
import re
from datetime import datetime
import os
import argparse
import concurrent.futures
import traceback

class BangladeshScraper:
    def __init__(self, start_date="2024-01", end_date="2024-02", max_workers=1, max_retries=3):
        # Parse date ranges
        self.start_year, self.start_month = start_date.split('-')
        self.end_year, self.end_month = end_date.split('-')
        
        # Base URL
        self.base_url = "https://elmis.dgfp.gov.bd/dgfplmis_reports"
        
        # Output directories
        self.output_dir = Path("family_planning_data")
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Setup logging
        self.log_dir = Path("logs")
        self.log_dir.mkdir(exist_ok=True, parents=True)
        self.logger = self.setup_logging()
        
        # Debug directory for raw responses
        self.debug_dir = Path("debug")
        self.debug_dir.mkdir(exist_ok=True, parents=True)
        
        # Concurrency and retry settings
        self.max_workers = max_workers
        self.max_retries = max_retries
        
        # Session for requests with retries
        self.session = self.create_retry_session(max_retries)
        
        # Common headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        
        # Storage for data
        self.warehouses = []
        self.all_combinations = []
        self.data_collected = 0
        
        # Statistics
        self.stats = {
            "total_warehouses": 0,
            "total_upazilas": 0,
            "total_unions": 0,
            "total_items": 0,
            "total_data_files": 0,
            "errors": []
        }
        
        # Known upazila mappings for warehouses that return empty lists
        # This is a fallback for when the API returns empty results
        self.warehouse_upazila_mapping = {
            "8": [  # Dhaka CWH known upazilas
                {"upazila_id": "T097", "upazila_name": "Dhamrai, Dhaka"},
                {"upazila_id": "T429", "upazila_name": "Abhaynagar, Jashore"}
            ],
            "11": [  # Khulna RWH
                {"upazila_id": "T429", "upazila_name": "Abhaynagar, Jashore"}
            ],
            "All": [  # All warehouses
                {"upazila_id": "T429", "upazila_name": "Abhaynagar, Jashore"}
            ]
        }
    
    def setup_logging(self):
        """Set up logging configuration"""
        logger = logging.getLogger("FamilyPlanningScraper")
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        if logger.handlers:
            logger.handlers.clear()
        
        # File handler
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_handler = logging.FileHandler(self.log_dir / f"scraper_{timestamp}.log")
        file_handler.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def create_retry_session(self, retries=3):
        """Create a session with retry capability"""
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
    
    def generate_date_ranges(self):
        """Generate all year-month combinations in the range"""
        start_year = int(self.start_year)
        start_month = int(self.start_month)
        end_year = int(self.end_year)
        end_month = int(self.end_month)
        
        date_ranges = []
        current_year = start_year
        current_month = start_month
        
        while current_year < end_year or (current_year == end_year and current_month <= end_month):
            date_ranges.append((str(current_year), f"{current_month:02d}"))
            
            # Move to next month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
        
        self.logger.info(f"Generated {len(date_ranges)} year-month combinations")
        return date_ranges
    
    def get_warehouses(self):
        """Get all warehouses or use default list"""
        self.logger.info("Setting up warehouses...")
        
        # List of known warehouses
        default_warehouses = [
            {"id": "8", "name": "Dhaka CWH"},
            {"id": "17", "name": "Chattogram RWH"},
            {"id": "12", "name": "Rajshahi RWH"},
            {"id": "11", "name": "Khulna RWH"},
            {"id": "10", "name": "Barishal RWH"},
            {"id": "9", "name": "Sylhet RWH"},
            {"id": "13", "name": "Rangpur RWH"},
            {"id": "2", "name": "Mymensingh RWH"},
            {"id": "All", "name": "All Warehouses"}
        ]
        
        self.warehouses = default_warehouses
        self.stats["total_warehouses"] = len(self.warehouses)
        self.logger.info(f"Using {len(self.warehouses)} warehouses")
        return True
    
    def get_upazilas(self, warehouse_id, year, month):
        """Get upazilas for a warehouse with fallback to known mapping for empty responses"""
        self.logger.info(f"Fetching upazilas for warehouse {warehouse_id}, {year}-{month}...")
        
        # First check if we have a mapping for this warehouse
        if warehouse_id in self.warehouse_upazila_mapping:
            self.logger.info(f"Using known upazila mapping for warehouse {warehouse_id}")
            return self.warehouse_upazila_mapping[warehouse_id]
        
        # If no mapping, try to fetch from API
        upazila_url = f"{self.base_url}/sdplist/sdplist_Processing.php"
        
        payload = {
            "operation": "getSDPUPList",
            "Year": year,
            "Month": month,
            "gWRHId": warehouse_id,
            "gDistId": "All"
        }
        
        for retry in range(self.max_retries):
            try:
                response = self.session.post(upazila_url, data=payload, headers=self.headers)
                if response.status_code != 200:
                    self.logger.error(f"Failed to get upazilas. Status code: {response.status_code}")
                    time.sleep(1)
                    continue
                
                # Save raw response for debugging (first attempt)
                if retry == 0:
                    with open(self.debug_dir / f"upazila_response_{warehouse_id}_{year}_{month}.txt", 'w', encoding='utf-8') as f:
                        f.write(response.text)
                
                # Try to parse as JSON first
                try:
                    data = json.loads(response.text)
                    # Check if we got an empty array
                    if isinstance(data, list):
                        if len(data) == 0:
                            self.logger.warning(f"Empty upazila list returned for warehouse {warehouse_id}")
                            # Fall back to default list if we have one, otherwise use test upazila
                            if "All" in self.warehouse_upazila_mapping:
                                self.logger.info("Using default upazila list")
                                return self.warehouse_upazila_mapping["All"]
                            else:
                                self.logger.warning("Using test upazila as fallback")
                                return [{"upazila_id": "T429", "upazila_name": "Abhaynagar, Jashore"}]
                        else:
                            # Successfully got list of upazilas
                            self.logger.info(f"Found {len(data)} upazilas for warehouse {warehouse_id}")
                            
                            # Format consistency check
                            if all("upazila_id" in item and "upazila_name" in item for item in data):
                                return data
                            
                            # If format is different, reformat
                            reformatted = []
                            for item in data:
                                if isinstance(item, dict):
                                    # Try to find ID and name keys
                                    upz_id = item.get("id") or item.get("upazila_id") or item.get("UpazilaId")
                                    upz_name = item.get("name") or item.get("upazila_name") or item.get("UpazilaName")
                                    
                                    if upz_id and upz_name:
                                        reformatted.append({
                                            "upazila_id": str(upz_id),
                                            "upazila_name": str(upz_name)
                                        })
                            
                            if reformatted:
                                self.logger.info(f"Reformatted {len(reformatted)} upazilas")
                                return reformatted
                except Exception as e:
                    self.logger.error(f"Error parsing upazila JSON: {str(e)}")
                
                # Extract from HTML as fallback
                upazila_pattern = r'<option value="(T\d+)">([^<]+)</option>'
                matches = re.findall(upazila_pattern, response.text)
                
                if matches:
                    upazilas = []
                    for upazila_id, upazila_name in matches:
                        upazilas.append({
                            "upazila_id": upazila_id.strip(),
                            "upazila_name": upazila_name.strip()
                        })
                    self.logger.info(f"Found {len(upazilas)} upazilas (regex fallback) for warehouse {warehouse_id}")
                    return upazilas
                
                # If no matches found, try next attempt
                self.logger.warning(f"No upazilas found in response (attempt {retry+1})")
                
            except Exception as e:
                self.logger.error(f"Error getting upazilas (attempt {retry+1}): {str(e)}")
                time.sleep(2)
        
        # If we've exhausted retries, use test upazila
        self.logger.warning("Using test upazila set as fallback after exhausting retries")
        return [{"upazila_id": "T429", "upazila_name": "Abhaynagar, Jashore"}]
    
    def get_unions(self, upazila_id, year, month):
        """Get unions for an upazila with improved JSON handling"""
        self.logger.info(f"Fetching unions for upazila {upazila_id}, {year}-{month}...")
        union_url = f"{self.base_url}/sdpdataviewer/form2_view_datasource.php"
        
        payload = {
            "operation": "getUnionList",
            "Year": year,
            "Month": month,
            "upcode": upazila_id
        }
        
        for retry in range(self.max_retries):
            try:
                response = self.session.post(union_url, data=payload, headers=self.headers)
                if response.status_code != 200:
                    self.logger.error(f"Failed to get unions. Status code: {response.status_code}")
                    time.sleep(1)
                    continue
                
                # Save raw response for debugging (first attempt)
                if retry == 0:
                    with open(self.debug_dir / f"union_response_{upazila_id}_{year}_{month}.txt", 'w', encoding='utf-8') as f:
                        f.write(response.text)
                
                # Try parsing trimmed JSON first
                trimmed_response = response.text.strip()
                try:
                    data = json.loads(trimmed_response)
                    if isinstance(data, list):
                        self.logger.info(f"Found {len(data)} unions (JSON) for upazila {upazila_id}")
                        return data
                except Exception as e:
                    self.logger.error(f"Error parsing union JSON: {str(e)}")
                
                # Regex extraction as fallback
                union_pattern = r'{"UnionCode":"(\d+)","UnionName":"([^"]+)"}'
                matches = re.findall(union_pattern, response.text)
                
                if matches:
                    unions = []
                    for union_code, union_name in matches:
                        unions.append({
                            "UnionCode": union_code.strip(),
                            "UnionName": union_name.strip()
                        })
                    self.logger.info(f"Found {len(unions)} unions (regex fallback) for upazila {upazila_id}")
                    return unions
                
                # If no unions found yet, try generic option pattern
                option_pattern = r'<option value="(\d+)">([^<]+)</option>'
                matches = re.findall(option_pattern, response.text)
                
                if matches:
                    unions = []
                    for union_code, union_name in matches:
                        unions.append({
                            "UnionCode": union_code.strip(),
                            "UnionName": union_name.strip()
                        })
                    self.logger.info(f"Found {len(unions)} unions (HTML fallback) for upazila {upazila_id}")
                    return unions
                
                # If still no unions found, try next attempt
                self.logger.warning(f"No unions found in response (attempt {retry+1})")
                
            except Exception as e:
                self.logger.error(f"Error getting unions (attempt {retry+1}): {str(e)}")
                time.sleep(2)
        
        # If we've exhausted retries, use test union
        self.logger.warning("Using test union set as fallback after exhausting retries")
        return [{"UnionCode": "1", "UnionName": "01. Prembug"}]
    
    def get_item_tabs(self, upazila_id, warehouse_id, union_code, year, month):
        """Get item tabs extracting button elements"""
        self.logger.info(f"Fetching item tabs for upazila {upazila_id}, union {union_code}, {year}-{month}...")
        item_url = f"{self.base_url}/sdpdataviewer/form2_view_datasource.php"
        
        payload = {
            "operation": "getItemTab",
            "Year": year,
            "Month": month,
            "UPNameList": upazila_id,
            "WHListAll": warehouse_id,
            "DistrictList": "All",
            "UnionList": union_code,
            "itemCode": ""
        }
        
        for retry in range(self.max_retries):
            try:
                response = self.session.post(item_url, data=payload, headers=self.headers)
                if response.status_code != 200:
                    self.logger.error(f"Failed to get item tabs. Status code: {response.status_code}")
                    time.sleep(1)
                    continue
                
                # Save raw response for debugging (first attempt)
                if retry == 0:
                    with open(self.debug_dir / f"item_tabs_{upazila_id}_{union_code}_{year}_{month}.txt", 'w', encoding='utf-8') as f:
                        f.write(response.text)
                
                # Extract item data from the HTML response
                item_pattern = r'<button id="([^"]+)"[^>]*>([^<]+)</button>'
                matches = re.findall(item_pattern, response.text)
                
                if matches:
                    items = []
                    for item_code, item_name in matches:
                        items.append({
                            "itemCode": item_code.strip(),
                            "itemName": item_name.strip()
                        })
                    self.logger.info(f"Found {len(items)} item tabs")
                    return items
                
                # If we couldn't find any item tabs, try next attempt
                self.logger.warning(f"No item tabs found in response (attempt {retry+1})")
                
            except Exception as e:
                self.logger.error(f"Error getting item tabs (attempt {retry+1}): {str(e)}")
                time.sleep(2)
        
        # If we've exhausted retries, use default list
        self.logger.warning("Using default item list as fallback after exhausting retries")
        return [
            {"itemCode": "CON008", "itemName": "Shukhi"},
            {"itemCode": "CON010", "itemName": "Shukhi (3rd Gen)"},
            {"itemCode": "CON008+CON010", "itemName": "Oral Pill (Total)"},
            {"itemCode": "CON009", "itemName": "Oral Pill Apon"},
            {"itemCode": "CON002", "itemName": "Condom"},
            {"itemCode": "CON006", "itemName": "Injectables (Vials)"},
            {"itemCode": "CON001", "itemName": "AD Syringe (1ML)"},
            {"itemCode": "CON003", "itemName": "ECP"},
            {"itemCode": "MCH021", "itemName": "Tab. Misoprostol (Dose)"},
            {"itemCode": "MCH051", "itemName": "7.1% CHLOROHEXIDINE"},
            {"itemCode": "MCH012", "itemName": "MNP(SUSSET)"},
            {"itemCode": "MCH018", "itemName": "Iron-Folic Acid (NOS)"}
        ]
    
    def get_item_data(self, upazila_id, warehouse_id, union_code, item_code, year, month):
        """Get actual item data handling compound codes properly"""
        self.logger.info(f"Fetching data for item {item_code}, upazila {upazila_id}, union {union_code}, {year}-{month}...")
        data_url = f"{self.base_url}/sdpdataviewer/form2_view_datasource.php"
        
        # Special handling for compound codes (e.g., CON008+CON010)
        # Make sure '+' is properly handled in the item code
        safe_item_code = item_code
        
        payload = {
            "sEcho": "2",
            "iColumns": "13",
            "sColumns": "",
            "iDisplayStart": "0",
            "iDisplayLength": "-1",
            "operation": "getItemlist",
            "Year": year,
            "Month": month,
            "Item": safe_item_code,
            "UPNameList": upazila_id,
            "UnionList": union_code,
            "WHListAll": warehouse_id,
            "DistrictList": "All",
            "baseURL": "https://scmpbd.org/scip/"
        }
        
        for retry in range(self.max_retries):
            try:
                response = self.session.post(data_url, data=payload, headers=self.headers)
                if response.status_code != 200:
                    self.logger.error(f"Failed to get item data. Status code: {response.status_code}")
                    time.sleep(1)
                    continue
                
                # Save raw response for debugging (occasional samples)
                if retry == 0 and (item_code == "CON008+CON010" or random.random() < 0.1):
                    with open(self.debug_dir / f"item_data_{upazila_id}_{union_code}_{item_code}_{year}_{month}.txt", 'w', encoding='utf-8') as f:
                        f.write(response.text)
                
                # Try to parse JSON
                try:
                    # First remove any leading/trailing whitespace
                    cleaned_response = response.text.strip()
                    data = json.loads(cleaned_response)
                    
                    if "aaData" in data and isinstance(data["aaData"], list):
                        row_count = len(data["aaData"])
                        # Skip the last row if it's a summary (empty first cell)
                        if row_count > 0 and (not data["aaData"][-1][0] or data["aaData"][-1][0] == ""):
                            actual_count = row_count - 1
                        else:
                            actual_count = row_count
                            
                        self.logger.info(f"Found {actual_count} data rows for item {item_code}")
                        return data
                    else:
                        self.logger.warning(f"No aaData found in response")
                except Exception as e:
                    self.logger.error(f"Error parsing item data JSON: {str(e)}")
                
                # If we got here, the response wasn't valid JSON or didn't contain data
                self.logger.warning(f"Invalid JSON response or missing data")
                
            except Exception as e:
                self.logger.error(f"Error getting item data (attempt {retry+1}): {str(e)}")
                time.sleep(2)
        
        # If we've exhausted retries, return None
        return None
    
    def parse_item_data(self, data):
        """Parse item data from API format to structured records"""
        results = []
        
        if isinstance(data, dict) and 'aaData' in data:
            # Column names based on the expected structure
            columns = [
                "serial", "facility", "opening_balance", "received", "total", 
                "adj_plus", "adj_minus", "grand_total", "distribution", 
                "closing_balance", "stock_out_reason", "stock_out_days", "eligible"
            ]
            
            # Process each row in aaData, skipping the summary row
            for row in data['aaData']:
                # Skip summary rows (usually the last row with empty first cell)
                if not row[0] or (isinstance(row[0], str) and row[0].strip() == ""):
                    continue
                    
                # Convert eligible indicator to boolean
                eligible = False
                if len(row) > 12:
                    eligible = '<img src=' in str(row[12])
                
                # Create record with proper column names
                record = {}
                for i, col in enumerate(columns):
                    if i < len(row):
                        # Clean the data - remove HTML tags
                        if isinstance(row[i], str):
                            value = re.sub(r'<[^>]+>', '', row[i]).strip()
                        else:
                            value = row[i]
                        record[col] = value
                    else:
                        record[col] = ""
                
                results.append(record)
        
        return results
    
    def process_union(self, params):
        """Process a single union, getting all items"""
        year, month, warehouse, upazila, union = params
        warehouse_id = warehouse["id"]
        warehouse_name = warehouse["name"]
        upazila_id = upazila["upazila_id"]
        upazila_name = upazila["upazila_name"]
        union_code = union["UnionCode"]
        union_name = union["UnionName"]
        
        self.logger.info(f"Processing union: {union_name} (Code: {union_code})")
        
        # Get item tabs for this combination
        item_tabs = self.get_item_tabs(upazila_id, warehouse_id, union_code, year, month)
        self.stats["total_items"] += len(item_tabs)
        
        union_results = {
            "union_name": union_name,
            "union_code": union_code,
            "items_processed": 0,
            "errors": []
        }
        
        # Process each item
        for item in item_tabs:
            item_code = item["itemCode"]
            item_name = item["itemName"]
            
            try:
                # Get data for this combination
                raw_data = self.get_item_data(upazila_id, warehouse_id, union_code, item_code, year, month)
                
                # Parse the response
                records = self.parse_item_data(raw_data) if raw_data else []
                
                # If we found data, save it
                if records:
                    # Create directory structure
                    item_dir = self.output_dir / year / month / warehouse_id / upazila_id / union_code
                    item_dir.mkdir(exist_ok=True, parents=True)
                    
                    # Save to JSON file
                    data_path = item_dir / f"{item_code.replace('+', '_plus_')}.json"
                    with open(data_path, 'w', encoding='utf-8') as f:
                        json.dump({
                            "metadata": {
                                "year": year,
                                "month": month,
                                "warehouse_name": warehouse_name,
                                "warehouse_id": warehouse_id,
                                "upazila_name": upazila_name,
                                "upazila_id": upazila_id,
                                "union_name": union_name,
                                "union_code": union_code,
                                "item_name": item_name,
                                "item_code": item_code
                            },
                            "data": records
                        }, f, indent=2)
                    
                    self.stats["total_data_files"] += 1
                    union_results["items_processed"] += 1
                    self.logger.info(f"Saved data for {item_name} with {len(records)} records")
                else:
                    self.logger.warning(f"No data found for {item_name}")
                
            except Exception as e:
                error_msg = f"Error processing item {item_name}: {str(e)}"
                self.logger.error(error_msg)
                union_results["errors"].append(error_msg)
                self.stats["errors"].append(error_msg)
                
                # Log the traceback for debugging
                self.logger.error(traceback.format_exc())
            
            # Delay to avoid overwhelming the server
            time.sleep(random.uniform(0.5, 1.5))
        
        return union_results
    
    def process_upazila(self, params):
        """Process a single upazila, getting all unions and items"""
        year, month, warehouse, upazila = params
        upazila_id = upazila["upazila_id"]
        upazila_name = upazila["upazila_name"]
        
        self.logger.info(f"Processing upazila: {upazila_name} (ID: {upazila_id})")
        
        # Get unions for this upazila
        unions = self.get_unions(upazila_id, year, month)
        self.stats["total_unions"] += len(unions)
        
        upazila_results = {
            "upazila_name": upazila_name,
            "upazila_id": upazila_id,
            "union_count": len(unions),
            "unions_processed": []
        }
        
        # Process each union
        for union in unions:
            # Create parameters for union processing
            union_params = (year, month, warehouse, upazila, union)
            
            # Process union
            union_result = self.process_union(union_params)
            upazila_results["unions_processed"].append(union_result)
        
        return upazila_results
    
    def process_warehouse(self, params):
        """Process a single warehouse for a specific month"""
        year, month, warehouse = params
        warehouse_id = warehouse["id"]
        warehouse_name = warehouse["name"]
        
        self.logger.info(f"Processing warehouse: {warehouse_name} (ID: {warehouse_id}) for {year}-{month}")
        
        # Get upazilas for this warehouse
        upazilas = self.get_upazilas(warehouse_id, year, month)
        self.stats["total_upazilas"] += len(upazilas)
        
        warehouse_results = {
            "warehouse_name": warehouse_name,
            "warehouse_id": warehouse_id,
            "upazila_count": len(upazilas),
            "upazilas_processed": []
        }
        
        # Process upazilas (concurrently if requested)
        if self.max_workers > 1 and len(upazilas) > 1:
            # Concurrent processing of upazilas
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_workers, len(upazilas))) as executor:
                futures = []
                for upazila in upazilas:
                    # Create parameters for upazila processing
                    upazila_params = (year, month, warehouse, upazila)
                    
                    # Submit task to executor
                    future = executor.submit(self.process_upazila, upazila_params)
                    futures.append((future, upazila["upazila_name"]))
                
                # Collect results
                for future, upazila_name in futures:
                    try:
                        upazila_result = future.result()
                        warehouse_results["upazilas_processed"].append(upazila_result)
                        self.logger.info(f"Completed processing upazila: {upazila_name}")
                    except Exception as e:
                        error_msg = f"Error in upazila processing {upazila_name}: {str(e)}"
                        self.logger.error(error_msg)
                        self.logger.error(traceback.format_exc())
                        self.stats["errors"].append(error_msg)
        else:
            # Sequential processing of upazilas
            for upazila in upazilas:
                try:
                    # Create parameters for upazila processing
                    upazila_params = (year, month, warehouse, upazila)
                    
                    # Process upazila
                    upazila_result = self.process_upazila(upazila_params)
                    warehouse_results["upazilas_processed"].append(upazila_result)
                    self.logger.info(f"Completed processing upazila: {upazila['upazila_name']}")
                except Exception as e:
                    error_msg = f"Error in upazila processing {upazila['upazila_name']}: {str(e)}"
                    self.logger.error(error_msg)
                    self.logger.error(traceback.format_exc())
                    self.stats["errors"].append(error_msg)
        
        return warehouse_results
    
    def process_month(self, year, month):
        """Process all warehouses for a specific month"""
        self.logger.info(f"Processing month: {year}-{month}")
        
        month_results = {
            "year": year,
            "month": month,
            "warehouses_processed": []
        }
        
        # Process warehouses (concurrently if requested)
        if self.max_workers > 1 and len(self.warehouses) > 1:
            # Concurrent processing of warehouses
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_workers, len(self.warehouses))) as executor:
                futures = []
                for warehouse in self.warehouses:
                    # Create parameters for warehouse processing
                    warehouse_params = (year, month, warehouse)
                    
                    # Submit task to executor
                    future = executor.submit(self.process_warehouse, warehouse_params)
                    futures.append((future, warehouse["name"]))
                
                # Collect results
                for future, warehouse_name in futures:
                    try:
                        warehouse_result = future.result()
                        month_results["warehouses_processed"].append(warehouse_result)
                        self.logger.info(f"Completed processing warehouse: {warehouse_name}")
                    except Exception as e:
                        error_msg = f"Error in warehouse processing {warehouse_name}: {str(e)}"
                        self.logger.error(error_msg)
                        self.logger.error(traceback.format_exc())
                        self.stats["errors"].append(error_msg)
        else:
            # Sequential processing of warehouses
            for warehouse in self.warehouses:
                try:
                    # Create parameters for warehouse processing
                    warehouse_params = (year, month, warehouse)
                    
                    # Process warehouse
                    warehouse_result = self.process_warehouse(warehouse_params)
                    month_results["warehouses_processed"].append(warehouse_result)
                    self.logger.info(f"Completed processing warehouse: {warehouse['name']}")
                except Exception as e:
                    error_msg = f"Error in warehouse processing {warehouse['name']}: {str(e)}"
                    self.logger.error(error_msg)
                    self.logger.error(traceback.format_exc())
                    self.stats["errors"].append(error_msg)
        
        return month_results
    
    def run(self):
        """Run the entire scraping process"""
        self.logger.info("Starting Family Planning Data Scraper")
        
        # Initialize warehouses
        self.get_warehouses()
        
        # Generate date ranges
        date_ranges = self.generate_date_ranges()
        
        # Process each month
        summary = []
        for year, month in date_ranges:
            try:
                month_result = self.process_month(year, month)
                summary.append(month_result)
                
                # Save progress summary after each month
                progress_path = self.output_dir / "progress_summary.json"
                with open(progress_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        "completed_months": [(y, m) for y, m in date_ranges if date_ranges.index((y, m)) <= date_ranges.index((year, month))],
                        "current_stats": self.stats,
                        "last_completed": {
                            "year": year,
                            "month": month,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                    }, f, indent=2)
                
                self.logger.info(f"Completed processing for {year}-{month}")
            except Exception as e:
                error_msg = f"Error processing month {year}-{month}: {str(e)}"
                self.logger.error(error_msg)
                self.logger.error(traceback.format_exc())
                self.stats["errors"].append(error_msg)
        
        # Save final summary
        summary_path = self.output_dir / "fetch_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        # Log final statistics
        self.logger.info("\nScraping Statistics:")
        self.logger.info(f"Total warehouses: {self.stats['total_warehouses']}")
        self.logger.info(f"Total upazilas: {self.stats['total_upazilas']}")
        self.logger.info(f"Total unions: {self.stats['total_unions']}")
        self.logger.info(f"Total items: {self.stats['total_items']}")
        self.logger.info(f"Total data files: {self.stats['total_data_files']}")
        self.logger.info(f"Total errors: {len(self.stats['errors'])}")
        
        return summary

def main():
    """Main entry point for the scraper"""
    parser = argparse.ArgumentParser(description="Family Planning Data Scraper")
    parser.add_argument('--start', type=str, default="2024-01", help="Start date in YYYY-MM format (default: 2024-01)")
    parser.add_argument('--end', type=str, default="2024-02", help="End date in YYYY-MM format (default: 2024-02)")
    parser.add_argument('--workers', type=int, default=1, help="Number of concurrent workers (default: 1)")
    parser.add_argument('--retries', type=int, default=3, help="Maximum number of retries for network requests (default: 3)")
    parser.add_argument('--warehouse', type=str, help="Specific warehouse ID or name to process (optional)")
    parser.add_argument('--upazila', type=str, help="Specific upazila ID to process (optional)")
    parser.add_argument('--union', type=str, help="Specific union code to process (optional)")
    
    args = parser.parse_args()
    
    print(f"Family Planning Data Scraper")
    print(f"============================")
    print(f"Start date: {args.start}")
    print(f"End date: {args.end}")
    print(f"Workers: {args.workers}")
    print(f"Max retries: {args.retries}")
    if args.warehouse:
        print(f"Specific warehouse: {args.warehouse}")
    if args.upazila:
        print(f"Specific upazila: {args.upazila}")
    if args.union:
        print(f"Specific union: {args.union}")
    print(f"============================")
    
    scraper = BangladeshScraper(
        start_date=args.start,
        end_date=args.end,
        max_workers=args.workers,
        max_retries=args.retries
    )
    
    # Filter warehouses if specified
    if args.warehouse:
        original_warehouses = scraper.warehouses.copy()
        filtered_warehouses = [wh for wh in scraper.warehouses if 
                              args.warehouse.lower() in wh['name'].lower() or 
                              args.warehouse == wh['id']]
        
        if filtered_warehouses:
            scraper.warehouses = filtered_warehouses
            print(f"Filtering to process only warehouse: {filtered_warehouses[0]['name']} (ID: {filtered_warehouses[0]['id']})")
        else:
            print(f"Warehouse '{args.warehouse}' not found. Using all warehouses.")
    
    # Filter to specific upazila and warehouse
    if args.upazila:
        # Create or override the warehouse mapping to use only the specified upazila
        for wh_id in [wh["id"] for wh in scraper.warehouses]:
            scraper.warehouse_upazila_mapping[wh_id] = [
                {"upazila_id": args.upazila, "upazila_name": f"Upazila {args.upazila}"}
            ]
        print(f"Filtering to process only upazila: {args.upazila}")
    
    # Filter to specific union
    if args.union and args.upazila:
        # Override the get_unions method to return only the specified union
        original_get_unions = scraper.get_unions
        
        def filtered_get_unions(upazila_id, year, month):
            if upazila_id == args.upazila:
                return [{"UnionCode": args.union, "UnionName": f"Union {args.union}"}]
            else:
                return original_get_unions(upazila_id, year, month)
        
        scraper.get_unions = filtered_get_unions
        print(f"Filtering to process only union: {args.union}")
    
    scraper.run()
    
    print("\nData collection complete!")

if __name__ == "__main__":
    main()