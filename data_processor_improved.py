import json
import time
import calendar
from pathlib import Path
import logging
import re

logger = logging.getLogger("FamilyPlanningScraper")

# Dictionary to map warehouse names to districts
# This is a starting point - you may need to expand or correct this mapping
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

# Function to extract union name from facility string
def extract_union_from_facility(facility):
    """Extract union name from facility string"""
    if not facility:
        return ""
    
    # Pattern to match: digits followed by dot, space, then the union name
    match = re.search(r'\d+\.\s+(.*?)(?:\s*\(|$)', facility)
    if match:
        return match.group(1).strip()
    return ""

def process_union_data(self, year, month, warehouse, upazila, union, output_dir):
    """Process data for a single union"""
    union_code = union.get('UnionCode')
    union_name = union.get('UnionName')
    
    if not union_code or not union_name:
        logger.warning(f"Invalid union data: {union}")
        return 0
    
    logger.debug(f"Processing union: {union_name}")
    
    wh_id = warehouse['whrec_id']
    wh_name = warehouse['wh_name'].replace('&#039;', "'")
    upz_id = upazila.get('upazila_id')
    upz_name = upazila.get('upazila_name')
    
    # Get district from warehouse name
    district = WAREHOUSE_DISTRICT_MAP.get(wh_name, "")
    
    files_saved = 0
    errors = []
    
    # Try to get the available item tabs first
    try:
        logger.info(f"Getting available item tabs for {upz_name}, {union_name}")
        item_tabs = self.get_item_tab(year, month, upz_id, wh_id, union_code)
        
        if item_tabs and len(item_tabs) > 0:
            logger.info(f"Found {len(item_tabs)} item tabs: {', '.join([tab.get('itemName', 'Unknown') for tab in item_tabs])}")
            # Process each available item in the tabs
            for item_tab in item_tabs:
                item_code = item_tab.get('itemCode')
                item_name = item_tab.get('itemName')
                
                if not item_code or not item_name:
                    continue
                
                files_saved += self._process_single_item(year, month, warehouse, upazila, union, item_tab, output_dir, district)
        else:
            logger.warning(f"No item tabs found, falling back to predefined items list")
            # Fall back to predefined items
            for item in self.items:
                files_saved += self._process_single_item(year, month, warehouse, upazila, union, item, output_dir, district)
    except Exception as e:
        logger.error(f"Error getting item tabs: {str(e)}")
        # Fall back to predefined items
        for item in self.items:
            files_saved += self._process_single_item(year, month, warehouse, upazila, union, item, output_dir, district)
    
    return files_saved

def _process_single_item(self, year, month, warehouse, upazila, union, item, output_dir, district=""):
    """Process a single item for a union"""
    wh_id = warehouse['whrec_id']
    wh_name = warehouse['wh_name'].replace('&#039;', "'")
    upz_id = upazila.get('upazila_id')
    upz_name = upazila.get('upazila_name')
    union_code = union.get('UnionCode')
    union_name = union.get('UnionName')
    item_code = item.get('itemCode')
    item_name = item.get('itemName')
    
    logger.info(f"Processing item {item_name} for {union_name}, {upz_name}")
    
    try:
        # Get data with retries and fallbacks
        data = None
        
        # Strategy 1: API method
        data = self.get_item_data(year, month, wh_id, upz_id, union_code, item_code)
        
        # Strategy 2: If first method fails, try direct Excel download
        if not data:
            logger.info(f"API method failed, trying Excel download")
            excel_data = self.direct_download_excel(year, month, wh_id, upz_id, union_code, item_code)
            if excel_data:
                # Process Excel data if implemented
                # This would need Excel parsing implementation
                pass
        
        if data:
            # Process each record to enhance it with additional data
            for record in data:
                # Extract union name from facility
                facility_union = extract_union_from_facility(record.get('facility', ''))
                
                # Ensure stock_out fields are properly formatted even if empty
                if not record.get('stock_out_reason'):
                    record['stock_out_reason'] = ""
                if not record.get('stock_out_days'):
                    record['stock_out_days'] = ""
                
                # Add additional metadata
                record['district'] = district
                record['union_name'] = union_name
                record['union_code'] = union_code
                record['facility_union'] = facility_union
            
            # Create subfolder structure: output_dir/year/month/warehouse
            year_dir = output_dir / year
            month_dir = year_dir / month
            wh_dir = month_dir / wh_id
            
            # Create directories if they don't exist
            wh_dir.mkdir(parents=True, exist_ok=True)
            
            # Save data to file
            filename = f"{upz_id}_{union_code}_{item_code}.json"
            filepath = wh_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump({
                    'metadata': {
                        'year': year,
                        'month': month,
                        'warehouse_name': wh_name,
                        'warehouse_id': wh_id,
                        'district': district,
                        'upazila_name': upz_name,
                        'upazila_id': upz_id,
                        'union_name': union_name,
                        'union_code': union_code,
                        'item_name': item_name,
                        'item_code': item_code
                    },
                    'data': data
                }, f, indent=2)
            
            logger.info(f"Saved data for {item_name} in {union_name}, {upz_name} ({len(data)} records)")
            return 1
        else:
            logger.warning(f"No data found for {item_name} in {union_name}, {upz_name}")
            return 0
            
    except Exception as e:
        logger.error(f"Error processing item {item_name}: {str(e)}")
        return 0
