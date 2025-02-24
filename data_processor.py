import json
import time
import calendar
from pathlib import Path
import logging

logger = logging.getLogger("FamilyPlanningScraper")

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
                
                files_saved += self._process_single_item(year, month, warehouse, upazila, union, item_tab, output_dir)
        else:
            logger.warning(f"No item tabs found, falling back to predefined items list")
            # Fall back to predefined items
            for item in self.items:
                files_saved += self._process_single_item(year, month, warehouse, upazila, union, item, output_dir)
    except Exception as e:
        logger.error(f"Error getting item tabs: {str(e)}")
        # Fall back to predefined items
        for item in self.items:
            files_saved += self._process_single_item(year, month, warehouse, upazila, union, item, output_dir)
    
    return files_saved

def _process_single_item(self, year, month, warehouse, upazila, union, item, output_dir):
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

def process_upazila(self, year, month, warehouse, upazila, output_dir):
    """Process data for a single upazila"""
    upz_id = upazila.get('upazila_id')
    upz_name = upazila.get('upazila_name')
    
    if not upz_id or not upz_name:
        logger.warning(f"Invalid upazila data: {upazila}")
        return {"union_count": 0, "files_saved": 0, "errors": ["Invalid upazila data"]}
    
    logger.info(f"Processing upazila: {upz_name}")
    
    # Get unions for this upazila
    unions = self.get_unions(upz_id, year, month)
    logger.info(f"Found {len(unions)} unions for upazila {upz_name}")
    
    union_results = []
    files_saved = 0
    errors = []
    
    # Process each union
    for union in unions:
        try:
            union_files = self.process_union_data(year, month, warehouse, upazila, union, output_dir)
            files_saved += union_files
            
            union_results.append({
                "union_name": union.get('UnionName', 'Unknown'),
                "union_code": union.get('UnionCode', 'Unknown'),
                "files_saved": union_files
            })
            
        except Exception as e:
            error_msg = f"Error processing union {union.get('UnionName', 'Unknown')}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
        
        # Add a delay between unions to avoid overwhelming the server
        time.sleep(2)
    
    return {
        "union_count": len(unions),
        "files_saved": files_saved,
        "union_results": union_results,
        "errors": errors
    }

def process_warehouse_month(self, year, month, warehouse, output_dir):
    """Process data for a single warehouse for a specific month"""
    wh_id = warehouse['whrec_id']
    wh_name = warehouse['wh_name'].replace('&#039;', "'")
    
    logger.info(f"Processing warehouse: {wh_name} for {year}-{month}")
    
    # Get all upazilas for this warehouse and month
    upazilas = self.get_upazilas(year, month, wh_id)
    logger.info(f"Found {len(upazilas)} upazilas for warehouse {wh_name}")
    
    warehouse_summary = {
        'name': wh_name,
        'id': wh_id,
        'upazila_count': len(upazilas),
        'union_count': 0,
        'data_files': 0,
        'errors': [],
        'upazila_results': []
    }
    
    # Process each upazila
    for upazila in upazilas:
        try:
            upazila_result = self.process_upazila(year, month, warehouse, upazila, output_dir)
            
            warehouse_summary['union_count'] += upazila_result['union_count']
            warehouse_summary['data_files'] += upazila_result['files_saved']
            warehouse_summary['errors'].extend(upazila_result['errors'])
            
            warehouse_summary['upazila_results'].append({
                'upazila_name': upazila.get('upazila_name', 'Unknown'),
                'upazila_id': upazila.get('upazila_id', 'Unknown'),
                'union_count': upazila_result['union_count'],
                'files_saved': upazila_result['files_saved'],
                'union_results': upazila_result.get('union_results', [])
            })
            
        except Exception as e:
            error_msg = f"Error processing upazila {upazila.get('upazila_name', 'Unknown')}: {str(e)}"
            logger.error(error_msg)
            warehouse_summary['errors'].append(error_msg)
        
        # Add a delay between upazilas to avoid overwhelming the server
        time.sleep(5)
    
    # Save warehouse summary for this month
    log_dir = output_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    
    log_filename = f"{year}_{month}_{wh_id}_log.json"
    with open(log_dir / log_filename, 'w', encoding='utf-8') as f:
        json.dump(warehouse_summary, f, indent=2)
    
    return warehouse_summary

def process_month(self, year_month_tuple, output_dir):
    """Process all warehouses for a specific month"""
    year, month = year_month_tuple
    
    logger.info(f"\n{'='*50}\nProcessing data for {year}-{month}\n{'='*50}")
    
    month_name = calendar.month_name[int(month)]
    logger.info(f"Starting data collection for {month_name} {year}")
    
    monthly_summary = {
        'year': year,
        'month': month,
        'month_name': month_name,
        'warehouses': []
    }
    
    # Process each warehouse
    for warehouse in self.warehouses:
        try:
            warehouse_summary = self.process_warehouse_month(year, month, warehouse, output_dir)
            monthly_summary['warehouses'].append(warehouse_summary)
        except Exception as e:
            error_msg = f"Error processing warehouse {warehouse.get('wh_name', 'Unknown')}: {str(e)}"
            logger.error(error_msg)
            monthly_summary['warehouses'].append({
                'name': warehouse.get('wh_name', 'Unknown').replace('&#039;', "'"),
                'id': warehouse.get('whrec_id', 'Unknown'),
                'errors': [error_msg]
            })
        
        # Add a delay between warehouses to avoid overwhelming the server
        time.sleep(10)
    
    # Save monthly summary
    log_dir = output_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    
    monthly_log_filename = f"{year}_{month}_summary.json"
    with open(log_dir / monthly_log_filename, 'w', encoding='utf-8') as f:
        json.dump(monthly_summary, f, indent=2)
    
    logger.info(f"Completed data collection for {month_name} {year}")
    return monthly_summary
