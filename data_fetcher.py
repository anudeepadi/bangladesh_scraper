import time
from bs4 import BeautifulSoup
import logging
import json
import re

logger = logging.getLogger("FamilyPlanningScraper")

def get_upazilas(self, year, month, warehouse_id="All"):
    """Get list of all upazilas for a specific warehouse"""
    data = {
        "operation": "getSDPUPList",
        "Year": year,
        "Month": month,
        "gWRHId": warehouse_id,
        "gDistId": "All"
    }
    
    response = self._make_request('POST', f"{self.base_url}sdplist/sdplist_Processing.php", 
                                data=data, headers=self.headers)
    if not response:
        return []
        
    try:
        result = response.json()
        if isinstance(result, list):
            return result
        return result.get('data', [])
    except Exception as e:
        logger.error(f"Error parsing upazila data: {str(e)}")
        return []

def get_unions(self, upazila_code, year, month):
    """Get unions for a specific upazila"""
    data = {
        "operation": "getUnionList",
        "Year": year,
        "Month": month,
        "upcode": upazila_code
    }
    
    response = self._make_request('POST', f"{self.base_url}sdpdataviewer/form2_view_datasource.php", 
                                data=data, headers=self.headers)
    if not response:
        return []
        
    try:
        result = response.json()
        if isinstance(result, list):
            return result
        return result.get('data', [])
    except Exception as e:
        logger.error(f"Error parsing union data: {str(e)}")
        return []

def get_item_tab(self, year, month, upazila, warehouse_id="All", union="1"):
    """Get the item tabs available for a specific location"""
    data = {
        "operation": "getItemTab",
        "Year": year,
        "Month": month,
        "UPNameList": upazila,
        "WHListAll": warehouse_id,
        "DistrictList": "All",
        "UnionList": union,
        "itemCode": ""
    }
    
    response = self._make_request('POST', f"{self.base_url}sdpdataviewer/form2_view_datasource.php", 
                                data=data, headers=self.headers)
    if not response:
        return []
        
    try:
        # The response is HTML, not JSON, so we need to parse it with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        items = []
        
        for button in soup.find_all('button'):
            item_code = button.get('id')
            item_name = button.text
            if item_code:
                items.append({
                    'itemCode': item_code,
                    'itemName': item_name
                })
        
        logger.info(f"Parsed {len(items)} items from HTML response")
        return items
        
    except Exception as e:
        logger.error(f"Error parsing item tab data: {str(e)}")
        return []

def get_item_data(self, year, month, warehouse_id, upazila, union, item_code):
    """Get data for specific item and location using the exact API method"""
    # Use the exact API method from the specification
    data = {
        "sEcho": 2,
        "iColumns": 13,
        "sColumns": "",
        "iDisplayStart": 0,
        "iDisplayLength": -1,
        "operation": "getItemlist",
        "Year": year,
        "Month": month,
        "Item": item_code,
        "UPNameList": upazila,
        "UnionList": union,
        "WHListAll": warehouse_id,
        "DistrictList": "All",
        "baseURL": "https://scmpbd.org/scip/"
    }
    
    logger.info(f"Fetching data for item {item_code} via API")
    
    try:
        url = f"{self.base_url}sdpdataviewer/form2_view_datasource.php"
        response = self._make_request('POST', url, data=data, headers=self.headers)
        
        if response and response.text:
            try:
                result = response.json()
                # The data is in 'aaData' not 'data'
                if 'aaData' in result:
                    raw_data = result['aaData']
                    # Convert the raw array data to structured records
                    structured_data = []
                    
                    for row in raw_data:
                        # Skip the "Grand Total" row which is the last one
                        if row[0] == '' and 'Grand Total' in row[1]:
                            continue
                            
                        record = {
                            'serial': row[0],
                            'facility': row[1],
                            'opening_balance': row[2],
                            'received': row[3],
                            'total': row[4],
                            'adj_plus': row[5],
                            'adj_minus': row[6],
                            'grand_total': row[7],
                            'distribution': row[8],
                            'closing_balance': row[9],
                            'stock_out_reason': row[10],
                            'stock_out_days': row[11],
                            'eligible': True if 'tick.png' in row[12] else False
                        }
                        
                        # Clean up HTML tags from values
                        for key, value in record.items():
                            if isinstance(value, str):
                                # Remove HTML tags
                                record[key] = re.sub(r'<[^>]+>', '', value).strip()
                        
                        structured_data.append(record)
                        
                    logger.info(f"Converted {len(structured_data)} rows from aaData format")
                    return structured_data
                    
                elif 'data' in result:
                    return result['data']
                elif isinstance(result, list):
                    return result
                else:
                    logger.warning(f"Unexpected API response format: {list(result.keys())}")
                    return []
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON response: {response.text[:100]}...")
        else:
            logger.warning("No response or empty response from API")
    except Exception as e:
        logger.error(f"Error with API call: {str(e)}")
    
    # If the API method fails, try direct scraping
    logger.info("API method failed, trying direct HTML scraping")
    return self.scrape_item_data(year, month, warehouse_id, upazila, union, item_code)

def scrape_item_data(self, year, month, warehouse_id, upazila, union, item_code):
    """Scrape item data from the web page as a fallback method"""
    logger.info(f"Attempting to scrape item data directly from web page")
    
    try:
        # Build the URL with parameters
        url = f"{self.base_url}sdpdataviewer/form2_view.php"
        params = {
            'Year': year,
            'Month': month,
            'WHListAll': warehouse_id,
            'DistrictList': 'All',
            'UPNameList': upazila,
            'UnionList': union,
            'Item': item_code
        }
        
        # Use GET request with params
        response = self._make_request('GET', url, params=params)
        
        if not response:
            return []
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Save HTML for debugging if needed
        # debug_file = f"debug_{year}_{month}_{warehouse_id}_{upazila}_{union}_{item_code}.html"
        # with open(debug_file, 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        # logger.info(f"Saved debug HTML to {debug_file}")
        
        # Find the data table
        table = soup.find('table', id='example')
        
        # If that fails, try finding any table with appropriate structure
        if not table:
            tables = soup.find_all('table')
            for t in tables:
                if t.find('thead') and t.find('tbody') and len(t.find_all('th')) > 5:
                    table = t
                    break
        
        if not table:
            logger.warning("Could not find data table in HTML")
            return []
        
        # Extract the data
        data = []
        rows = table.find('tbody').find_all('tr')
        
        for row in rows:
            if 'Grand Total' in row.text:
                continue  # Skip the total row
            
            cells = row.find_all('td')
            if len(cells) < 8:  # Require at least 8 cells
                continue  # Skip rows with too few cells
            
            try:
                record = {
                    'serial': cells[0].text.strip(),
                    'facility': cells[1].text.strip(),
                    'opening_balance': cells[2].text.strip(),
                    'received': cells[3].text.strip(),
                    'total': cells[4].text.strip(),
                    'adj_plus': cells[5].text.strip() if len(cells) > 5 else '0',
                    'adj_minus': cells[6].text.strip() if len(cells) > 6 else '0',
                    'grand_total': cells[7].text.strip() if len(cells) > 7 else cells[4].text.strip(),
                    'distribution': cells[8].text.strip() if len(cells) > 8 else '0',
                    'closing_balance': cells[9].text.strip() if len(cells) > 9 else '0',
                    'stock_out_reason': cells[10].text.strip() if len(cells) > 10 else '',
                    'stock_out_days': cells[11].text.strip() if len(cells) > 11 else '',
                    'eligible': True  # Assume eligible by default
                }
                
                data.append(record)
            except Exception as e:
                logger.error(f"Error processing row: {str(e)}")
        
        logger.info(f"Successfully scraped {len(data)} records from web page")
        return data
        
    except Exception as e:
        logger.error(f"Error scraping item data: {str(e)}")
        return []

def direct_download_excel(self, year, month, warehouse_id, upazila, union, item_code):
    """Attempt to download data directly as Excel as a last resort"""
    try:
        # Build the URL for Excel export
        url = f"{self.base_url}sdpdataviewer/form2_view_excel.php"
        params = {
            'Year': year,
            'Month': month,
            'WHListAll': warehouse_id,
            'DistrictList': 'All',
            'UPNameList': upazila,
            'UnionList': union,
            'Item': item_code
        }
        
        # Download the Excel file
        logger.info(f"Attempting to download Excel data")
        response = self._make_request('GET', url, params=params)
        
        if not response or not response.content:
            logger.warning("No Excel content returned")
            return None
        
        # Return the content for processing
        logger.info(f"Successfully downloaded Excel data ({len(response.content)} bytes)")
        return response.content
        
    except Exception as e:
        logger.error(f"Error downloading Excel: {str(e)}")
        return None
