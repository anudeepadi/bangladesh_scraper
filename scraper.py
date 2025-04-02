import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
from pathlib import Path
import logging
from datetime import datetime
import urllib.parse
import re

class FamilyPlanningLocationScraper:
    def __init__(self, base_url="https://elmis.dgfp.gov.bd/dgfplmis_reports", output_dir="location_data"):
        self.base_url = base_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Session for requests
        self.session = requests.Session()
        
        # Common headers to mimic browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Set up logging
        self.logger = self.setup_logging()
        
        # Target month and year
        self.month_name = "December"
        self.month_num = 12
        self.year = 2024
        
        # Storage for extracted data
        self.warehouses = []
        self.districts = {}
        self.upazilas = {}
        self.unions = {}
        self.combinations = []
    
    def setup_logging(self):
        """Set up logging configuration"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        logger = logging.getLogger("LocationScraper")
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        if logger.handlers:
            logger.handlers.clear()
        
        # File handler
        file_handler = logging.FileHandler(log_dir / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
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
    
    def extract_form_options(self, form_url):
        """Extract dropdown options from the report form page"""
        try:
            response = self.session.get(form_url, headers=self.headers)
            if response.status_code != 200:
                self.logger.error(f"Failed to access form page. Status code: {response.status_code}")
                return False
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find warehouse select element
            warehouse_select = soup.find('select', {'name': 'warehouse'})
            if warehouse_select:
                for option in warehouse_select.find_all('option'):
                    if option.get('value') and option.get('value') != '':
                        self.warehouses.append({
                            'id': option.get('value'),
                            'name': option.text.strip()
                        })
                self.logger.info(f"Extracted {len(self.warehouses)} warehouses")
            else:
                self.logger.warning("Warehouse select element not found")
            
            # We'll save what we have for now
            return len(self.warehouses) > 0
            
        except Exception as e:
            self.logger.error(f"Error extracting form options: {str(e)}")
            return False
    
    def get_district_options(self, warehouse_id):
        """Get districts for a warehouse using the report form's dynamic options"""
        district_url = f"{self.base_url}/ajax/get_district_options.php?warehouse_id={warehouse_id}"
        
        try:
            response = self.session.get(district_url, headers=self.headers)
            if response.status_code != 200:
                self.logger.error(f"Failed to get districts. Status code: {response.status_code}")
                return []
            
            # Response may be HTML options or JSON
            try:
                # Try parsing as JSON
                data = response.json()
                districts = []
                for item in data:
                    districts.append({
                        'id': item['id'],
                        'name': item['name']
                    })
            except:
                # Try parsing as HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                districts = []
                for option in soup.find_all('option'):
                    if option.get('value') and option.get('value') != '':
                        districts.append({
                            'id': option.get('value'),
                            'name': option.text.strip()
                        })
            
            self.districts[warehouse_id] = districts
            self.logger.info(f"Found {len(districts)} districts for warehouse {warehouse_id}")
            return districts
            
        except Exception as e:
            self.logger.error(f"Error getting district options: {str(e)}")
            return []
    
    def get_upazila_options(self, warehouse_id, district_id):
        """Get upazilas for a warehouse and district"""
        upazila_url = f"{self.base_url}/ajax/get_upazila_options.php?warehouse_id={warehouse_id}&district_id={district_id}"
        
        try:
            response = self.session.get(upazila_url, headers=self.headers)
            if response.status_code != 200:
                self.logger.error(f"Failed to get upazilas. Status code: {response.status_code}")
                return []
            
            # Parse response (similar to districts)
            try:
                data = response.json()
                upazilas = []
                for item in data:
                    upazilas.append({
                        'id': item['id'],
                        'name': item['name']
                    })
            except:
                soup = BeautifulSoup(response.text, 'html.parser')
                upazilas = []
                for option in soup.find_all('option'):
                    if option.get('value') and option.get('value') != '':
                        upazilas.append({
                            'id': option.get('value'),
                            'name': option.text.strip()
                        })
            
            key = f"{warehouse_id}_{district_id}"
            self.upazilas[key] = upazilas
            self.logger.info(f"Found {len(upazilas)} upazilas for district {district_id}")
            return upazilas
            
        except Exception as e:
            self.logger.error(f"Error getting upazila options: {str(e)}")
            return []
    
    def get_union_options(self, warehouse_id, upazila_id):
        """Get unions for a warehouse and upazila"""
        union_url = f"{self.base_url}/ajax/get_union_options.php?warehouse_id={warehouse_id}&upazila_id={upazila_id}"
        
        try:
            response = self.session.get(union_url, headers=self.headers)
            if response.status_code != 200:
                self.logger.error(f"Failed to get unions. Status code: {response.status_code}")
                return []
            
            # Parse response
            try:
                data = response.json()
                unions = []
                for item in data:
                    unions.append({
                        'id': item['id'],
                        'name': item['name']
                    })
            except:
                soup = BeautifulSoup(response.text, 'html.parser')
                unions = []
                for option in soup.find_all('option'):
                    if option.get('value') and option.get('value') != '':
                        unions.append({
                            'id': option.get('value'),
                            'name': option.text.strip()
                        })
            
            key = f"{warehouse_id}_{upazila_id}"
            self.unions[key] = unions
            self.logger.info(f"Found {len(unions)} unions for upazila {upazila_id}")
            return unions
            
        except Exception as e:
            self.logger.error(f"Error getting union options: {str(e)}")
            return []
    
    def collect_all_locations(self):
        """Collect all location combinations"""
        # First, get base form to extract warehouses
        form_url = f"{self.base_url}/report_form.php?report_id=sdp_stockout_status_by_upazila"
        
        if not self.extract_form_options(form_url):
            self.logger.error("Failed to extract form options. Cannot proceed.")
            return
        
        # If no warehouses found, try alternative approach with predefined warehouses
        if not self.warehouses:
            self.logger.warning("No warehouses found from form. Using alternative approach.")
            # Add default "All" warehouse as a fallback
            self.warehouses = [{'id': 'all', 'name': 'All'}]
        
        # Process each warehouse
        for warehouse in self.warehouses:
            warehouse_id = warehouse['id']
            warehouse_name = warehouse['name']
            
            self.logger.info(f"Processing warehouse: {warehouse_name}")
            
            # Get districts
            districts = self.get_district_options(warehouse_id)
            
            # If no districts, add warehouse-only combination
            if not districts:
                self.combinations.append({
                    'warehouse_id': warehouse_id,
                    'warehouse_name': warehouse_name,
                    'district_id': 'all',
                    'district_name': 'All',
                    'upazila_id': None,
                    'upazila_name': None,
                    'union_id': None,
                    'union_name': None
                })
                continue
            
            # Process each district
            for district in districts:
                district_id = district['id']
                district_name = district['name']
                
                self.logger.info(f"Processing district: {district_name}")
                
                # Get upazilas
                upazilas = self.get_upazila_options(warehouse_id, district_id)
                
                # If no upazilas, add warehouse-district combination
                if not upazilas:
                    self.combinations.append({
                        'warehouse_id': warehouse_id,
                        'warehouse_name': warehouse_name,
                        'district_id': district_id,
                        'district_name': district_name,
                        'upazila_id': None,
                        'upazila_name': None,
                        'union_id': None,
                        'union_name': None
                    })
                    continue
                
                # Process each upazila
                for upazila in upazilas:
                    upazila_id = upazila['id']
                    upazila_name = upazila['name']
                    
                    self.logger.info(f"Processing upazila: {upazila_name}")
                    
                    # Get unions
                    unions = self.get_union_options(warehouse_id, upazila_id)
                    
                    # If no unions, add warehouse-district-upazila combination
                    if not unions:
                        self.combinations.append({
                            'warehouse_id': warehouse_id,
                            'warehouse_name': warehouse_name,
                            'district_id': district_id,
                            'district_name': district_name,
                            'upazila_id': upazila_id,
                            'upazila_name': upazila_name,
                            'union_id': None,
                            'union_name': None
                        })
                        continue
                    
                    # Process each union
                    for union in unions:
                        union_id = union['id']
                        union_name = union['name']
                        
                        # Add complete combination
                        self.combinations.append({
                            'warehouse_id': warehouse_id,
                            'warehouse_name': warehouse_name,
                            'district_id': district_id,
                            'district_name': district_name,
                            'upazila_id': upazila_id,
                            'upazila_name': upazila_name,
                            'union_id': union_id,
                            'union_name': union_name
                        })
                    
                    # Rate limit to avoid overwhelming the server
                    time.sleep(0.5)
                
                # Rate limit between districts
                time.sleep(1)
    
    def generate_report_urls(self):
        """Generate report URLs for all combinations"""
        if not self.combinations:
            self.logger.warning("No combinations found to generate URLs")
            return
        
        # Add month and year info to all combinations
        for combo in self.combinations:
            combo['month'] = self.month_num
            combo['month_name'] = self.month_name
            combo['year'] = self.year
        
        # Generate URLs
        report_data = []
        for combo in self.combinations:
            location_str = f"Warehouse : {combo['warehouse_name']}, District : {combo.get('district_name', 'All')}"
            
            # Add upazila and union if available
            if combo.get('upazila_name'):
                location_str += f", Upazila : {combo['upazila_name']}"
            if combo.get('union_name'):
                location_str += f", Union : {combo['union_name']}"
            
            # Format header list
            header_list = [
                f"Month : {self.month_name}, Year : {self.year}",
                location_str,
                "Form 2 View"
            ]
            
            # Encode header list
            encoded_headers = urllib.parse.quote(json.dumps(header_list))
            
            # Generate unique report name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_name = f"SDP_Stock_out_Status_By_Upazila_on_{self.month_name}_{self.year}_{timestamp}"
            
            # Build URL
            report_url = (
                f"{self.base_url}/report/print_master_dynamic_column.php"
                f"?jBaseUrl={self.base_url}/"
                f"&lan=en-GB"
                f"&reportSaveName={report_name}"
                f"&reportHeaderList={encoded_headers}"
                f"&chart=-1"
            )
            
            # Add URL to combo data
            combo_with_url = combo.copy()
            combo_with_url['report_url'] = report_url
            report_data.append(combo_with_url)
        
        # Save URLs to CSV
        df = pd.DataFrame(report_data)
        output_file = self.output_dir / f"location_combinations_dec2024.csv"
        df.to_csv(output_file, index=False)
        
        self.logger.info(f"Generated and saved {len(report_data)} report URLs to {output_file}")
        
        # Also save as JSON
        json_output = self.output_dir / f"location_combinations_dec2024.json"
        with open(json_output, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=4)
        
        self.logger.info(f"Saved JSON version to {json_output}")
    
    def run(self):
        """Run the entire scraping process"""
        self.logger.info("Starting location scraping process")
        
        # Collect all location combinations
        self.collect_all_locations()
        
        # Generate report URLs
        self.generate_report_urls()
        
        self.logger.info("Completed location scraping process")

def extract_locations_from_url(example_url):
    """Analyze the example URL to identify location parameters"""
    # Extract the reportHeaderList parameter
    match = re.search(r'reportHeaderList=\[(.*?)\]', example_url)
    if not match:
        return {}
    
    # Decode the header list
    encoded_headers = match.group(1)
    # Replace URL-encoded quotes and brackets
    encoded_headers = encoded_headers.replace('%22', '"').replace('%5B', '[').replace('%5D', ']')
    
    # Extract location information from the second header item which contains location info
    try:
        headers = json.loads(f"[{encoded_headers}]")
        location_info = headers[1] if len(headers) > 1 else ""
        
        # Parse location info
        loc_data = {}
        
        # Extract warehouse
        wh_match = re.search(r'Warehouse\s*:\s*(.*?)(?:,|$)', location_info)
        if wh_match:
            loc_data['warehouse'] = wh_match.group(1).strip()
        
        # Extract district
        dist_match = re.search(r'District\s*:\s*(.*?)(?:,|$)', location_info)
        if dist_match:
            loc_data['district'] = dist_match.group(1).strip()
        
        # Extract upazila
        upz_match = re.search(r'Upazila\s*:\s*(.*?)(?:,|$)', location_info)
        if upz_match:
            loc_data['upazila'] = upz_match.group(1).strip()
        
        # Extract union
        union_match = re.search(r'Union\s*:\s*(.*?)(?:,|$)', location_info)
        if union_match:
            loc_data['union'] = union_match.group(1).strip()
        
        return loc_data
    except:
        return {}

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape location combinations from MOHFW portal")
    parser.add_argument('--output', type=str, default="location_data",
                        help="Output directory for location data (default: location_data)")
    parser.add_argument('--example-url', type=str, 
                        help="Example URL to analyze for location pattern (optional)")
    
    args = parser.parse_args()
    
    # If example URL provided, analyze it first
    if args.example_url:
        print("Analyzing example URL for location pattern...")
        locations = extract_locations_from_url(args.example_url)
        print(f"Detected locations: {locations}")
    
    # Create and run scraper
    scraper = FamilyPlanningLocationScraper(output_dir=args.output)
    scraper.run()

if __name__ == "__main__":
    main()