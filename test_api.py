import requests
import json
import argparse
from pprint import pprint

def test_get_upazilas(year, month, warehouse_id="All"):
    """Test getting list of upazilas for a specific warehouse"""
    url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdplist/sdplist_Processing.php"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://elmis.dgfp.gov.bd',
        'Referer': 'https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view.php'
    }
    data = {
        "operation": "getSDPUPList",
        "Year": year,
        "Month": month,
        "gWRHId": warehouse_id,
        "gDistId": "All"
    }
    
    print(f"Testing get_upazilas with Year={year}, Month={month}, Warehouse={warehouse_id}")
    print(f"URL: {url}")
    print(f"Data: {data}")
    
    response = requests.post(url, data=data, headers=headers)
    
    print(f"Status code: {response.status_code}")
    if response.status_code == 200:
        try:
            result = response.json()
            print(f"Response type: {type(result)}")
            print(f"Response length: {len(result) if isinstance(result, list) else 'Not a list'}")
            print("First few results:")
            if isinstance(result, list):
                pprint(result[:5])
            else:
                if 'data' in result:
                    pprint(result['data'][:5])
                else:
                    pprint(result)
            
            # Save result to file
            with open(f"upazila_test_{year}_{month}_{warehouse_id}.json", "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
            print(f"Full result saved to upazila_test_{year}_{month}_{warehouse_id}.json")
            
        except Exception as e:
            print(f"Error parsing response: {str(e)}")
            print("Raw response content:")
            print(response.text[:500])  # Print first 500 chars
    else:
        print("Error response content:")
        print(response.text[:500])  # Print first 500 chars

def test_get_unions(upazila_code, year, month):
    """Test getting unions for a specific upazila"""
    url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view_datasource.php"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://elmis.dgfp.gov.bd',
        'Referer': 'https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view.php'
    }
    data = {
        "operation": "getUnionList",
        "Year": year,
        "Month": month,
        "upcode": upazila_code
    }
    
    print(f"Testing get_unions with Upazila={upazila_code}, Year={year}, Month={month}")
    print(f"URL: {url}")
    print(f"Data: {data}")
    
    response = requests.post(url, data=data, headers=headers)
    
    print(f"Status code: {response.status_code}")
    if response.status_code == 200:
        try:
            result = response.json()
            print(f"Response type: {type(result)}")
            print(f"Response length: {len(result) if isinstance(result, list) else 'Not a list'}")
            print("Results:")
            if isinstance(result, list):
                pprint(result)
            else:
                if 'data' in result:
                    pprint(result['data'])
                else:
                    pprint(result)
            
            # Save result to file
            with open(f"union_test_{upazila_code}_{year}_{month}.json", "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
            print(f"Full result saved to union_test_{upazila_code}_{year}_{month}.json")
            
        except Exception as e:
            print(f"Error parsing response: {str(e)}")
            print("Raw response content:")
            print(response.text[:500])
    else:
        print("Error response content:")
        print(response.text[:500])

def test_get_item_tab(year, month, upazila, warehouse_id="All", union="1"):
    """Test getting available item tabs"""
    url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view_datasource.php"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://elmis.dgfp.gov.bd',
        'Referer': 'https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view.php'
    }
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
    
    print(f"Testing get_item_tab with Year={year}, Month={month}, Upazila={upazila}, Union={union}")
    print(f"URL: {url}")
    print(f"Data: {data}")
    
    response = requests.post(url, data=data, headers=headers)
    
    print(f"Status code: {response.status_code}")
    if response.status_code == 200:
        try:
            result = response.json()
            print(f"Response type: {type(result)}")
            if isinstance(result, dict) and 'button' in result:
                print(f"Button items: {len(result['button'])}")
                print("Items:")
                pprint(result['button'])
            else:
                print("Raw response:")
                pprint(result)
            
            # Save result to file
            with open(f"item_tab_test_{upazila}_{year}_{month}.json", "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
            print(f"Full result saved to item_tab_test_{upazila}_{year}_{month}.json")
            
        except Exception as e:
            print(f"Error parsing response: {str(e)}")
            print("Raw response content:")
            print(response.text[:500])
    else:
        print("Error response content:")
        print(response.text[:500])

def test_get_item_data(year, month, warehouse_id, upazila, union, item_code):
    """Test getting data for a specific item"""
    url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view_datasource.php"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://elmis.dgfp.gov.bd',
        'Referer': 'https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view.php'
    }
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
    
    print(f"Testing get_item_data with Year={year}, Month={month}, Item={item_code}")
    print(f"  Upazila={upazila}, Union={union}, Warehouse={warehouse_id}")
    print(f"URL: {url}")
    print(f"Data: {data}")
    
    response = requests.post(url, data=data, headers=headers)
    
    print(f"Status code: {response.status_code}")
    if response.status_code == 200:
        try:
            result = response.json()
            print(f"Response type: {type(result)}")
            if isinstance(result, dict) and 'data' in result:
                print(f"Data items: {len(result['data'])}")
                print("First few records:")
                pprint(result['data'][:3])
            else:
                print("Raw response:")
                pprint(result[:3] if isinstance(result, list) and len(result) > 3 else result)
            
            # Save result to file
            with open(f"item_data_test_{item_code}_{upazila}_{union}_{year}_{month}.json", "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
            print(f"Full result saved to item_data_test_{item_code}_{upazila}_{union}_{year}_{month}.json")
            
        except Exception as e:
            print(f"Error parsing response: {str(e)}")
            print("Raw response content:")
            print(response.text[:500])
    else:
        print("Error response content:")
        print(response.text[:500])

def test_web_page_access(year, month, warehouse_id, upazila, union, item_code):
    """Test accessing the web page directly"""
    url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view.php"
    params = {
        'Year': year,
        'Month': month,
        'WHListAll': warehouse_id,
        'DistrictList': 'All',
        'UPNameList': upazila,
        'UnionList': union,
        'Item': item_code
    }
    
    print(f"Testing direct web page access")
    print(f"URL: {url}")
    print(f"Params: {params}")
    
    response = requests.get(url, params=params)
    
    print(f"Status code: {response.status_code}")
    if response.status_code == 200:
        # Save the HTML to a file
        filename = f"webpage_test_{year}_{month}_{upazila}_{union}_{item_code}.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"Full HTML saved to {filename}")
        
        # Check if the page contains a data table
        if 'id="example"' in response.text or '<table' in response.text:
            print("Page appears to contain a data table.")
        else:
            print("Page does not appear to contain a data table.")
    else:
        print("Error response content:")
        print(response.text[:500])

def main():
    parser = argparse.ArgumentParser(description="Test API endpoints for the Family Planning Data Scraper")
    parser.add_argument('--test', choices=['upazilas', 'unions', 'item-tab', 'item-data', 'webpage'], 
                       required=True, help="Which API test to run")
    parser.add_argument('--year', default="2023", help="Year to use (default: 2023)")
    parser.add_argument('--month', default="01", help="Month to use (default: 01)")
    parser.add_argument('--warehouse', default="All", help="Warehouse ID (default: All)")
    parser.add_argument('--upazila', help="Upazila ID (required for unions, item-tab, item-data, webpage)")
    parser.add_argument('--union', default="1", help="Union code (default: 1)")
    parser.add_argument('--item', default="CON008", help="Item code (default: CON008 - Shukhi)")
    
    args = parser.parse_args()
    
    # Validate required arguments for specific tests
    if args.test in ['unions', 'item-tab', 'item-data', 'webpage'] and not args.upazila:
        parser.error(f"--upazila is required for test '{args.test}'")
    
    # Run the appropriate test
    if args.test == 'upazilas':
        test_get_upazilas(args.year, args.month, args.warehouse)
    elif args.test == 'unions':
        test_get_unions(args.upazila, args.year, args.month)
    elif args.test == 'item-tab':
        test_get_item_tab(args.year, args.month, args.upazila, args.warehouse, args.union)
    elif args.test == 'item-data':
        test_get_item_data(args.year, args.month, args.warehouse, args.upazila, args.union, args.item)
    elif args.test == 'webpage':
        test_web_page_access(args.year, args.month, args.warehouse, args.upazila, args.union, args.item)

if __name__ == "__main__":
    main()
