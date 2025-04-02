import re
import json

def parse_item_tabs_html(html_content):
    """
    Parse item tabs from HTML button elements
    Sample: <button id="CON008" type="button" class="btn btn-default active">Shukhi</button>
    """
    items = []
    
    # Match pattern: id="ITEM_CODE" ... >ITEM_NAME</button>
    pattern = r'<button id="([^"]+)"[^>]*>([^<]+)<\/button>'
    matches = re.findall(pattern, html_content)
    
    for item_code, item_name in matches:
        items.append({
            "itemCode": item_code,
            "itemName": item_name.strip()
        })
    
    return items

def parse_item_data(data):
    """
    Parse item data from the aaData array format to properly structured records
    """
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
            # Skip summary rows (usually the last row)
            if isinstance(row[0], str) and row[0].strip() == "":
                continue
                
            # Convert eligible indicator to boolean
            eligible = False
            if len(row) > 12:
                eligible = '<img src=' in row[12]
            
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

def extract_json_from_response(response_text):
    """
    Try to extract valid JSON from a potentially mixed response
    """
    # First try parsing the entire text as JSON
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        pass
    
    # Try to find a JSON object pattern
    json_match = re.search(r'(\{[^{}]*".*":[^{}]*\})', response_text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass
    
    # Try to find a JSON array pattern
    json_match = re.search(r'(\[\s*{.*}\s*\])', response_text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass
    
    # Last resort, check for failure response
    if "{failure:true}" in response_text:
        return {"failure": True}
            
    return None

# Example usage
if __name__ == "__main__":
    # Test HTML parsing
    html_content = """
    <button id="CON008" type="button" class="btn btn-default active">Shukhi</button>
    <button id="CON010" type="button" class="btn btn-default ">Shukhi (3rd Gen)</button>
    <button id="CON008+CON010" type="button" class="btn btn-default ">Oral Pill (Total)</button>
    """
    
    items = parse_item_tabs_html(html_content)
    print("Parsed items:", items)
    
    # Test item data parsing
    item_data = {
        "aaData": [
            ["1", "Facility 1", "0", "0", "0", "0", "0", "0", "0", "0", "", "", "<img src='tick.png'>"],
            ["2", "Facility 2", "10", "5", "15", "0", "0", "15", "10", "5", "", "", "<img src='tick.png'>"],
            ["", "<span class=isBold>Grand Total</span>", "<span class=isBold>10</span>", "<span class=isBold>5</span>", "<span class=isBold>15</span>", "<span class=isBold>0</span>", "<span class=isBold>0</span>", "<span class=isBold>15</span>", "<span class=isBold>10</span>", "<span class=isBold>5</span>", "", "", ""]
        ]
    }
    
    records = parse_item_data(item_data)
    print("\nParsed records:", records)