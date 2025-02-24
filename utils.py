import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import json

def setup_logging(logger_name):
    """Set up logging configuration"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers if any
    if logger.handlers:
        logger.handlers.clear()
    
    # File handler for detailed logs
    file_handler = logging.FileHandler(log_dir / f"{logger_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    file_handler.setLevel(logging.INFO)
    
    # Console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatter and add to handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def save_json(data, filepath):
    """Save data to a JSON file"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

def convert_to_csv(json_dir, output_file):
    """Convert JSON data files to a CSV file"""
    import glob
    
    # Find all JSON files
    json_files = glob.glob(f"{json_dir}/**/*.json", recursive=True)
    
    # Extract data from each file
    data = []
    for file in json_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
                
            metadata = file_data.get('metadata', {})
            
            for record in file_data.get('data', []):
                row = {**metadata}
                row.update(record)
                data.append(row)
        except Exception as e:
            print(f"Error processing {file}: {str(e)}")
    
    # Create DataFrame and save to CSV
    if data:
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False)
        print(f"Saved {len(df)} records to {output_file}")
    else:
        print("No data found to convert")

def calculate_summary_stats(json_dir):
    """Calculate summary statistics from JSON files"""
    import glob
    
    # Find all JSON files
    json_files = glob.glob(f"{json_dir}/**/*.json", recursive=True)
    
    # Extract data
    data = []
    for file in json_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
                
            metadata = file_data.get('metadata', {})
            
            for record in file_data.get('data', []):
                row = {**metadata}
                row.update(record)
                data.append(row)
        except Exception as e:
            print(f"Error processing {file}: {str(e)}")
    
    # Create DataFrame
    if data:
        df = pd.DataFrame(data)
        
        # Calculate statistics
        stats = {
            'total_records': len(df),
            'warehouses': df['warehouse_name'].nunique(),
            'upazilas': df['upazila_name'].nunique(),
            'unions': df['union_name'].nunique(),
            'items': df['item_name'].nunique(),
            'years': df['year'].nunique(),
            'months': df['month'].nunique()
        }
        
        # Add more detailed statistics
        if 'opening_balance' in df.columns:
            df['opening_balance'] = pd.to_numeric(df['opening_balance'], errors='coerce')
            df['closing_balance'] = pd.to_numeric(df['closing_balance'], errors='coerce')
            df['received'] = pd.to_numeric(df['received'], errors='coerce')
            df['distribution'] = pd.to_numeric(df['distribution'], errors='coerce')
            
            stats.update({
                'total_opening_balance': df['opening_balance'].sum(),
                'total_closing_balance': df['closing_balance'].sum(),
                'total_received': df['received'].sum(),
                'total_distribution': df['distribution'].sum()
            })
        
        return stats
    else:
        return {'error': 'No data found'}
