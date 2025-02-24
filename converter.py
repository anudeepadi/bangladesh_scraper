import argparse
import pandas as pd
import json
import glob
from pathlib import Path
import logging
from tqdm import tqdm
from datetime import datetime

class FamilyPlanningDataConverter:
    def __init__(self, input_dir="family_planning_data", output_dir="csv_output"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Set up logging
        self.logger = self.setup_logging()
    
    def setup_logging(self):
        """Set up logging configuration"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        logger = logging.getLogger("DataConverter")
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        if logger.handlers:
            logger.handlers.clear()
        
        # File handler for detailed logs
        file_handler = logging.FileHandler(log_dir / f"converter_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
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
    
    def find_json_files(self):
        """Find all JSON files in the input directory"""
        pattern = f"{self.input_dir}/**/*.json"
        files = glob.glob(pattern, recursive=True)
        
        # Filter out log files
        data_files = [f for f in files if "/logs/" not in f and "summary.json" not in f]
        
        self.logger.info(f"Found {len(data_files)} JSON data files")
        return data_files
    
    def process_file(self, file_path):
        """Process a single JSON file and convert it to a dataframe"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract metadata
            metadata = data.get('metadata', {})
            year = metadata.get('year')
            month = metadata.get('month')
            warehouse_name = metadata.get('warehouse_name')
            warehouse_id = metadata.get('warehouse_id')
            upazila_name = metadata.get('upazila_name')
            upazila_id = metadata.get('upazila_id')
            union_name = metadata.get('union_name')
            union_code = metadata.get('union_code')
            item_name = metadata.get('item_name')
            item_code = metadata.get('item_code')
            
            # Extract item data
            item_data = data.get('data', [])
            
            # Create rows for the DataFrame
            rows = []
            for record in item_data:
                row = {
                    'year': year,
                    'month': month,
                    'warehouse_name': warehouse_name,
                    'warehouse_id': warehouse_id,
                    'upazila_name': upazila_name,
                    'upazila_id': upazila_id,
                    'union_name': union_name,
                    'union_code': union_code,
                    'item_name': item_name,
                    'item_code': item_code,
                    'serial': record.get('serial'),
                    'facility': record.get('facility'),
                    'opening_balance': record.get('opening_balance'),
                    'received': record.get('received'),
                    'total': record.get('total'),
                    'adj_plus': record.get('adj_plus'),
                    'adj_minus': record.get('adj_minus'),
                    'grand_total': record.get('grand_total'),
                    'distribution': record.get('distribution'),
                    'closing_balance': record.get('closing_balance'),
                    'stock_out_reason': record.get('stock_out_reason'),
                    'stock_out_days': record.get('stock_out_days'),
                    'eligible': record.get('eligible')
                }
                rows.append(row)
            
            return pd.DataFrame(rows)
        
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")
            return None
    
    def convert_to_csv(self, batch_size=1000):
        """Convert all JSON files to CSV format"""
        files = self.find_json_files()
        
        if not files:
            self.logger.warning("No JSON files found to convert")
            return
        
        # Process files in batches
        all_dfs = []
        batch_count = 0
        
        for i, file_path in enumerate(tqdm(files, desc="Processing files")):
            df = self.process_file(file_path)
            
            if df is not None and not df.empty:
                all_dfs.append(df)
            
            # When batch size is reached, save to CSV
            if len(all_dfs) >= batch_size:
                batch_count += 1
                self.save_batch(all_dfs, batch_count)
                all_dfs = []  # Clear the list
        
        # Save any remaining files
        if all_dfs:
            batch_count += 1
            self.save_batch(all_dfs, batch_count)
        
        self.logger.info(f"Conversion complete. Created {batch_count} CSV files.")
    
    def save_batch(self, dataframes, batch_num):
        """Save a batch of dataframes to CSV"""
        if not dataframes:
            return
        
        try:
            # Combine all dataframes in the batch
            combined_df = pd.concat(dataframes, ignore_index=True)
            
            # Save to CSV
            filename = f"family_planning_data_batch_{batch_num}.csv"
            filepath = self.output_dir / filename
            
            combined_df.to_csv(filepath, index=False)
            self.logger.info(f"Saved batch {batch_num} with {len(combined_df)} records to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error saving batch {batch_num}: {str(e)}")
    
    def process_summary_files(self):
        """Process summary JSON files to generate statistics"""
        summary_file = self.input_dir / 'fetch_summary.json'
        
        if not summary_file.exists():
            self.logger.warning("Summary file not found")
            return
        
        try:
            with open(summary_file, 'r', encoding='utf-8') as f:
                summary_data = json.load(f)
            
            # Extract summary stats
            monthly_stats = []
            
            for month in summary_data:
                year = month.get('year')
                month_num = month.get('month')
                
                warehouse_stats = []
                for wh in month.get('warehouses', []):
                    warehouse_stats.append({
                        'warehouse_name': wh.get('name'),
                        'warehouse_id': wh.get('id'),
                        'upazila_count': wh.get('upazila_count', 0),
                        'union_count': wh.get('union_count', 0),
                        'data_files': wh.get('data_files', 0),
                        'error_count': len(wh.get('errors', []))
                    })
                
                warehouse_df = pd.DataFrame(warehouse_stats)
                
                monthly_stats.append({
                    'year': year,
                    'month': month_num,
                    'warehouses': len(month.get('warehouses', [])),
                    'total_upazilas': warehouse_df['upazila_count'].sum(),
                    'total_unions': warehouse_df['union_count'].sum(),
                    'total_files': warehouse_df['data_files'].sum(),
                    'total_errors': warehouse_df['error_count'].sum()
                })
            
            # Save statistics to CSV
            stats_df = pd.DataFrame(monthly_stats)
            stats_file = self.output_dir / 'monthly_statistics.csv'
            stats_df.to_csv(stats_file, index=False)
            
            self.logger.info(f"Saved monthly statistics to {stats_file}")
            
            # Also save the warehouse-level statistics
            all_warehouse_stats = []
            for month in summary_data:
                year = month.get('year')
                month_num = month.get('month')
                
                for wh in month.get('warehouses', []):
                    all_warehouse_stats.append({
                        'year': year,
                        'month': month_num,
                        'warehouse_name': wh.get('name'),
                        'warehouse_id': wh.get('id'),
                        'upazila_count': wh.get('upazila_count', 0),
                        'union_count': wh.get('union_count', 0),
                        'data_files': wh.get('data_files', 0),
                        'error_count': len(wh.get('errors', []))
                    })
            
            warehouse_stats_df = pd.DataFrame(all_warehouse_stats)
            warehouse_stats_file = self.output_dir / 'warehouse_statistics.csv'
            warehouse_stats_df.to_csv(warehouse_stats_file, index=False)
            
            self.logger.info(f"Saved warehouse statistics to {warehouse_stats_file}")
            
        except Exception as e:
            self.logger.error(f"Error processing summary files: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Convert Family Planning JSON data to CSV")
    parser.add_argument('--input', type=str, default="family_planning_data",
                        help="Input directory containing JSON files (default: family_planning_data)")
    parser.add_argument('--output', type=str, default="csv_output",
                        help="Output directory for CSV files (default: csv_output)")
    parser.add_argument('--batch-size', type=int, default=1000,
                        help="Number of files to process in each batch (default: 1000)")
    parser.add_argument('--stats-only', action='store_true',
                        help="Only process summary statistics, not individual data files")
    
    args = parser.parse_args()
    
    converter = FamilyPlanningDataConverter(input_dir=args.input, output_dir=args.output)
    
    if args.stats_only:
        converter.process_summary_files()
    else:
        converter.convert_to_csv(batch_size=args.batch_size)
        converter.process_summary_files()

if __name__ == "__main__":
    main()
