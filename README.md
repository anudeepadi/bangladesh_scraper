# Family Planning Data Scraper

A tool for collecting family planning data from elmis.dgfp.gov.bd across warehouses, upazilas, and unions in Bangladesh.

## Features

- Scrapes family planning data for multiple items from 2016-2025
- Processes all warehouses, upazilas, and unions
- Uses multiple fallback strategies to ensure data collection
- Saves data in a structured JSON format
- Can convert collected data to CSV format
- Generates summary statistics

## Installation

1. Clone this repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Data Collection

To collect data using default settings (sequential processing from 2016-12 to 2025-01):

```bash
python main.py
```

With custom options:

```bash
python main.py --start 2020-01 --end 2022-12 --workers 4 --resume 2020-06 --warehouse "Dhaka CWH"
```

Parameters:
- `--start`: Start date in YYYY-MM format (default: 2016-12)
- `--end`: End date in YYYY-MM format (default: 2025-01)
- `--workers`: Number of concurrent workers (default: 1)
- `--resume`: Resume from date in YYYY-MM format (optional)
- `--warehouse`: Specific warehouse ID or name to process (optional)
- `--retries`: Maximum number of retries for network requests (default: 5)

### Data Conversion

To convert the collected JSON files to CSV format:

```bash
python converter.py
```

With custom options:

```bash
python converter.py --input custom_data_dir --output custom_output_dir --batch-size 500 --stats-only
```

Parameters:
- `--input`: Input directory containing JSON files (default: family_planning_data)
- `--output`: Output directory for CSV files (default: csv_output)
- `--batch-size`: Number of files to process in each batch (default: 1000)
- `--stats-only`: Only process summary statistics, not individual data files

## Data Structure

### Input Data

The scraper collects data for the following items:
- Shukhi (CON008)
- Shukhi (3rd Gen) (CON010)
- Oral Pill (Total) (CON008+CON010)
- Oral Pill Apon (CON009)
- Condom (CON002)
- Injectables (Vials) (CON006)
- AD Syringe (1ML) (CON001)
- ECP (CON003)
- Tab. Misoprostol (Dose) (MCH021)
- 7.1% CHLOROHEXIDINE (MCH051)
- MNP(SUSSET) (MCH012)
- Iron-Folic Acid (NOS) (MCH018)

### Output Format

Data is saved in JSON files with the following structure:
```json
{
  "metadata": {
    "year": "2022",
    "month": "01",
    "warehouse_name": "Dhaka CWH",
    "warehouse_id": "8",
    "upazila_name": "Dhamrai, Dhaka",
    "upazila_id": "T097",
    "union_name": "01. Amta",
    "union_code": "1",
    "item_name": "Condom",
    "item_code": "CON002"
  },
  "data": [
    {
      "serial": "1",
      "facility": "1/Ka, NAME, FWA, 01. Amta",
      "opening_balance": "500",
      "received": "500",
      "total": "1000",
      "adj_plus": "0",
      "adj_minus": "0",
      "grand_total": "1000",
      "distribution": "264",
      "closing_balance": "736",
      "stock_out_reason": "",
      "stock_out_days": "",
      "eligible": true
    },
    ...
  ]
}
```

## Troubleshooting

If you encounter issues with data collection:

1. Check the log files in the `logs` directory for detailed error messages
2. Try increasing the number of retries with `--retries 10`
3. Use single-threaded mode (default) for more stable collection
4. Try specifying a single warehouse to isolate issues

## License

MIT

## Acknowledgements

This tool was created to facilitate access to important family planning data for analysis and research purposes.
