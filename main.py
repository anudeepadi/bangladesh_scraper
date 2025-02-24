import argparse
from scraper import FamilyPlanningDataFetcher

def main():
    parser = argparse.ArgumentParser(description="Family Planning Data Fetcher")
    parser.add_argument('--start', type=str, default="2016-12", help="Start date in YYYY-MM format (default: 2016-12)")
    parser.add_argument('--end', type=str, default="2025-01", help="End date in YYYY-MM format (default: 2025-01)")
    parser.add_argument('--resume', type=str, help="Resume from date in YYYY-MM format")
    parser.add_argument('--workers', type=int, default=1, help="Number of concurrent workers (default: 1)")
    parser.add_argument('--warehouse', type=str, help="Specific warehouse ID or name to process (optional)")
    parser.add_argument('--retries', type=int, default=5, help="Maximum number of retries for network requests")
    
    args = parser.parse_args()
    
    print(f"Family Planning Data Fetcher")
    print(f"============================")
    print(f"Start date: {args.start}")
    print(f"End date: {args.end}")
    print(f"Workers: {args.workers}")
    print(f"Max retries: {args.retries}")
    if args.resume:
        print(f"Resuming from: {args.resume}")
    if args.warehouse:
        print(f"Processing warehouse: {args.warehouse}")
    print(f"============================")
    
    fetcher = FamilyPlanningDataFetcher(
        start_date=args.start,
        end_date=args.end,
        max_workers=args.workers,
        max_retries=args.retries
    )
    
    summary = fetcher.fetch_all_data(resume_from=args.resume, specific_warehouse=args.warehouse)
    fetcher.generate_stats(summary)
    
    print("\nData collection complete!")

if __name__ == "__main__":
    main()
