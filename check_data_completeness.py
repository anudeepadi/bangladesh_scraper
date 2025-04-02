import os
import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set

class DataCompletenessChecker:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.expected_months = set(f"{i:02d}" for i in range(1, 13))
        self.expected_item_types = {
            'CON001', 'CON002', 'CON003', 'CON006', 'CON008', 
            'CON008_plus_CON010', 'MCH021'
        }
        self.results = defaultdict(dict)

    def check_year_completeness(self, year: str) -> Dict:
        """Check completeness for a specific year"""
        year_path = self.base_path / year
        if not year_path.exists():
            return {"error": f"Year {year} directory not found"}

        months_present = set()
        months_missing = set()
        districts_by_month = defaultdict(set)
        upazilas_by_district = defaultdict(set)
        unions_by_upazila = defaultdict(set)
        items_by_union = defaultdict(set)

        # Check months
        for month in self.expected_months:
            month_path = year_path / month
            if month_path.exists():
                months_present.add(month)
                # Check districts
                for district in month_path.iterdir():
                    if district.is_dir():
                        districts_by_month[month].add(district.name)
                        # Check upazilas
                        for upazila in district.iterdir():
                            if upazila.is_dir():
                                upazilas_by_district[district.name].add(upazila.name)
                                # Check unions
                                for union in upazila.iterdir():
                                    if union.is_dir():
                                        unions_by_upazila[upazila.name].add(union.name)
                                        # Check items
                                        for item_file in union.iterdir():
                                            if item_file.is_file() and item_file.suffix == '.json':
                                                items_by_union[union.name].add(item_file.stem)
            else:
                months_missing.add(month)

        return {
            "months_present": sorted(months_present),
            "months_missing": sorted(months_missing),
            "districts_by_month": {m: sorted(d) for m, d in districts_by_month.items()},
            "upazilas_by_district": {d: sorted(u) for d, u in upazilas_by_district.items()},
            "unions_by_upazila": {u: sorted(un) for u, un in unions_by_upazila.items()},
            "items_by_union": {u: sorted(i) for u, i in items_by_union.items()}
        }

    def check_all_years(self, start_year: int = 2015, end_year: int = 2024) -> Dict:
        """Check completeness for all years in the range"""
        for year in range(start_year, end_year + 1):
            year_str = str(year)
            self.results[year_str] = self.check_year_completeness(year_str)
        return self.results

    def generate_report(self) -> str:
        """Generate a human-readable report of the findings"""
        report = []
        report.append("Family Planning Data Completeness Report")
        report.append("=" * 50)

        for year, year_data in sorted(self.results.items()):
            report.append(f"\nYear {year}:")
            report.append("-" * 20)
            
            # Report months
            report.append(f"Months present: {', '.join(year_data['months_present'])}")
            if year_data['months_missing']:
                report.append(f"Months missing: {', '.join(year_data['months_missing'])}")
            
            # Report districts
            for month, districts in year_data['districts_by_month'].items():
                report.append(f"\nMonth {month} - Districts: {len(districts)}")
                for district, upazilas in year_data['upazilas_by_district'].items():
                    if district in districts:
                        report.append(f"  District {district} - Upazilas: {len(upazilas)}")
                        for upazila, unions in year_data['unions_by_upazila'].items():
                            if upazila in upazilas:
                                report.append(f"    Upazila {upazila} - Unions: {len(unions)}")
                                for union, items in year_data['items_by_union'].items():
                                    if union in unions:
                                        missing_items = self.expected_item_types - set(items)
                                        if missing_items:
                                            report.append(f"      Union {union} - Missing items: {', '.join(missing_items)}")

        return "\n".join(report)

def main():
    base_path = Path("family_planning_data")
    checker = DataCompletenessChecker(base_path)
    checker.check_all_years()
    
    # Generate and save report
    report = checker.generate_report()
    with open("data_completeness_report.txt", "w", encoding="utf-8") as f:
        f.write(report)
    
    # Save detailed results as JSON
    with open("data_completeness_details.json", "w", encoding="utf-8") as f:
        json.dump(checker.results, f, indent=2)

    print("Analysis complete. Reports saved to:")
    print("- data_completeness_report.txt (human-readable)")
    print("- data_completeness_details.json (detailed data)")

if __name__ == "__main__":
    main() 