# Configuration for Family Planning Data Fetcher

# API endpoints
BASE_URL = "https://elmis.dgfp.gov.bd/dgfplmis_reports/"
SCMBD_URL = "https://scmpbd.org/scip/"

# Items to scrape data for
ITEMS = [
    {"itemName": "Shukhi", "itemCode": "CON008"},
    {"itemName": "Shukhi (3rd Gen)", "itemCode": "CON010"},
    {"itemName": "Oral Pill (Total)", "itemCode": "CON008+CON010"},
    {"itemName": "Oral Pill Apon", "itemCode": "CON009"},
    {"itemName": "Condom", "itemCode": "CON002"},
    {"itemName": "Injectables (Vials)", "itemCode": "CON006"},
    {"itemName": "AD Syringe (1ML)", "itemCode": "CON001"},
    {"itemName": "ECP", "itemCode": "CON003"},
    {"itemName": "Tab. Misoprostol (Dose)", "itemCode": "MCH021"},
    {"itemName": "7.1% CHLOROHEXIDINE", "itemCode": "MCH051"},
    {"itemName": "MNP(SUSSET)", "itemCode": "MCH012"},
    {"itemName": "Iron-Folic Acid (NOS)", "itemCode": "MCH018"}
]

# Default settings
DEFAULT_START_DATE = "2016-12"
DEFAULT_END_DATE = "2025-01"
DEFAULT_MAX_WORKERS = 1
DEFAULT_MAX_RETRIES = 5

# HTTP request headers
HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://elmis.dgfp.gov.bd',
    'Referer': 'https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view.php'
}

# Fallback warehouse list if API fails
FALLBACK_WAREHOUSES = [
    {"whrec_id": "WH-011", "wh_name": "Bandarban RWH"},
    {"whrec_id": "WH-022", "wh_name": "Barishal RWH"},
    {"whrec_id": "WH-001", "wh_name": "Bhola RWH"},
    {"whrec_id": "WH-018", "wh_name": "Bogura RWH"},
    {"whrec_id": "WH-019", "wh_name": "Chattogram RWH"},
    {"whrec_id": "WH-020", "wh_name": "Cox's Bazar RWH"},
    {"whrec_id": "WH-014", "wh_name": "Cumilla RWH"},
    {"whrec_id": "WH-002", "wh_name": "Dhaka CWH"},
    {"whrec_id": "WH-021", "wh_name": "Dinajpur RWH"},
    {"whrec_id": "WH-003", "wh_name": "Faridpur RWH"},
    {"whrec_id": "WH-004", "wh_name": "Jamalpur RWH"},
    {"whrec_id": "WH-005", "wh_name": "Jashore RWH"},
    {"whrec_id": "WH-006", "wh_name": "Khulna RWH"},
    {"whrec_id": "WH-007", "wh_name": "Kushtia RWH"},
    {"whrec_id": "WH-008", "wh_name": "Mymensingh RWH"},
    {"whrec_id": "WH-009", "wh_name": "Noakhali RWH"},
    {"whrec_id": "WH-010", "wh_name": "Pabna RWH"},
    {"whrec_id": "WH-012", "wh_name": "Patuakhali RWH"},
    {"whrec_id": "WH-013", "wh_name": "Rajshahi RWH"},
    {"whrec_id": "WH-015", "wh_name": "Rangamati RWH"},
    {"whrec_id": "WH-016", "wh_name": "Rangpur RWH"},
    {"whrec_id": "WH-017", "wh_name": "Sylhet RWH"},
    {"whrec_id": "WH-023", "wh_name": "Tangail RWH"}
]
