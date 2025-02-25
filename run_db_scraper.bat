@echo off
echo Installing requirements...
pip install -r requirements-db.txt

echo Creating database table if it doesn't exist...
sqlcmd -S %SERVER% -d %BANGLADESH_DATABASE% -U %UID% -P %PID% -i create_form_f3_table.sql

echo Starting database scraper...
python db_scraper.py --workers 4 --retries 5 %*

echo Done!
pause
