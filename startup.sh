# loading tables in db
python ./src/nhl_data_parser.py build_from_csv_backup --csv_path ./csvs --sql_file_path ./src/create_and_load_tables.sql

# initial update
python ./src/nhl_data_parser.py --update_database 

# adding cron tab
chmod 0644 /etc/cron.d/nhl_cron && crontab /etc/cron.d/nhl_cron

cron -f
