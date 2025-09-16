FROM python:3.13-slim

RUN apt-get update && apt-get install -y cron

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ./src ./src
COPY ./crontab /etc/cron.d/nhl_cron
COPY ./csvs ./csvs

RUN python ./src/nhl_data_parser.py build_from_csv_backup --csv_path ./csvs --sql_file_path ./src/create_and_load_tables.sql
RUN python ./src/nhl_data_parser --update 


RUN chmod 0644 /etc/cron.d/nhl_cron && \
    crontab /etc/cron.d/nhl_cron

CMD ["cron", "-f"]