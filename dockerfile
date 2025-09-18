FROM python:3.13

RUN apt-get update && apt-get install -y cron default-mysql-client

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ./src ./src
COPY ./crontab /etc/cron.d/nhl_cron
COPY ./csvs ./csvs
COPY ./database_creds.json ./database_creds.json
# COPY ./test_connection.py ./test_connection.py
COPY ./startup.sh ./startup.sh

CMD ["sh", "startup.sh"]