# Currency-Exchange-Rates-Ingestion-Pipeline

designing and developing a data ingestion pipeline - in Python - that would ingest the currencies exchange rates (usd and eur based on egp) incrementally using https://openexchangerates.org/ api and load them locally partitioned by the month of ingestion, so you would have at the end of each day a file containing the exchange rate of that day located in the respective partition.

The pipeline scheduled using Airflow.
To excute airflow locally:

$ virtualenv airflow -p python3

$ source airflow/bin/activate

(airflow)$ pip install apache-airflow

$ airflow initdb

$ airflow webserver

$ airflow scheduler


the Web Server is listening to port 8080 of your local machine, open localhost:8080 in a browser to access the Web Server UI
put the .py file inside home/airflow/dags so it will apear in airflow UI
