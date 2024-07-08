# Stock Data Dump Project

## Overview
This project is designed to fetch historical stock data for a predefined list of companies and store it in a MySQL database. It utilizes Python, Spark, and the Alpha Vantage API to perform data extraction, transformation, and loading (ETL) tasks.

## Features
- Fetch historical stock data using the Alpha Vantage API.
- Process and transform data using Apache Spark.
- Store the transformed data in a MySQL database.
- Configurable list of companies and date range for data fetching.

## Prerequisites
- Python 3.12 or higher
- Apache Spark
- MySQL Database
- An API key from Alpha Vantage

## Installation
1. Clone the repository to your local machine.
2. Install the required Python dependencies:
   For Installing poetry : <pre> pip install poetry </pre>
   For Installing packages defined in poetry.toml file : <pre> poetry install </pre>
   
## Steps to Execute
- Kindly Do Changes in the config.yml files related to the database, companies date range and API Key.
- Run the following command to execute the ETL process:
  For historical data:<pre> poetry run python historical_dump.py </pre>
  For day-to-day data (Note: Day -1 data requires a premium version API key):<pre> poetry run python daily_dump.py </pre>
   
   