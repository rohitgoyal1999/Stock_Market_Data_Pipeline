import yaml
import time
import requests
import jaydebeapi
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


def load_config(file_path='config.yml'):
    """Load configuration from a YAML file."""
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print("Configuration file not found.")
        exit(1)
    except yaml.YAMLError as e:
        print(f"Error reading configuration file: {e}")
        exit(1)

def create_spark_session(app_name="StockDataDump", jars_path="./jar_files/mysql-connector-java-8.0.26.jar"):
    """Create and configure a Spark session."""
    try:
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", jars_path) \
            .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
            .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
            .getOrCreate()
    except Exception as e:
        print(f"Failed to create Spark session: {e}")
        exit(1)

def fetch_stock_data(symbol, api_key, start_date, end_date):
    """Fetch stock data for a given symbol within a date range."""
    print(f"Fetching data for {symbol}")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}'
    try:
        response = requests.get(url)
        data = response.json()
        if 'Time Series (Daily)' in data:
            return parse_stock_data(data['Time Series (Daily)'], symbol, start_date, end_date)
        else:
            print(f"Error fetching data for {symbol}: {data.get('Note', 'Unknown error')}")
            return []
    except requests.RequestException as e:
        print(f"Request error for {symbol}: {e}")
        return []

def parse_stock_data(stock_data, symbol, start_date, end_date):
    """Parse stock data from API response."""
    rows = []
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    for date, values in stock_data.items():
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        if start_date <= date_obj <= end_date:
            row = (date, symbol, float(values['1. open']), float(values['4. close']),
                   float(values['2. high']), float(values['3. low']), int(values['5. volume']))
            rows.append(row)
    return rows

def fetch_and_process_data(companies, api_key, start_date, end_date):
    """Fetch and process stock data for a list of companies."""
    all_data = []
    for company in companies:
        all_data.extend(fetch_stock_data(company, api_key, start_date, end_date))
        time.sleep(12)  # Respect API call limits
    return all_data


def create_dataframe(spark, data):
    """Create a Spark DataFrame from stock data."""
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Company", StringType(), True),
        StructField("Open", FloatType(), True),
        StructField("Close", FloatType(), True),
        StructField("High", FloatType(), True),
        StructField("Low", FloatType(), True),
        StructField("Volume", IntegerType(), True)
    ])
    return spark.createDataFrame(data, schema=schema)

def write_to_database(df, db_config, mode):
    """Write the DataFrame to a database."""
    jdbc_url = f"jdbc:mysql://{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    try:
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "historical_stock_data") \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("autocommit","false") \
            .mode(mode) \
            .save()
        if mode =="overwrite":
            # Creating SQL index
            create_indexes_sql = [
                "CREATE INDEX idx_company_date ON historical_stock_data (Company(30), Date(10));",
                "CREATE INDEX idx_date ON historical_stock_data (Date(10));",
                "CREATE INDEX idx_company ON historical_stock_data (Company(30));"
            ]
            connection_properties={
                "user": db_config['user'],
                "password":db_config['password'],
                "driver": "com.mysql.cj.jdbc.Driver",
            }
            execute_index_queries(jdbc_url,connection_properties,create_indexes_sql)
    except Exception as e:
        print(f"Error writing to database: {e}")
        
        
def execute_index_queries(jdbc_url, connection_properties, queries):
    """Create Index"""
    config = load_config()
    conn = jaydebeapi.connect(
        "com.mysql.cj.jdbc.Driver",
        jdbc_url,
        [
            connection_properties['user'], connection_properties['password']
        ],
        config['spark']['jars_path']
    )
    conn.jconn.setAutoCommit(False)
    cursor = conn.cursor()
    for query in queries:
        cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()