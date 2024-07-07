from datetime import datetime, timedelta
from utils import load_config, create_spark_session, create_dataframe, fetch_and_process_data, write_to_database

def main():
    # Load configuration
    config = load_config()
    spark = create_spark_session(jars_path=config['spark']['jars_path'])

    companies = config['companies']
    api_key = config['api_key']
    db_config = config['database']

    yesterday = (datetime.now() - timedelta(2)).strftime('%Y-%m-%d')
    end_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    all_data = fetch_and_process_data(companies, api_key, yesterday, end_date)
    if all_data:
        df = create_dataframe(spark, all_data)
        write_to_database(df, db_config, "append")

    spark.stop()

if __name__ == "__main__":
    main()
