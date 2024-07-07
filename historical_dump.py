from utils import load_config, create_spark_session, create_dataframe, fetch_and_process_data, write_to_database

def main():
    # Load configuration
    config = load_config()
    spark = create_spark_session(jars_path=config['spark']['jars_path'])

    companies = config['companies']
    api_key = config['api_key']
    db_config = config['database']
    start_date = config['date_range']['start_date']
    end_date = config['date_range']['end_date']

    all_data = fetch_and_process_data(companies, api_key, start_date, end_date)
    if all_data:
        df = create_dataframe(spark, all_data)
        write_to_database(df, db_config, "overwrite")

    spark.stop()

if __name__ == "__main__":
    main()
