from elt_datahub_bigquery_daily import DataHubToBigQueryPipeline
from etl_mysql_bigquery_hourly import MySQLToBigQueryPipeline
import argparse
from google.oauth2 import service_account

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Doing ETL/ELT from source to destination"
    )
    parser.add_argument('--type', help="type of ingestion whether 'etl' or 'elt'", choices=['etl', 'elt'], required=True)
    parser.add_argument('--to', help="BigQuery table ID destination with following format <DATASET_ID>.<TABLE_NAME>", required=True)
    parser.add_argument('--service-account', help="File path to Google's Service Account", required=True)

    args = parser.parse_args()
    credentials = service_account.Credentials.from_service_account_file(args.service_account)

    if args.type == 'etl':
        print('[ETL] STARTING!')
        MySQLToBigQueryPipeline(args.to, credentials).run()
        print('[ETL] DONE!')
    elif args.type == 'elt':
        print('[ELT] STARTING!')
        DataHubToBigQueryPipeline(args.to, credentials).run()
        print('[ELT] DONE!')