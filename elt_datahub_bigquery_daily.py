import datapackage
import pandas as pd
from pipeline import GenericPipelineInterface
from google.oauth2.credentials import Credentials
import logging
logging.basicConfig(filename='ingestion.log',
    filemode='a',
    format=f'%(asctime)s - {__file__} %(message)s', 
    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

class DataHubToBigQueryPipeline(GenericPipelineInterface):
    def __init__(self, table_id: str, credentials: Credentials):
        self.table_id = table_id
        self.project_id = "static-gravity-312212"
        self.credentials = credentials

    def extract(self, data_url: str) -> pd.DataFrame:
        # COPY THIS CODE FROM https://datahub.io/core/finance-vix#pandas
    
        # to load Data Package into storage
        package = datapackage.Package(data_url)

        # to load only tabular data
        resources = package.resources
        for resource in resources:
            if resource.tabular:
                data = pd.read_csv(resource.descriptor['path'])
                
                return data
        
        return None

    def transform(self, source: pd.DataFrame) -> pd.DataFrame:
        # Because we implement ELT, thus skip the transform at this point.
        pass

    def load(self, transformed_result: pd.DataFrame):
        columns = [ '_'.join(col.split()).lower() for col in transformed_result.columns ]
        transformed_result.columns = columns # renaming columns for the sake of compatible field name in BigQuery

        transformed_result.to_gbq(
            destination_table=self.table_id, 
            project_id=self.project_id,
            if_exists='replace',
            credentials=self.credentials) # to make the pipeline idempotent

    def run(self):
        data_url = "https://datahub.io/core/finance-vix/datapackage.json"
        logging.info(f"Extracting the data from {data_url}")
        source = self.extract(data_url)
        logging.info(f"Loading data to BigQuery with '{self.table_id}' as the table name")
        self.load(source)