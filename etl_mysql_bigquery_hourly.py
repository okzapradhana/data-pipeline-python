import pandas as pd
from pipeline import GenericPipelineInterface
from google.oauth2.credentials import Credentials
from sqlalchemy import create_engine
from pathlib import Path
import logging
logging.basicConfig(filename='ingestion.log',
    filemode='a',
    format=f'%(asctime)s - {Path(__file__).name} %(message)s', 
    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

class MySQLToBigQueryPipeline(GenericPipelineInterface):
    def __init__(self, table_id: str, credentials: Credentials):
        self.table_id = table_id
        self.project_id = "static-gravity-312212"
        self.credentials = credentials # service account

    def extract(self, data_url: str) -> pd.DataFrame:
        with create_engine(data_url).connect() as conn:
            df = pd.read_sql("SELECT * FROM employees", con=conn)
            return df

    def transform(self, source: pd.DataFrame) -> pd.DataFrame:
        df = source.copy()

        df['full_name'] = df['first_name'] + df['last_name']
        df['age'] = ((pd.Timestamp.now().date() - df['birth_date']).dt.days/365).round().astype(int)

        # to avoid pyarrow.lib.ArrowTypeError: Expected bytes, got a 'datetime.date' object
        df[['birth_date', 'hire_date']] = df[['birth_date', 'hire_date']].astype('string')

        return df

    def load(self, transformed_result: pd.DataFrame):
        transformed_result.to_gbq(
            destination_table=self.table_id, 
            project_id=self.project_id,
            if_exists='replace',
            credentials=self.credentials)

    def run(self):
        data_url = "mysql+pymysql://guest:relational@relational.fit.cvut.cz/employee"
        logging.info(f"Extracting the data from {data_url}")
        source = self.extract(data_url)
        logging.info("Transforming")
        transformed_result = self.transform(source)
        logging.info(f"Load data to {self.table_id}")
        self.load(transformed_result)