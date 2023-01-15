import pandas as pd

class GenericPipelineInterface():
    def extract(self, source: str) -> pd.DataFrame:
        """Extract from source and return it as Dataframe

        Args:
            source (str): data source, i.e csv path

        Returns:
            pd.DataFrame: Dataframe contains data source
        """
        pass

    def transform(self, extracted_result: pd.DataFrame) -> pd.DataFrame:
        """Transform given extracted source then return DataFrame. 
        Don't implement anything here if your pipeline is using ELT approach

        Args:
            extracted_result (pd.DataFrame): Dataframe contains data source

        Returns:
            pd.DataFrame: Dataframe contains transformed data
        """
        pass

    def load(self, transformed_result: pd.DataFrame):
        """Load data to specific destination.
        It is up to you for the implementation whether load into postgre, bigquery, mysql etc.

        Args:
            transformed_result (pd.DataFrame): Dataframe contains transformed data.
        """
        pass

    def run():
        """Run the pipeline whether ETL or ELT
        """
        pass