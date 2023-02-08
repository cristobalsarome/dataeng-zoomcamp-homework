from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

@task()
def extract_from_gcs(color:str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    file_name = f'{color}_tripdata_{year}-{month:02}.parquet'
    gcs_path = f'data/{color}/{file_name}'
    gcs_block = GcsBucket.load("gcs-bucket")
    local_path = f'../data/gcs_downloaded/{color}/{file_name}'
    gcs_block.get_directory(from_path = gcs_path,
                            local_path = local_path )
    return Path(local_path)

@task()
def transform(path: Path) -> pd.DataFrame:
    '''Data Cleaning example'''
    print(f'transforming file in {path}')
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # for the homework we don't transform
    # df['passenger_count'] = df['passenger_count'].fillna(0)
    print(f"pos: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("gcp-creds")
    df.to_gbq(
        destination_table= "prefect_zoomcamp.yellow-nyt-rides",
        project_id= "dataeng-zoomcamp-376312",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize= 500_000,
        if_exists='append'
    )
    print(f'Rows inserted: {len(df)}')

@flow(log_prints=True)
def etl_gcs_to_bq( year:int, month: int, color: str):
    '''Main ETL flow to load data into Big Query'''
    print(f'Loading data from year={year}, month={month}, color={color}')
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)

@flow()
def gcs_to_bq_main_flow(year: int =2021,
                    months: list[int] = [1,2],                     
                    color: str = 'yellow'):
    for month in months:
        etl_gcs_to_bq(year, month, color)

if __name__ == '__main__':
    gcs_to_bq_main_flow()