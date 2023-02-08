from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import random

@task(retries=5)
def fetch(dataset_url:str) -> pd.DataFrame:
    '''Read data from web to pandas'''
    # if random() < 0.7:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    '''Fix datatype issues'''
    # convert the corresponding fields to date_time
    date_time_fields = ['lpep_pickup_datetime','lpep_dropoff_datetime', #for green taxi
                        'tpep_pickup_datetime', 'tpep_dropoff_datetime' ] # for yellow taxi
    for dt_field in date_time_fields:
        if dt_field in df.columns:
            df[dt_field] = pd.to_datetime(df[dt_field])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df
    
@task()
def write_local(df:pd.DataFrame, color: str, dataset_file: str) -> Path:
    '''Write DataFrame out locally as parquet file'''
    path = Path(f'../data/{color}/{dataset_file}.parquet')
    # if the folder doesnt exists, I create it
    if not path.parent.exists():
        path.parent.mkdir(parents=True)
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path: Path, color: str, dataset_file: str) -> None:
    '''Upload local parquet file to GCS'''
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.upload_from_path(
        from_path = path,
        to_path = f'data/{color}/{dataset_file}.parquet'
    )

@flow()
def etl_web_to_gcs() -> None:
    '''Main ETL function'''
    color = 'green'
    year = 2020
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = ('https://github.com/DataTalksClub/nyc-tlc-data/'
                  f'releases/download/{color}/{dataset_file}.csv.gz')

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path, color, dataset_file)


if __name__ == '__main__':
    etl_web_to_gcs()