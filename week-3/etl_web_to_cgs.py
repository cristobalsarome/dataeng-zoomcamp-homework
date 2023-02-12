from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import random
import re
import os

@task(retries=5)
def fetch( month:int = 1,
           year:int = 2019) -> str:
    url = ('https://github.com/DataTalksClub/nyc-tlc-data/releases/'
          f'download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz')
    csv_name = re.search('(?<=/)[^/]*$', url)[0]
    os.system(f"wget {url} -O {csv_name}")
    return csv_name


@task(retries=3)
def write_gcs(path: 'str') -> None:
    '''Upload local parquet file to GCS'''
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.upload_from_path(
        from_path = path,
        to_path = f'data/fhv/{path}'
    )
    os.remove(path)


@flow()
def etl_web_to_gcs() -> None:
    '''Main ETL function'''
    year = 2019
    for month in range(1,13):
        local_file = fetch(month = month,
                           year = year)
        write_gcs(local_file)


if __name__ == '__main__':
    etl_web_to_gcs()