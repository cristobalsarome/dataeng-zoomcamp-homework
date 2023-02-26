import io
import os
import requests
import re
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = ('https://github.com/DataTalksClub/nyc-tlc-data'
            '/releases/download/{service}/{service}_'
            'tripdata_{year}-{month}.csv.gz')

# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):

        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # download csv  
        request_url = init_url.format(service=service, year=year, month=month)
        file_name = re.search('(?<=/)[^/]*$', request_url)[0] # csv file_name 
        os.system(f"wget {request_url} -O {file_name}")
        print(f"csv: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")
        
        # remove file
        os.remove(file_name)

#main block
if __name__ == "__main__":
    web_to_gcs('2019', 'green')
    web_to_gcs('2020', 'green')
    web_to_gcs('2019', 'yellow')
    web_to_gcs('2020', 'yellow')
    # web_to_gcs('2019', 'fhv')
