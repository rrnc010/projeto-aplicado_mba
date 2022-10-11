import os, time
import pandas as pd
#import dataframe_image as di
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import bigquery_datatransfer_v1
from google.cloud import bigquery_datatransfer
#from google.protobuf.timestamp_pb2 import Timestamp
#from Mod_key import key

"""
autor: Rafael Cardoso
Projeto: P.A
"""


class conect_bigquery:
    """
    ##define proxy
    mod = key()
    mod.key_path_security()
    proxy = os.environ['prox']
    os.environ['http_proxy'] = proxy
    os.environ['HTTP_PROXY'] = proxy
    os.environ['https_proxy'] = proxy
    os.environ['HTTPS_PROXY'] = proxy
    """
    def __init__(self):
        ##cria autenticacao com api google
        self.path_json = "service_acount_pa.json" #os.environ['path_arq_api_bigquery']
        self.credentials = service_account.Credentials.from_service_account_file(self.path_json)
        self.project_id = "projeto-aplicado-ed"  #os.environ['project_id_bigquery']
        self.client = bigquery.Client(credentials=self.credentials, project=self.project_id)
        self.transfer_client = bigquery_datatransfer_v1.DataTransferServiceClient(credentials=self.credentials)

    def executaquery_df(self, qry):
        self.query_job = self.client.query(qry)
        self.results = self.query_job.result().to_dataframe()  # Wait for the job to complete.
        self.resultss = pd.DataFrame(self.results)

        return self.results

    def upload_cs(self, arq_origem, arq_destino, bucket, subpasta):
        self.path_to_file = arq_origem
        self.storage_client = storage.Client.from_service_account_json(self.path_json)
        self.bucket = self.storage_client.get_bucket(bucket)
        self.blob = self.bucket.blob(subpasta + arq_destino)
        self.blob.upload_from_filename(self.path_to_file)
        return self.blob.public_url

    """ 
    def executa_job_bigquery(self, transfer_id):
        self.transfer_config_id = transfer_id
        self.parent = self.transfer_client.transfer_config_path(self.project_id, self.transfer_config_id)
        self.start_time = Timestamp(seconds=int(time.time()))
        self.request = bigquery_datatransfer_v1.types.StartManualTransferRunsRequest({"parent": self.parent, "requested_run_time": self.start_time})
        self.response = self.transfer_client.start_manual_transfer_runs(self.request)
    """
    def status_job_bigquery(self, transfer_id):
        self.config_id = transfer_id
        self.parent = self.transfer_client.common_project_path(self.project_id)
        self.configs = self.transfer_client.list_transfer_configs(parent=self.parent)
        for config in self.configs:
           # status= print(config.name)
           if config.name=='projects/821342922194/locations/us/transferConfigs/'+self.config_id:
              status=config.state
        return str(status)


    def delete_objeto(self, bucket_name, objeto):
        """Deletes a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"
        self.storage_client = storage.Client.from_service_account_json(self.path_json)
        self.bucket =  self.storage_client.bucket(bucket_name)
        self.blob = self.bucket.blob(objeto)
        self.blob.delete()

        print(f"Blob {objeto} deleted.")

    def list_blobs(self,bucket_name):
        """Lists all the blobs in the bucket."""
        # bucket_name = "your-bucket-name"

        self.storage_client = storage.Client.from_service_account_json(self.path_json)

        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = self.storage_client.list_blobs(bucket_name)

        for blob in blobs:
            self.lista= blob
        return [self.lista]

    def delete_all_obj(self, bucket_name,directory_name):

        self.storage_client = storage.Client.from_service_account_json(self.path_json)
        self.bucket = self.storage_client.get_bucket(bucket_name)
        self.blobs = self.bucket.list_blobs(prefix=directory_name)
        for self.blob in self.blobs:
            self.blob.delete()

    def create_bucket_class_location(self, bucket_name):
        """
        Create a new bucket in the US region with the coldline storage
        class
        """
        # bucket_name = "your-new-bucket-name"

        self.storage_client = storage.Client.from_service_account_json(self.path_json)

        bucket = self.storage_client.bucket(bucket_name)
        bucket.storage_class = "COLDLINE"
        self.new_bucket = self.storage_client.create_bucket(bucket, location="us")

        print(
            "Created bucket {} in {} with storage class {}".format(
                self.new_bucket.name, self.new_bucket.location, self.new_bucket.storage_class
            )
        )
        return self.new_bucket

        # Construct a BigQuery client object.

    def cria_dataset(self,    dataset_id):
        try:
            dataset_ref = bigquery.DatasetReference.from_string(dataset_id, default_project=self.client.project)
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = 'US'
            dataset = self.client.create_dataset(dataset)
            print("dataset criado!")
        except Exception as e:
            print("dataset já existe!",e)