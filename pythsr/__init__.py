from google.cloud import bigquery, storage

"""
project_id = 'projectidtest'
bucket_name = 'bi_poc'
dataset_id = 'dw_data'
destination_table_id = 'nvc_w_pii'
source_file_name = './nvc_w_pii.csv'

# Field types: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema
table_schema = [
    bigquery.SchemaField('account_id', 'STRING'),
]
"""

class BigqueryManagement():

    def __init__(self, project_id, bucket_name=None, dataset_id=None, destination_table_id=None,
                 source_file_name=None, table_schema=None):
        
        self.bq_client = bigquery.Client()
        self.storage_client = storage.Client()

        self.project_id = project_id
        self.bucket_name = bucket_name
        self.dataset_id = dataset_id
        self.destination_table_id = destination_table_id
        self.source_file_name = './' + self.destination_table_id + '.csv' #if source_file_name is not None else self.source_file_name
        self.destination_blob_name = self.dataset_id + '/' + self.destination_table_id + '.csv'

        # Field types: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema
        self.table_schema = table_schema

        self.job_config = bigquery.LoadJobConfig()
        self.job_config.schema = table_schema
        self.job_config.skip_leading_rows = 1
        self.job_config.max_bad_records = 100
        self.job_config.field_delimiter = ","
        self.job_config.source_format = bigquery.SourceFormat.CSV # The source format defaults to CSV, so this line is optional.
        self.job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE # https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#loading_csv_data_with_schema_auto-detection
        self.job_config.allow_quoted_newlines = True



    def bigquery_load_job_config(self, max_bad_records=None):
        print(dir(self.job_config))
        print(self.job_config.max_bad_records)
        if max_bad_records is not None:
            self.job_config.max_bad_records = max_bad_records
        print(self.job_config.max_bad_records)
        
        return self



    def csv_to_gcs(self):
        """
        Uploads a file to the bucket.
        https://cloud.google.com/storage/docs/uploading-objects
        """
        assert 'source_file_name' in self.__dict__
        assert 'destination_blob_name' in self.__dict__
        assert 'storage_client' in self.__dict__

        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.destination_blob_name)

        blob.upload_from_filename(self.source_file_name)

        print('Uploaded file {} to {}.'.format(self.source_file_name, self.destination_blob_name))

        return self




    def gcs_to_bq(self):
        """
        Load CSV from GCS to BQ
        https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
        """
        assert '' in self.__dict__
        assert 'job_config' in self.__dict__
        assert 'destination_blob_name' in self.__dict__
        assert 'bucket_name' in self.__dict__
        assert 'destination_table_id' in self.__dict__
        assert 'dataset_id' in self.__dict__

        dataset_ref = bq_client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.destination_table_id)

        uri = 'gs://' + self.bucket_name + '/' + self.destination_blob_name

        load_job = bq_client.load_table_from_uri(
            uri,
            table_ref,
            job_config=self.job_config)  # API request
        print('Starting job {} from {}'.format(load_job.job_id, uri))

        load_job.result()  # Waits for table load to complete.
        print('Job finished.')

        destination_table = self.bq_client.get_table(dataset_ref.table(self.destination_table_id))
        print('Resulted table has {} rows.'.format(destination_table.num_rows))

        return self




    def csv_to_bq(self):
        csv_to_gcs(self)
        gcs_to_bq(self)

        return self