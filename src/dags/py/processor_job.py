from airflow.operators.python import PythonOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task_group
from py.app_config import AppConfig
import os

class ProcessorJob:
    def __init__(self) -> None:
        
        self.config = AppConfig()

    def list_size_s3_bucket(self, bucket: str) -> int:
        s3_client = S3Hook(self.config.s3_conn_id)
        
        return len(s3_client.list_keys(bucket_name=bucket))

    def fetch_s3_file(self, bucket: str, key: str) -> str:
        s3_client = S3Hook(self.config.s3_conn_id)
        file = s3_client.download_file(
            key=key,
            bucket_name=bucket,
            local_path='/data'
        )

        os.rename(src=file, dst=f'/data/{key}')

    def load_s3(self, dag):
        @task_group(group_id='load_s3')
        def load_s3_func():
            for key in self.config.BUCKET_FILES:
                if key == 'transactions_batch':
                    for batch in range(1, self.list_size_s3_bucket('final-project')):
                        PythonOperator(
                            task_id=f'fetch_{key}_{batch}.csv',
                            python_callable=self.fetch_s3_file,
                            op_kwargs={'bucket': 'final-project', 'key': f'{key}_{batch}.csv'},
                            dag=dag
                        )
                else:
                    PythonOperator(
                        task_id=f'fetch_{key}',
                        python_callable=self.fetch_s3_file,
                        op_kwargs={'bucket': 'final-project', 'key': key},
                        dag=dag
                    )

        return load_s3_func

    def create_vertica_task_group(self, group_id, schema, prefix, ddl, items):
        @task_group(group_id=group_id)
        def task_group_func():
            for item in items:
                if item == 'transactions' and ddl == 'create-insert':
                    for batch in range(1, self.list_size_s3_bucket('final-project')):
                        key = f'transactions_batch_{batch}.csv'
                        VerticaOperator(
                            task_id=f'{prefix}_{item}_{batch}',
                            vertica_conn_id=self.config.vertica_con_id,
                            params={'batch': f'/data/{key}'},
                            sql=f'sql/{schema}.{prefix}_{item}-{ddl}.sql'
                        )
                else:
                    VerticaOperator(
                        task_id=f'{prefix}_{item}',
                        vertica_conn_id=self.config.vertica_con_id,
                        sql=f'sql/{schema}.{prefix}_{item}-{ddl}.sql'
                    )

        return task_group_func    