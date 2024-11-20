import os

class AppConfig:
    BUCKET_FILES = ('currencies_history.csv', 'transactions_batch')

    def __init__(self) -> None:
        
        self.s3_conn_id = str(os.getenv('S3_CONN_ID') or "S3_CONNECTION")
        self.vertica_con_id = str(os.getenv('VERTICA_CONN_ID') or "VERTICA_WAREHOUSE_CONNECTION")