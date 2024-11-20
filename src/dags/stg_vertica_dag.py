from airflow.operators.empty import EmptyOperator
from airflow import DAG
from py.processor_job import ProcessorJob
import pendulum

with DAG(
        's3_to_stg_vertica',
        schedule_interval=None,
        start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
        catchup=False,
        tags=['final-project', 'stg', 's3'],
        is_paused_upon_creation=True
) as dag:
    
    processor_job = ProcessorJob()
    start = EmptyOperator(task_id='start')
    load_s3 = processor_job.load_s3(dag)
    delete_staging = processor_job.create_vertica_task_group('delete_staging', 'stg', 'd', 'delete', ['currencies', 'transactions'])
    load_staging = processor_job.create_vertica_task_group('load_staging', 'stg', 'd', 'create-insert', ['currencies', 'transactions'])
    end = EmptyOperator(task_id='end')

    (
        start >>
        load_s3() >>
        delete_staging() >>
        load_staging() >>
        end
    )