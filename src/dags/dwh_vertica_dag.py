from airflow.operators.empty import EmptyOperator
from airflow import DAG
from py.processor_job import ProcessorJob
import pendulum

with DAG(
        'stg_to_dwh_vertica',
        schedule_interval='@daily',
        start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
        catchup=True,
        tags=['final-project', 'dwh', 'stg'],
        is_paused_upon_creation=True
) as dag:
    
    processor_job = ProcessorJob()
    start = EmptyOperator(task_id='start')
    load_dwh = processor_job.create_vertica_task_group('load_dwh', 'dwh', 'mart', 'create-insert', ['global_metrics'])
    end = EmptyOperator(task_id='end')

    (
        start >>
        load_dwh() >>
        end
    )