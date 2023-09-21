from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from pandas import json_normalize
import json

def _process_data(ti):
    b_coin = ti.xcom_pull(task_ids="extract_data")
    #b_coin = b_coin['results'][0]
    processed_user = json_normalize({
        'usd': float(b_coin['bpi']['USD']['rate'].replace(',', '')),
        'time': b_coin['time']['updatedISO']})
    processed_user.to_csv('/tmp/processed_data.csv', index=None, header=False)

def _store_data():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY bitcoin_rate FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_data.csv'
    )

with DAG('bitcoin_processing', start_date=datetime(2023, 1, 1),
         schedule='*/1 * * * *', catchup=False):

        create_table = PostgresOperator(
            task_id = 'create_table',
            postgres_conn_id = 'postgres',
            sql = '''
                CREATE TABLE IF NOT EXISTS bitcoin_rate (
                    b_rate float not null,
                    dttm timestamp not null
                );
            '''
        )
         
        extract_data = SimpleHttpOperator(
            task_id = 'extract_data',
            http_conn_id = 'coindesk',
            endpoint = '/v1/bpi/currentprice.json',
            method = 'GET',
            response_filter = lambda response: json.loads(response.text),
            log_response = True,
            extra_options = {'verify':False}
        )

        process_data = PythonOperator(
            task_id='process_data',
            python_callable=_process_data
        )

        store_data = PythonOperator(
            task_id='store_data',
            python_callable=_store_data
        )

        create_table >> extract_data >> process_data >> store_data