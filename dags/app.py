from datetime import datetime, timedelta
from airflow import DAG
import petl as etl
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define the API endpoint
url = "https://api.covidtracking.com/v1/states/current.json"

# Extract the data directly from the URL
def fetch_covid_data(ti):
    table = etl.fromjson(url)
    print("Data pushed extracted")
    data = list(table)
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    selected_columns = ['date', 'state', 'positive', 'probableCases', 'negative', 'pending',
                        'totalTestResults', 'hospitalizedCurrently', 'hospitalizedCumulative',
                        'recovered', 'death', 'deathConfirmed', 'deathProbable', 'hospitalized',
                        'hospitalizedDischarged', 'total', 'totalTestResultsIncrease', 'score', 'grade']
    df = df[selected_columns]
    ti.xcom_push(key='covid_data', value=df.to_dict('records'))
    print("Data pushed to XCom")

def insert_covid_data_into_postgres(ti):
    covid_data = ti.xcom_pull(key='covid_data', task_ids='fetch_covid_data')
    if not covid_data:
        raise ValueError("No COVID data found")

    postgres_hook = PostgresHook(postgres_conn_id='covid_data_connection')
    insert_query = """
    INSERT INTO covid_data (date, state, positive, probableCases, negative, pending, totalTestResults,
                            hospitalizedCurrently, hospitalizedCumulative, recovered, death, deathConfirmed,
                            deathProbable, hospitalized, hospitalizedDischarged, total, totalTestResultsIncrease,
                            score, grade)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for record in covid_data:
        postgres_hook.run(insert_query, parameters=(record['date'], record['state'], record['positive'], record['probableCases'],
                                                    record['negative'], record['pending'], record['totalTestResults'],
                                                    record['hospitalizedCurrently'], record['hospitalizedCumulative'],
                                                    record['recovered'], record['death'], record['deathConfirmed'],
                                                    record['deathProbable'], record['hospitalized'], record['hospitalizedDischarged'],
                                                    record['total'], record['totalTestResultsIncrease'], record['score'], record['grade']))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_covid_data',
    default_args=default_args,
    description='A simple DAG to fetch COVID-19 data and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

fetch_covid_data_task = PythonOperator(
    task_id='fetch_covid_data',
    python_callable=fetch_covid_data,
    dag=dag,
)

create_covid_table_task = PostgresOperator(
    task_id='create_covid_table',
    postgres_conn_id='covid_data_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS covid_data (
        id SERIAL PRIMARY KEY,
        date TEXT,
        state TEXT,
        positive INTEGER,
        probableCases INTEGER,
        negative INTEGER,
        pending INTEGER,
        totalTestResults INTEGER,
        hospitalizedCurrently INTEGER,
        hospitalizedCumulative INTEGER,
        recovered INTEGER,
        death INTEGER,
        deathConfirmed INTEGER,
        deathProbable INTEGER,
        hospitalized INTEGER,
        hospitalizedDischarged INTEGER,
        total INTEGER,
        totalTestResultsIncrease INTEGER,
        score INTEGER,
        grade TEXT
    );
    """,
    dag=dag,
)

insert_covid_data_task = PythonOperator(
    task_id='insert_covid_data',
    python_callable=insert_covid_data_into_postgres,
    dag=dag,
)

fetch_covid_data_task >> create_covid_table_task >> insert_covid_data_task
