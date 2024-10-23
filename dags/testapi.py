import http.client
import json
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
import datetime

stored_data = {
    "1m": {
        "RSI": 37.23,
        "MACD": -31.316299984049692,
        "MACD_signal": -26.00492282969696,
        "MACD_histogram": -5.3113771543527335,
        "KDJ_K": 26.8081761006284,
        "KDJ_D": 22.83434186733486,
        "KDJ_J": 34.75584456721548,
        "MA": 61691.557142857135,
        "Bollinger_middle": 61715.53500000002,
        "Bollinger_upper": 61809.35357545286,
        "Bollinger_lower": 61621.71642454718,
        "VWAP": 61590.758884490395
    },
    "3m": {
        "RSI": 42.33,
        "MACD": -6.544702069993946,
        "MACD_signal": 12.640273565581458,
        "MACD_histogram": -19.184975635575405,
        "KDJ_K": 13.09523809523783,
        "KDJ_D": 8.375537542408617,
        "KDJ_J": 22.534639200896255,
        "MA": 61767.95714285716,
        "Bollinger_middle": 61749.565,
        "Bollinger_upper": 61888.11298843722,
        "Bollinger_lower": 61611.017011562784,
        "VWAP": 61952.45180297728
    },
    "5m": {
        "RSI": 46.54,
        "MACD": 17.37823601019045,
        "MACD_signal": 31.9710845414895,
        "MACD_histogram": -14.592848531299051,
        "KDJ_K": 25.428194993413943,
        "KDJ_D": 31.56565656565756,
        "KDJ_J": 13.153271848926714,
        "MA": 61722.942857142865,
        "Bollinger_middle": 61737.130000000005,
        "Bollinger_upper": 61895.471088792525,
        "Bollinger_lower": 61578.788911207484,
        "VWAP": 62319.28809971409
    },
    "15m": {
        "RSI": 52.09,
        "MACD": 69.96511162054958,
        "MACD_signal": 69.41113699323179,
        "MACD_histogram": 0.5539746273177855,
        "KDJ_K": 55.97473824164941,
        "KDJ_D": 71.16503240817745,
        "KDJ_J": 25.594149908593323,
        "MA": 61672.94285714285,
        "Bollinger_middle": 61619.64999999999,
        "Bollinger_upper": 61859.607417055595,
        "Bollinger_lower": 61379.69258294438,
        "VWAP": 63777.68456010551
    },
    "30m": {
        "RSI": 49.73,
        "MACD": -37.3275365489535,
        "MACD_signal": -114.33350511733161,
        "MACD_histogram": 77.00596856837811,
        "KDJ_K": 55.9913578195119,
        "KDJ_D": 64.10697512603222,
        "KDJ_J": 39.760123206471235,
        "MA": 61636.49285714287,
        "Bollinger_middle": 61506.19999999999,
        "Bollinger_upper": 62055.63613277613,
        "Bollinger_lower": 60956.763867223846,
        "VWAP": 63795.714088780005
    },
    "1h": {
        "RSI": 41.66,
        "MACD": -398.7127953879826,
        "MACD_signal": -519.3109349945482,
        "MACD_histogram": 120.59813960656561,
        "KDJ_K": 85.27661940506002,
        "KDJ_D": 79.4143189674602,
        "KDJ_J": 97.00122028025964,
        "MA": 61367.78571428572,
        "Bollinger_middle": 61631.69999999999,
        "Bollinger_upper": 62720.11570734714,
        "Bollinger_lower": 60543.28429265284,
        "VWAP": 61883.11410418162
    },
    "4h": {
        "RSI": 32.62,
        "MACD": -877.1501126457588,
        "MACD_signal": -596.4602250016034,
        "MACD_histogram": -280.6898876441554,
        "KDJ_K": 33.65000658212303,
        "KDJ_D": 29.761547703797362,
        "KDJ_J": 41.426924338774356,
        "MA": 62877.700000000004,
        "Bollinger_middle": 63657.469999999994,
        "Bollinger_upper": 66716.22924917277,
        "Bollinger_lower": 60598.71075082722,
        "VWAP": 60705.68390992447
    },
    "8h": {
        "RSI": 38.4,
        "MACD": -376.09010153469717,
        "MACD_signal": 189.24440412775448,
        "MACD_histogram": -565.3345056624516,
        "KDJ_K": 25.106403876375126,
        "KDJ_D": 20.251599851758126,
        "KDJ_J": 34.816011925609125,
        "MA": 64006.22142857143,
        "Bollinger_middle": 64370.61000000002,
        "Bollinger_upper": 67521.4106067665,
        "Bollinger_lower": 61219.80939323355,
        "VWAP": 62117.44987447165
    },
    "1d": {
        "RSI": 48.95,
        "MACD": 1001.3080307461496,
        "MACD_signal": 1253.6734847546672,
        "MACD_histogram": -252.36545400851764,
        "KDJ_K": 24.260495428517252,
        "KDJ_D": 37.075810075271846,
        "KDJ_J": -1.3701338649919421,
        "MA": 63690.8857142857,
        "Bollinger_middle": 62569.82000000003,
        "Bollinger_upper": 66928.2440535313,
        "Bollinger_lower": 58211.39594646875,
        "VWAP": 45929.202498349725
    },
    "3d": {
        "RSI": 50.33,
        "MACD": 272.92146104080166,
        "MACD_signal": -437.06575744057744,
        "MACD_histogram": 709.9872184813792,
        "KDJ_K": 65.6501090449954,
        "KDJ_D": 84.78362942289387,
        "KDJ_J": 27.383068289198462,
        "MA": 60649.47142857136,
        "Bollinger_middle": 60111.849999999955,
        "Bollinger_upper": 66042.97541614149,
        "Bollinger_lower": 54180.72458385843,
        "VWAP": 34859.28106261362
    },
    "1w": {
        "RSI": 51.49,
        "MACD": 1068.43565118908,
        "MACD_signal": 1609.5266936990195,
        "KDJ_K": 60.2741471240504,
        "KDJ_D": 69.40090281382221,
        "MA": 61026.057142857135,
        "Bollinger_middle": 62648.830000000075,
        "Bollinger_upper": 71672.92507997347,
        "Bollinger_lower": 53624.734920026676,
        "VWAP": 31990.67717754113
    }
}

# Function to extract data
def extract_data():
    conn = http.client.HTTPSConnection("crypto-price-technical-indicators-signals.p.rapidapi.com")
    headers = {
        'x-rapidapi-key': "c3b25e479cmsh013bc29bd085c76p19215bjsn65f14579e7db",
        'x-rapidapi-host': "crypto-price-technical-indicators-signals.p.rapidapi.com"
    }
    conn.request("GET", "/indicators/BTCUSDT", headers=headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data)

# Function to transform data
def transform_data(data):
    all_normalized_data = {}
    for timeframe, indicators in data.items():
        rsi = indicators['RSI']
        macd = indicators['MACD']['MACD']
        macd_signal = indicators['MACD']['signal']
        macd_histogram = indicators['MACD']['histogram']
        kdj_k = indicators['KDJ']['K']
        kdj_d = indicators['KDJ']['D']
        kdj_j = indicators['KDJ']['J']
        ma = indicators['MA']
        bollinger_middle = indicators['BollingerBands']['middle']
        bollinger_upper = indicators['BollingerBands']['upper']
        bollinger_lower = indicators['BollingerBands']['lower']
        vwaps = indicators['VWAP']

        all_normalized_data[timeframe] = {
            'RSI': rsi,
            'MACD': macd,
            'MACD_signal': macd_signal,
            'MACD_histogram': macd_histogram,
            'KDJ_K': kdj_k,
            'KDJ_D': kdj_d,
            'KDJ_J': kdj_j,
            'MA': ma,
            'Bollinger_middle': bollinger_middle,
            'Bollinger_upper': bollinger_upper,
            'Bollinger_lower': bollinger_lower,
            'VWAP': vwaps
        }
    return all_normalized_data

# Function to load data into PostgreSQL

def insert_crypto_data_into_postgres(ti):
    # Pull transformed crypto data from XCom
    crypto_data=stored_data #for testing purpose
    #crypto_data = ti.xcom_pull(key='crypto_data', task_ids='transform_task')
    if not crypto_data:
        raise ValueError("No crypto data found")

    # Initialize Postgres connection using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='crypto_connection')

    # Define the SQL insert query
    insert_query = """
    INSERT INTO crypto_indicators (timeframe, rsi, macd, macd_signal, macd_histogram, kdj_k, kdj_d, kdj_j, ma, 
                                   bollinger_middle, bollinger_upper, bollinger_lower, vwap)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Loop over each row in the crypto data and insert into PostgreSQL
    for index, row in crypto_data.iterrows():
        postgres_hook.run(insert_query, parameters=(
            row['timeframe'], row['RSI'], row['MACD'], row['MACD_signal'], row['MACD_histogram'], 
            row['KDJ_K'], row['KDJ_D'], row['KDJ_J'], row['MA'], 
            row['Bollinger_middle'], row['Bollinger_upper'], row['Bollinger_lower'], row['VWAP']
        ))

# Define your DAG
with DAG(
    'crypto_data_pipeline',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 10, 1),
        'retries': 1,
    },
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define your tasks
    load_crypto_data_task = PythonOperator(
        task_id='insert_crypto_data_into_postgres',
        python_callable=insert_crypto_data_into_postgres,
        provide_context=True
    )

# Set task dependencies
load_crypto_data_task
