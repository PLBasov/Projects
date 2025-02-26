import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# Не понятно зачем тут эта функция, но просто запишем данные в файл, 
# и не будем с ним ни чего делать дальше, так как дальше во всех функциях мы открываем всё заного. 
def get_data_pb():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_pb():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_domains_10 = top_data_df.groupby('zone', as_index=False).agg({'domain': 'nunique'}).sort_values(by='domain', ascending=False).head(10)
    with open('top_domains_10.csv', 'w') as f:
        f.write(top_domains_10.to_csv(index=False, header=False))

def get_max_len_pb():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_length = top_data_df['domain'].str.len().max()
    domains_with_max_length = top_data_df[top_data_df['domain'].str.len() == max_length]
    with open('domains_with_max_length.csv', 'w') as f:
        f.write(domains_with_max_length.to_csv(index=False, header=False))
        
def get_airflow_rank_pb():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_rank = top_data_df[top_data_df['domain'].str.contains('airflow.com')]
    with open('domain_rank.csv', 'w') as f:
        f.write(domain_rank.to_csv(index=False, header=False))
        
def print_data_pb(ds):
    with open('top_domains_10.csv', 'r') as f:
        top_data_10 = f.read()
    with open('domains_with_max_length.csv', 'r') as f:
        max_len = f.read()
    with open('domain_rank.csv', 'r') as f:
        airflow_rank = f.read() 
    date = ds

    print(f'Top domains for date {date}')
    print(top_data_10)

    print(f'Domain with max lenght for date {date}')
    print(max_len)
    
    print(f'Airflow rank for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'pavel-basov-rge5759',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 21)
}
schedule_interval = '0 12 */1 * *'

dag = DAG('basov_first_dag', default_args=default_args, schedule_interval=schedule_interval)


t1 = PythonOperator(task_id='get_data_pb',
                    python_callable=get_data_pb,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_pb',
                    python_callable=get_top_pb,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_len_pb',
                    python_callable=get_max_len_pb,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank_pb',
                        python_callable=get_airflow_rank_pb,
                        dag=dag)

t5 = PythonOperator(task_id='print_data_pb',
                        python_callable=print_data_pb,
                        dag=dag)

# так как всех функциях файл открывается заного, то первые 4 таски можно запустить сразу.
[t1, t2, t3, t4] >> t5
