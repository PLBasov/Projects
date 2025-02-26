import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
import requests
from io import StringIO
import json

default_args = {
    'owner': 'pavel-basov-rge5759',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 25),
    'schedule_interval': '0 12 */1 * *'
}


@dag(default_args=default_args, catchup=False)
def basov_second_dag():
    
    @task()
    def get_data():
        link =  requests.get("https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv", stream=True)
        data = pd.read_csv(StringIO(link.text))
        data.columns = data.columns.str.lower()
        login = 'pavel-basov-rge5759'
        year = 1994 + hash(f'{login}') % 23
        data_year = data.query('year == @year').dropna().to_json(orient="records")
        return {'data_year': data_year, 'year': year}
    
    @task()
    def get_top_game(data_dict):
        data_year = pd.DataFrame(json.loads(data_dict['data_year']))
        top_game = data_year.groupby('name', as_index=False)\
                            .agg({'global_sales': 'sum'})\
                            .sort_values('global_sales', ascending=False).head(1).name.iloc[0]
        return top_game
        
    @task()
    def get_top_genre_eu(data_dict):
        data_year = pd.DataFrame(json.loads(data_dict['data_year']))
        top_genre_eu = data_year.groupby('genre', as_index=False)\
                                .agg({'eu_sales': 'sum'})\
                                .sort_values('eu_sales', ascending=False).head(3).genre.tolist()  
        return top_genre_eu
    
    
    @task()
    def get_top_platform(data_dict):
        data_year = pd.DataFrame(json.loads(data_dict['data_year']))
        top_platform = data_year.query('na_sales > 1.0')\
                                .groupby('platform', as_index=False)\
                                .agg({'name':'nunique', 'na_sales': 'sum'})\
                                .sort_values('name', ascending=False).platform.iloc[0]
        return top_platform
        
    @task()
    def get_publisher_jp(data_dict):
        data_year = pd.DataFrame(json.loads(data_dict['data_year']))
        publisher_jp = data_year.groupby('publisher', as_index=False)\
                        .agg({'jp_sales': 'mean'})\
                        .sort_values('jp_sales', ascending=False).head(1).publisher.iloc[0]
        return publisher_jp
        
    @task()
    def get_eu_better_jp(data_dict):
        data_year = pd.DataFrame(json.loads(data_dict['data_year']))
        data_year['diff_eu_jp'] = data_year['eu_sales'] - data_year['jp_sales']
        eu_better_jp = data_year.query('diff_eu_jp > 0')\
                                .groupby('name', as_index=False)\
                                .agg({'diff_eu_jp': 'sum'})\
                                .sort_values('diff_eu_jp', ascending=False).shape[0]
        return eu_better_jp
    
    @task()
    def print_data(top_game, top_genre_eu, top_platform, publisher_jp, eu_better_jp, data_dict):
        context = get_current_context()
        date = context['ds']
        year = data_dict['year']

        print(f'{date} \n Global top game for {year}: \n {top_game}')
        print(f'{date} \n Top 3 genres in EU for {year}: \n {top_genre_eu}')
        print(f'{date} \n Top platforms in NA for {year}: \n {top_platform}')
        print(f'{date} \n Top publisher in JP for {year}: \n {publisher_jp}')
        print(f'{date} \n {eu_better_jp} games that sold better in EU than in JP for {year}')
        
        
    data = get_data()

    top_game = get_top_game(data)
    top_genre_eu = get_top_genre_eu(data)
    top_platform = get_top_platform(data)
    publisher_jp = get_publisher_jp(data)
    eu_better_jp = get_eu_better_jp(data)
    
    print_data(top_game, top_genre_eu, top_platform, publisher_jp, eu_better_jp, data)

basov_second_dag = basov_second_dag()