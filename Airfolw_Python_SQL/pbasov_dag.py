from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.operators.python_operator import PythonOperator 
from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task


connection = {'host': 'https://clickhouse.lab.karpov.courses',
'database':'simulator_20250420',
'user':'student',
'password':'dpo_python_2020'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student-rw',
    'password': '656e2b0c9c',
    'database': 'test'
}

default_args = {
    'owner': 'p-basov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 27),
}

schedule_interval = '0 0 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def pbasov_dag():
   
    @task()
    def extract_feed():
        feed_q = '''
        SELECT toDate(time) as event_date, user_id, age, gender, os,
        countIf(action = 'view') as views,
        countIf(action = 'like') as likes    
        FROM simulator_20250420.feed_actions 
        WHERE toDate(time) = yesterday()
        GROUP BY event_date, user_id, age, gender, os
        '''
        feed = ph.read_clickhouse(feed_q, connection=connection)
        return feed

    @task()
    def extract_messages():
        messages_q = '''
        SELECT event_date, user_id, age, gender, os, messages_sent, users_sent, messages_received, users_received 
        FROM (
            SELECT toDate(time) as event_date, user_id, age, gender, os, COUNT(receiver_id) as messages_sent, COUNT(DISTINCT receiver_id) as users_sent
            FROM simulator_20250420.message_actions
            GROUP BY event_date, user_id, age, gender, os
        ) as t1
        FULL JOIN (
            SELECT toDate(time) as event_date, receiver_id, count(user_id) as messages_received, count(distinct user_id) as users_received
            FROM simulator_20250420.message_actions 
            GROUP BY event_date, receiver_id
        ) as t2
        ON t1.user_id = t2.receiver_id AND t1.event_date = t2.event_date
        WHERE event_date = yesterday()
        '''
        messages = ph.read_clickhouse(messages_q, connection=connection)
        return messages 

    @task()
    def merge_data(feed, messages):
        full_table = feed.merge(messages, on = ['event_date', 'user_id', 'age', 'gender', 'os'] , how = 'outer').fillna(0)
        return full_table
    
    @task()
    def os_metrics(full_table):
        os = full_table[['event_date', 'os', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                       .groupby(['event_date', 'os'], as_index=False).sum().rename(columns={'os': 'dimension_value'})
        os.insert(1, 'dimension', 'os')
        return os
    
    @task()
    def gender_metrics(full_table):
        gender = full_table[['event_date', 'gender', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                           .groupby(['event_date', 'gender'], as_index=False).sum().rename(columns={'gender': 'dimension_value'})
        gender.insert(1, 'dimension', 'gender')
        return gender
    
    @task()
    def age_metrics(full_table):
        age = full_table[['event_date', 'age', 'views', 'likes', 'messages_received','messages_sent', 'users_received', 'users_sent']]\
                        .groupby(['event_date', 'age'], as_index=False).sum().rename(columns={'age': 'dimension_value'})
        age.insert(1, 'dimension', 'age')
        return age
    
    @task()
    def load_to_clickhouse(os, gender, age):
        final_df = pd.concat([os, gender, age])

        final_df = final_df.astype({
            'views': 'int',
            'likes': 'int',
            'messages_sent': 'int',
            'messages_received': 'int',
            'users_sent': 'int',
            'users_received': 'int'
        })

        create_table_query = """
        CREATE TABLE IF NOT EXISTS test.pbasov (
            event_date Date,
            dimension String,
            dimension_value String,
            views Int64,
            likes Int64,
            messages_sent Int64,
            messages_received Int64,
            users_sent Int64,
            users_received Int64
        ) ENGINE = MergeTree()
        ORDER BY (event_date, dimension, dimension_value)
        """
        ph.execute(create_table_query, connection=connection_test)

        ph.to_clickhouse(final_df, table='pbasov', index=False, connection=connection_test)

    
    feed = extract_feed()
    messages = extract_messages()
    merged = merge_data(feed, messages)
    os = os_metrics(merged)
    gender = gender_metrics(merged)
    age = age_metrics(merged)
    load_to_clickhouse(os, gender, age)

pbasov_dag = pbasov_dag()







