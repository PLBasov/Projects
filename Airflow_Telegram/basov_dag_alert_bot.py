import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

from airflow.decorators import dag, task


connection = {'host': 'https://clickhouse.lab.karpov.courses',
'database':'simulator_20250420',
'user':'student',
'password':'dpo_python_2020'
}

default_args = {
    'owner': 'p-basov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 15),
}

schedule_interval = '*/15 * * * *'

token = '8138277020:AAFA-pPVKcfV2Yti3VTieRselt_3lLdoozQ'
bot = telegram.Bot(token=token)
chat_id = '-1002614297220'


def check_anomaly(df, metric, a = 6, n = 4):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25) 
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a * df['iqr'] 
    df['low'] = df['q25'] - a * df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def basov_dag_alert_bot():

    @task
    def extract_data():
        query = """
            WITH messages AS (
            SELECT 
                uniqExact(user_id) AS users_message,
                toStartOfFifteenMinutes(time) as ts,
                count(*) AS messages_sent
            FROM simulator_20250420.message_actions
            WHERE ts >=  DATE(now()) - 1 and ts < toStartOfFifteenMinutes(toDateTime(now()))
            GROUP BY ts
            ORDER BY ts
            ),
            feeds AS (
                SELECT 
                    uniqExact(user_id) AS users_feed,
                    toStartOfFifteenMinutes(time) as ts,
                    sum(action = 'view') as views,
                    sum(action = 'like') as likes,
                    round(100*likes / views, 2) as ctr
                FROM simulator_20250420.feed_actions 
                WHERE ts >=  DATE(now()) - 1 and ts < toStartOfFifteenMinutes(toDateTime(now()))
                GROUP BY ts
                ORDER BY ts
            )
            SELECT ts,
                users_feed,
                toDate(ts) as date,
                formatDateTime(ts, '%R') as hm,
                m.users_message,
                views,
                likes,
                ctr,
                m.messages_sent
            FROM feeds
                LEFT JOIN messages m 
                USING (ts)
            """
        
        df = ph.read_clickhouse(query, connection=connection)
        return df


    @task
    def check_data(df):  
        metrics_list = ['users_feed', 'users_message', 'views', 'likes', 'ctr', 'messages_sent']

        for metric in metrics_list:
            current_df = df[['ts', 'date', 'hm', metric]].copy() 
            is_alert, current_df = check_anomaly(current_df, metric)

            if is_alert == 1:
                msg = f"""
Обнаружена аномалия в интервале: {current_df['hm'].iloc[-2]}—{current_df['hm'].iloc[-1]}
Метрика: {metric}  
Текущее значение: {current_df[metric].iloc[-1]} 
Предыдущее значение: {current_df[metric].iloc[-2]}
Отклонение: {round(100 * (1 - (current_df[metric].iloc[-1]/current_df[metric].iloc[-2])), 2)}% 
                """

                plt.figure(figsize=(10, 6))
                sns.lineplot(x=current_df['ts'], y=current_df[metric], label='metric')
                sns.lineplot(x=current_df['ts'], y=current_df['up'], linestyle='--', linewidth=1, label='up')
                sns.lineplot(x=current_df['ts'], y=current_df['low'], linestyle='--', linewidth=1, color='salmon', label='low')

                plt.title(metric.replace('_', ' '), fontsize=12, fontweight='bold')
                plt.grid(True)
                plt.xlabel("Время")
                plt.ylabel(metric)
                plt.tight_layout()

                buf_chart = io.BytesIO()
                plt.savefig(buf_chart, dpi=150)
                buf_chart.seek(0)
                plt.close()

                bot.send_message(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=buf_chart)
    

    data = extract_data()
    check_data(data)

basov_dag_alert_bot = basov_dag_alert_bot()