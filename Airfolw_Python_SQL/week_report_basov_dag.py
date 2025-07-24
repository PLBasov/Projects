from datetime import datetime, timedelta
import io
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph

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
    'start_date': datetime(2025, 5, 29),
}

schedule_interval = '0 11 * * *'

token = '8138277020:AAFA-pPVKcfV2Yti3VTieRselt_3lLdoozQ'
bot = telegram.Bot(token=token)
chat_id = '-1002614297220' 


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def week_report_basov_dag():
    
    @task()
    def extract_df():
        query = '''
                SELECT toDate(time) as day,
                        count(DISTINCT user_id) as dau,
                        sum(action = 'view') as views,
                        sum(action = 'like') as likes,
                        likes/views as ctr
                FROM {db}.feed_actions 
                WHERE toDate(time) BETWEEN today()-7 and  today()-1
                GROUP BY day
                '''
        df = ph.read_clickhouse(query, connection=connection)
        return df
    
    @task()
    def text_report(df):
        yesterday_row = df.iloc[-1]  # последняя строка — вчера
        report_text = f"""*Ежедневный отчет по ленте новостей* (за {yesterday_row['day']}):
DAU: {int(yesterday_row['dau'])}
Просмотры: {int(yesterday_row['views'])}
Лайки: {int(yesterday_row['likes'])}
CTR: {float(yesterday_row['ctr']):.4f}
"""
        return report_text
    
    @task()
    def plot_metrics(df):
        plt.figure(figsize=(10, 6))
        sns.set(style="whitegrid")
        days = df['day'].astype(str).tolist()

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        axes[0, 0].plot(days, df['dau'], marker='o', color='blue')
        axes[0, 0].set_title('DAU')

        axes[0, 1].plot(days, df['views'], marker='o', color='green')
        axes[0, 1].set_title('Просмотры')

        axes[1, 0].plot(days, df['likes'], marker='o', color='orange')
        axes[1, 0].set_title('Лайки')

        axes[1, 1].plot(days, df['ctr'], marker='o', color='purple')
        axes[1, 1].set_title('CTR')

        plt.tight_layout()

        buf_chart = io.BytesIO()
        plt.savefig(buf_chart, format='png')
        plt.close()
        buf_chart.seek(0)
        return buf_chart.getvalue()

    @task()
    def send_telegram_msg(text, image_bytes):
        bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
        bot.send_photo(chat_id=chat_id, photo=image_bytes)
       
        
    df = extract_df()
    text_report = text_report(df)
    plot_metrics = plot_metrics(df)
    send_telegram_msg(text_report, plot_metrics)
    
week_report_basov_dag = week_report_basov_dag()