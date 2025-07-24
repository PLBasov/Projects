from datetime import datetime, timedelta
import io
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
import pandas as pd

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

token = token
bot = telegram.Bot(token=token)
chat_id = chat_id


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def big_report_basov_dag():
    
    @task()
    def extract_data():
        data_q = '''
        SELECT t1.event_date as date, messages_sent, users_sent, users_received, feed_users, views, likes, ctr
        FROM (
            SELECT toDate(time) as event_date, COUNT(receiver_id) as messages_sent, count(distinct user_id) as users_sent, COUNT(DISTINCT receiver_id) as users_received
            FROM simulator_20250420.message_actions
            GROUP BY event_date
        ) as t1
        FULL JOIN (
            SELECT toDate(time) as event_date,
            count(distinct user_id) as feed_users,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes,
            likes/views as ctr
            FROM simulator_20250420.feed_actions 
            GROUP BY event_date
        ) as t2
        ON t1.event_date = t2.event_date
        ORDER BY date 
        '''
        data = ph.read_clickhouse(data_q, connection=connection)
        return data

    
    @task()
    def text_report(data):
        yesterday_row = data.iloc[-2].fillna(0)
        day_before_row = data.iloc[-3].fillna(0) if len(data) > 2 else None 

        def calc_diff(current, previous):
            try:
                if previous is None or previous == 0:
                    return "нет данных"
                diff = ((current - previous) / previous) * 100
                return f"{diff:+.1f}%"
            except Exception:
                return "нет данных"

        messages_sent = int(yesterday_row['messages_sent'])
        users_sent = int(yesterday_row['users_sent'])
        users_received = int(yesterday_row['users_received'])
        views = int(yesterday_row['views'])
        likes = int(yesterday_row['likes'])
        ctr = float(yesterday_row['ctr'])
        feed_users = int(yesterday_row['feed_users']) 

        prev_messages_sent = int(day_before_row['messages_sent']) if day_before_row is not None else 0
        prev_users_sent = int(day_before_row['users_sent']) if day_before_row is not None else 0
        prev_users_received = int(day_before_row['users_received']) if day_before_row is not None else 0
        prev_views = int(day_before_row['views']) if day_before_row is not None else 0
        prev_likes = int(day_before_row['likes']) if day_before_row is not None else 0
        prev_ctr = float(day_before_row['ctr']) if day_before_row is not None else 0
        prev_feed_users = int(day_before_row['feed_users']) if day_before_row is not None else 0  

        report_text = f"""
*Ежедневный отчет* (за {yesterday_row['date'].date()}):
DAU: {feed_users} ({calc_diff(feed_users, prev_feed_users)} д/д)
Отправлено сообщений: {messages_sent} ({calc_diff(messages_sent, prev_messages_sent)} д/д)
Отправители: {users_sent} ({calc_diff(users_sent, prev_users_sent)} д/д)
Получатели: {users_received} ({calc_diff(users_received, prev_users_received)} д/д)
Просмотры: {views} ({calc_diff(views, prev_views)} д/д)
Лайки: {likes} ({calc_diff(likes, prev_likes)} д/д)
CTR: {ctr:.2%} ({calc_diff(ctr, prev_ctr)} д/д)
    """
        return report_text.strip()
    
    @task()
    def file_report(data):
        file_object = io.BytesIO()
        data.to_csv(file_object, index=False)
        file_object.name = 'data.csv'
        file_object.seek(0)
        return file_object
         
    
    @task()
    def extract_df():
        query = '''
                SELECT toMonday(toDateTime(this_week)) AS date, status AS status, AVG(num_users) AS "AVG(num_users)"
                FROM
                  (SELECT previous_week, this_week, status, uniq(user_id)*-1 as num_users
                FROM
                  (SELECT user_id,
                          groupUniqArray(toMonday(toDate(time))) as weeks_visited,
                          addWeeks(this_week, -1) as previous_week,
                          addWeeks(arrayJoin(weeks_visited), +1) as this_week,
                          if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status
                FROM simulator_20250420.feed_actions
                GROUP BY user_id
                HAVING status = 'gone')
                GROUP BY status, this_week, previous_week
                UNION ALL 
                SELECT previous_week,this_week, status, toInt64(uniq(user_id)) as num_users
                FROM
                 (SELECT user_id,
                         groupUniqArray(toMonday(toDate(time))) as weeks_visited,
                         addWeeks(this_week, -1) as previous_week,
                         arrayJoin(weeks_visited) as this_week,
                         if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status
               FROM simulator_20250420.feed_actions
               GROUP BY user_id)
               GROUP BY status, this_week, previous_week) AS virtual_table
               WHERE this_week >= toDate('2025-03-01') AND ((this_week < toMonday(today())))
               GROUP BY status, toMonday(toDateTime(this_week))
               ORDER BY date
                '''
        df = ph.read_clickhouse(query, connection=connection)
        return df
    
    @task()
    def chart_report(df):
        df['date'] = pd.to_datetime(df['date']).dt.date
        stacked_data = df.pivot(index='date', columns='status', values='AVG(num_users)').fillna(0)

        stacked_data.plot(kind='bar', stacked=True, figsize=(10, 6))

        plt.title('Количество пользователей по неделям (в разрезе статусов)')
        plt.ylabel('Количество пользователей')
        plt.legend(title='Статус')
        plt.xlabel('')
        plt.grid(True, axis='y', linestyle='--', alpha=0.7, zorder=0)
        plt.legend(title='Статус')
        plt.tight_layout()
                
        buf_chart = io.BytesIO()
        plt.savefig(buf_chart, format='png')
        plt.close()
        buf_chart.seek(0)
        return buf_chart.getvalue()
    
    @task()
    def plot_all_metrics(data):
        data['date'] = pd.to_datetime(data['date'])

        today = datetime.today().date()
        current_monday = today - timedelta(days=today.weekday())
        current_sunday = current_monday + timedelta(days=6)
        end_date_current = today - timedelta(days=1)
        last_monday = current_monday - timedelta(weeks=1)
        last_sunday = current_sunday - timedelta(weeks=1)

        full_date_range = pd.date_range(start=current_monday, end=current_sunday, freq='D')

        current_mask = (data['date'] >= pd.to_datetime(current_monday)) & (data['date'] <= pd.to_datetime(end_date_current))
        previous_mask = (data['date'] >= pd.to_datetime(last_monday)) & (data['date'] <= pd.to_datetime(last_sunday))

        current_data = data[current_mask].copy()
        previous_data = data[previous_mask].copy()

        def align_by_weekday(df, base_date):
            df = df.copy()
            base_date = pd.to_datetime(base_date)
            df['day_of_week'] = df['date'].dt.weekday
            df['aligned_date'] = base_date + pd.to_timedelta(df['day_of_week'], unit='d')
            return df.set_index('aligned_date')

        current_aligned = align_by_weekday(current_data, current_monday)
        previous_aligned = align_by_weekday(previous_data, current_monday)

        combined = pd.DataFrame(index=full_date_range)

        metric_columns = {
            'messages_sent': 'Сообщения',
            'users_sent': 'Отправители',
            'users_received': 'Получатели',
            'views': 'Просмотры',
            'likes': 'Лайки',
            'ctr': 'CTR'
        }

        for metric in metric_columns:
            combined[f'{metric}_current'] = current_aligned[metric]
            combined[f'{metric}_previous'] = previous_aligned[metric]

        fig, axes = plt.subplots(2, 3, figsize=(20, 16))
        axes = axes.flatten()

        for ax, (metric, title) in zip(axes, metric_columns.items()):
            ax.plot(
                combined.index,
                combined[f'{metric}_previous'],
                label='Прошлая неделя',
                linestyle='--',
                marker='o',
                color='gray'
            )
            ax.plot(
                combined.index,
                combined[f'{metric}_current'],
                label='Текущая неделя',
                marker='o',
                color='blue'
            )

            ax.set_title(title)
            ax.grid(True)
            ax.legend()
            ax.tick_params(axis='x', rotation=45)
            ax.set_xticks(combined.index)
            ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in combined.index])

        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        plt.close(fig)
        buf.seek(0)

        return buf.getvalue()

    @task()
    def plot_dau_metric(data):
        data['date'] = pd.to_datetime(data['date'])

        today = datetime.today().date()
        current_monday = today - timedelta(days=today.weekday())
        current_sunday = current_monday + timedelta(days=6)
        end_date_current = today - timedelta(days=1)
        last_monday = current_monday - timedelta(weeks=1)
        last_sunday = current_sunday - timedelta(weeks=1)

        full_date_range = pd.date_range(start=current_monday, end=current_sunday, freq='D')

        current_mask = (data['date'] >= pd.to_datetime(current_monday)) & (data['date'] <= pd.to_datetime(end_date_current))
        previous_mask = (data['date'] >= pd.to_datetime(last_monday)) & (data['date'] <= pd.to_datetime(last_sunday))

        current_data = data[current_mask].copy()
        previous_data = data[previous_mask].copy()

        def align_by_weekday(df, base_date):
            df = df.copy()
            base_date = pd.to_datetime(base_date)
            df['day_of_week'] = df['date'].dt.weekday
            df['aligned_date'] = base_date + pd.to_timedelta(df['day_of_week'], unit='d')
            return df.set_index('aligned_date')

        current_aligned = align_by_weekday(current_data, current_monday)
        previous_aligned = align_by_weekday(previous_data, current_monday)

        combined = pd.DataFrame(index=full_date_range)
        metric = 'feed_users'
        title = 'DAU (активные пользователи)'
        combined[f'{metric}_current'] = current_aligned[metric]
        combined[f'{metric}_previous'] = previous_aligned[metric]

        plt.figure(figsize=(10, 6))
        plt.plot(
            combined.index,
            combined[f'{metric}_previous'],
            label='Прошлая неделя',
            linestyle='--',
            marker='o',
            color='gray'
        )
        plt.plot(
            combined.index,
            combined[f'{metric}_current'],
            label='Текущая неделя',
            marker='o',
            color='blue'
        )

        plt.title(title)
        plt.xticks(combined.index, [d.strftime('%Y-%m-%d') for d in combined.index], rotation=45)
        plt.grid(True)
        plt.legend()
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        plt.close()
        buf.seek(0)

        return buf.getvalue()

    @task()
    def send_telegram_msg(text, all_metrics_plot_bytes, dau_plot_bytes, cohort_plot_bytes, file_object):
        bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
        bot.send_photo(chat_id=chat_id, photo=dau_plot_bytes)
        bot.send_photo(chat_id=chat_id, photo=all_metrics_plot_bytes)
        bot.send_photo(chat_id=chat_id, photo=cohort_plot_bytes)
        bot.sendDocument(chat_id=chat_id, document=file_object.read(), filename=file_object.name)
        
        
        
    data = extract_data()
    text_report = text_report(data)
    dau_plot = plot_dau_metric(data)
    metrics_plot = plot_all_metrics(data)
    file = file_report(data)
    df = extract_df()
    chart_report = chart_report(df)
    send_telegram_msg(text_report, metrics_plot, dau_plot, chart_report, file)
    
big_report_basov_dag = big_report_basov_dag()
