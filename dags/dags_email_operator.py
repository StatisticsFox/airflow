from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id= 'send_email_task',
        to='1234kimlove@naver.com',
        subject="사랑해",
        html_content="Airflow 작업을 완료했습니다."
    )