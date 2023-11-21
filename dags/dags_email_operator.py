from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id= 'send_email_task',
        to='1234kimlove@naver.com',
        subject="사랑이에게 보내는 메일!!",
        html_content="이거 자동으로 메일 보내는거 실습 중인데 잘 가나? 사랑해!! 하투하투하투 바보 멍청이 똥개 해삼 말미잘 \n 이 메세지를 보면 사랑한다고 카톡으로 보내주세요! 안보면 내일 회사 지각함"
    )