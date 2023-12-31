from airflow import DAG
import pendulum
from airflow.decorators import task



with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # 파이썬 오퍼레이터에서 템플릿과 함께 macro를 사용한 경우
    @task(task_id='task_using_macros',
      templates_dict={'start_date':'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
                      'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
     }
    )
    def get_datetime_macro(**kwargs):
        # 위 templates_dict라는 string이 key값 나머지 전체가 value값으로 들어감 
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date없음' # key 값이 없으면 스트링으로 없다고 할당
            end_date = templates_dict.get('end_date') or 'end_date없음'
            print(start_date)
            print(end_date)

    # dag 안에서 직접 날짜 연산을 한 경우 
    @task(task_id='task_direct_calc')
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta
        # 스케줄러 부하 경감을 위해 패키지 import 하는 부분을 task 안에 명명 주기적으로 문법 검사를 하는 airflow의 부하를 줄일 수 있음
        # 오퍼레이터 안에만 필요한 함수는 오퍼레이터 안에 선언을 하는게 나중에 신상에 좋음
        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone('Asia/Seoul').replace(day=1) + relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d')) # 날짜 형식 지정
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()