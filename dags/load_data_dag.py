# imports
import json
import logging
import requests
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


log = logging.getLogger(__name__)

# setting up the default arguments
default_args = {
    "owner": 'airflow',
    "depends_on_past": False
}
# initialising DAG
dag = DAG('LoadData',
          description='Pipeline to load data to DB',
          schedule_interval=None,
          start_date=datetime(2018, 2, 15),
          catchup=False,
          default_args=default_args)

# getting the target URL
url = Variable.get('url', default_var='')
response = requests.get(url)
my_dict = response.json()

# forming the list of tuple objects
rows = []
for state in my_dict.keys():
    for date in my_dict[state]['dates'].keys():
        data = my_dict[state]['dates'][date]
        try:
            delta_confirmed = data['delta']['confirmed']
        except KeyError:
            delta_confirmed = 0
        my_tup = (state, date, json.dumps(data), delta_confirmed)
        rows.append(my_tup)

values = ', '.join(map(str, rows))
sql = 'INSERT INTO covid_data (state, date, data, delta_confirmed) VALUES {}'.format(values)

# operator for loading data to the DB
load_to_db = SqliteOperator(
    task_id='load_to_db',
    sql=sql,
    dag=dag
)


def conditionally_trigger(context, dag_run_obj):
    return dag_run_obj


# operator to trigger the aggregation DAG
start_aggregation = TriggerDagRunOperator(
    task_id='start_aggregation',
    trigger_dag_id='AggregateData',
    python_callable=conditionally_trigger,
    dag=dag
)

# setting up dependencies
load_to_db.set_downstream(start_aggregation)
