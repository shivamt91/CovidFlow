# imports
import logging
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.python_operator import PythonOperator


log = logging.getLogger(__name__)

# setting up the default arguments
default_args = {
    "owner": 'airflow',
    "depends_on_past": False
}
# initialising DAG
dag = DAG('AggregateData',
          description='Pipeline to aggregate data in DB',
          schedule_interval=None,
          start_date=datetime(2018, 2, 15),
          catchup=False,
          default_args=default_args)


# custom SQLite operator to return results
class MySqliteOperator(SqliteOperator):
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = SqliteHook(sqlite_conn_id=self.sqlite_conn_id)
        return hook.get_records(self.sql, parameters=self.parameters)


sql = 'SELECT state, date, data, MAX(delta_confirmed) FROM covid_data GROUP BY state;'
# operator to run aggregate results query
aggregate_from_db = MySqliteOperator(
    task_id='aggregate_from_db',
    sql=sql,
    dag=dag
)


# method to show results using XCOM
def get_results(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='aggregate_from_db')
    df = pd.DataFrame(data=data)
    df.columns = ['state', 'date', 'data', 'delta_confirmed']
    log.info(df)


# operator for showing results
show_results = PythonOperator(
    task_id='show_results',
    provide_context=True,
    python_callable=get_results,
    dag=dag
)

# setting up dependencies
aggregate_from_db.set_downstream(show_results)
