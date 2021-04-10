from airflow import DAG
from  datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from callable.callable_mvp import callable_mvp
from callable.config_mvp import config_mvp

def create_dag(dag_id, default_args, schedule, env):
    dag = DAG(dag_id,
              default_args=default_args,
              schedule_interval=schedule
              )

    with dag:
        
        configuration = PythonOperator(
            dag = dag,
            task_id = 'configuration',
            provide_context = True,
            op_kwargs={'env': env , 'client':'nbfc'},
            #op_kwargs={'env': env , 'client':'ffhlatam_arg'},
            python_callable = config_mvp)

        batch_prediction = PythonOperator(
            dag = dag,
            task_id = 'mvp',
            provide_context = True,
            op_kwargs={'env': env , 'client':'nbfc'},
            #op_kwargs={'env': env , 'client':'ffhlatam_arg'},
            python_callable = callable_mvp)

        configuration >> batch_prediction

    return dag

for env in ['test']:
    dag_id = f'Simon_mvp_{env}'

    default_args = {
        'owner': 'nbfc_airflow_service',
        'depends_on_past': False,
        'catchup': False,
        'start_date': datetime.now(),
        'email': ['acerelli@fvlab.ca'],
        'email_on_failure': True,
        'retries': 0
    }

    schedule = '@once'

    globals()[dag_id] = create_dag(dag_id, default_args, schedule, env)
