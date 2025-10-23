import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime , timedelta
from batch.scripts.ingestion import ingestion 

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',filename="/opt/airflow/logs/app.log",filemode='a')
logger=logging.getLogger(__name__)

args={ 
    'owner': 'ali rahiqi',
    'depends_on_past': False,
    'retries': 3,
    'execution_timeout': timedelta(hours=1),
    'retry_delay': timedelta(minutes=3)}


def on_failure_callback(context):
    task_instance = context['task_instance']
    logger.error(f"Task {task_instance.task_id} failed. DAG: {task_instance.dag_id} at {datetime.now()}")



with DAG('disasters', default_args=args, start_date=datetime(2025,1,1), schedule_interval='@yearly', catchup=False ,on_failure_callback=on_failure_callback) as dag :
    ingestion = PythonOperator(task_id="ingestion",python_callable=ingestion)
    transformation = BashOperator(task_id='transformation',bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0  /project/batch/scripts/transformation.py')
    modelization = BashOperator(task_id='modelization',bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0  /project/batch/scripts/modelization.py')


    ingestion>>transformation>>modelization
  





    