import pendulum
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="part_2_task_5_dag",
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 3, 6, tz="UTC"),
    catchup=False,
) as dag:
    task_1 = DummyOperator(task_id="task_1")
   
    with TaskGroup("tasks_2_3_parallel") as tasks_2_3_parallel:
        task_2 = DummyOperator(task_id="task_2")
        task_3 = DummyOperator(task_id="task_3")

    with TaskGroup("tasks_4_5_6_parallel") as tasks_4_5_6_parallel:
        task_4 = DummyOperator(task_id="task_4")
        task_5 = DummyOperator(task_id="task_5")
        task_6 = DummyOperator(task_id="task_6")


    task_1 >> tasks_2_3_parallel >> tasks_4_5_6_parallel

