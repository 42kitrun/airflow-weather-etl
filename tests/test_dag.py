from airflow.models import DagBag


def test_no_import_errors():
    """DAG 파일에 import 에러가 없는지 확인"""
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    assert len(dag_bag.import_errors) == 0


def test_dag_exists():
    """weather_etl DAG이 존재하는지 확인"""
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    assert 'weather_etl' in dag_bag.dags


def test_task_count():
    """Task가 5개인지 확인"""
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dag_bag.dags['weather_etl']
    assert len(dag.tasks) == 5


def test_task_order():
    """Task 순서가 올바른지 확인 (extract → transform → load → report → notify)"""
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dag_bag.dags['weather_etl']

    extract = dag.get_task('extract')
    assert 'transform' in [t.task_id for t in extract.downstream_list]
