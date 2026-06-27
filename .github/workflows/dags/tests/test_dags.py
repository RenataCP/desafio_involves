import pytest
from airflow.models import DagBag

@pytest.fixture()
def dag_bag():
    return DagBag(dag_folder="dags/", include_examples=False)

def test_dags_load_without_import_errors(dag_bag):
    assert len(dag_bag.import_errors) == 0, f"DAG import errors:{dag_bag.import_errors}"

def test_all_dags_have_owners(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.default_args.get("owner") != None, f"{dag_id} uses default owner"