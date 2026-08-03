import os
import pprint

from airflow.models import DagBag


_DAGS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../dags")
)


def test_dagbag_import():
    dagbag = DagBag(
        dag_folder=_DAGS_DIR,
        include_examples=False,
    )

    assert (
        len(dagbag.import_errors) == 0
    ), f"Import errors:\n{pprint.pformat(dagbag.import_errors)}"

    assert len(dagbag.dags) > 0


test_dagbag_import() 