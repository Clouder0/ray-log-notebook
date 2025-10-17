import os

import nbformat
import pytest
from nbconvert.preprocessors import ExecutePreprocessor
from ray.cluster_utils import Cluster


def run_notebook(notebook_path, env=None):
    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=nbformat.NO_CONVERT)

    proc = ExecutePreprocessor(timeout=600)
    # The preprocessor modifies the notebook object in-place.
    proc.preprocess(nb, {"metadata": {"path": "notebooks/"}, "env": env or {}})
    with open("output.ipynb", "w") as f:
        nbformat.write(nb, f)

    return nb


def assert_log_in_notebook(notebook, sentinel):
    all_outputs = []
    for cell in notebook.cells:
        if "outputs" in cell:
            for output in cell.outputs:
                if output.output_type == "stream":
                    all_outputs.append(output.text)
                    if sentinel in output.text:
                        return

    # If the sentinel was not found, fail with a detailed message
    pytest.fail(
        f"Log sentinel '{sentinel}' not found in any cell output.\n"
        f"Captured outputs:\n---\n{''.join(all_outputs)}---"
    )


def test_local_mode_log_capture():
    notebook = run_notebook("notebooks/test_local.ipynb")
    print(notebook)
    assert_log_in_notebook(notebook, "TEST_LOG_SENTINEL_LOCAL")


@pytest.fixture(scope="module")
def ray_cluster():
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "dashboard_port": 8266,
            "ray_client_server_port": 10002,
            "include_dashboard": True,
        },
    )
    address = "ray://127.0.0.1:10002"
    yield address
    cluster.shutdown()


def test_client_mode_log_capture(ray_cluster):
    env = os.environ.copy()
    env["RAY_ADDRESS"] = ray_cluster
    notebook = run_notebook("notebooks/test_client.ipynb", env=env)
    assert_log_in_notebook(notebook, "TEST_LOG_SENTINEL_CLIENT")
