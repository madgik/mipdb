import time

import pytest
import docker

from mipdb.database import MonetDB, get_db_config
from mipdb.reader import CSVFileReader
from mipdb.reader import JsonFileReader

DATA_MODEL_FILE = "tests/data/success/data_model_v_1_0/CDEsMetadata.json"
DATASET_FILE = "tests/data/success/data_model_v_1_0/dataset.csv"
PORT = 50123


@pytest.fixture
def data_model_metadata():
    reader = JsonFileReader(DATA_MODEL_FILE)
    return reader.read()


@pytest.fixture
def dataset_data():
    reader = CSVFileReader(DATASET_FILE)
    return reader.read()


@pytest.fixture
def dataset_data():
    dataset_file = "tests/data/success/data_model_v_1_0/dataset.csv"
    reader = CSVFileReader(dataset_file)
    return reader.read()


class MonetDBSetupError(Exception):
    """Raised when the MonetDB container is unable to start."""


class DockerNotFoundError(Exception):
    """Raised when attempting to run tests while docker daemon is not running."""


@pytest.fixture(scope="session")
def monetdb_container():
    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        raise DockerNotFoundError(
            "The docker daemon cannot be found. Make sure it is running." ""
        )
    try:
        container = client.containers.get("mipdb-testing")
    except docker.errors.NotFound:
        container = client.containers.run(
            "madgik/mipenginedb:latest",
            detach=True,
            ports={"50000/tcp": "50123"},
            name="mipdb-testing",
            publish_all_ports=True,
        )
    # The time needed to start a monetdb container varies considerably. We need
    # to wait until some phrases appear in the logs to avoid starting the tests
    # too soon. The process is abandoned after 100 tries (50 sec).
    for _ in range(100):
        if b"new database mapi:monetdb" in container.logs():
            break
        time.sleep(0.5)
    else:
        raise MonetDBSetupError
    yield
    container = client.containers.get("mipdb-testing")
    container.remove(v=True, force=True)


@pytest.fixture(scope="function")
def db():
    dbconfig = get_db_config(ip=None, port=50123)
    return MonetDB.from_config(dbconfig)


@pytest.fixture(scope="function")
def cleanup_db(db):
    yield
    schemas = db.get_schemas()
    for schema in schemas:
        db.drop_schema(schema)
