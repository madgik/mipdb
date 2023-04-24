import os
import time

import pytest
import docker

from mipdb.commands import get_db_config
from mipdb.database import MonetDB
from mipdb.reader import JsonFileReader
from mipdb.tables import User

DATA_MODEL_FILE = "tests/data/success/data_model_v_1_0/CDEsMetadata.json"
DATASET_FILE = "tests/data/success/data_model_v_1_0/dataset.csv"
DATA_FOLDER = "tests/data/"
SUCCESS_DATA_FOLDER = DATA_FOLDER + "success"
FAIL_DATA_FOLDER = DATA_FOLDER + "fail"
ABSOLUTE_PATH_DATA_FOLDER = f"{os.path.dirname(os.path.realpath(__file__))}/data/"
ABSOLUTE_PATH_DATASET_FILE = f"{os.path.dirname(os.path.realpath(__file__))}/data/success/data_model_v_1_0/dataset.csv"
ABSOLUTE_PATH_SUCCESS_DATA_FOLDER = ABSOLUTE_PATH_DATA_FOLDER + "success"
ABSOLUTE_PATH_FAIL_DATA_FOLDER = ABSOLUTE_PATH_DATA_FOLDER + "fail"
IP = "127.0.0.1"
PORT = 50123
USERNAME = "admin"
PASSWORD = "admin"
DB_NAME = "db"

DEFAULT_OPTIONS = [
    "--ip",
    IP,
    "--port",
    PORT,
    "--username",
    USERNAME,
    "--password",
    PASSWORD,
    "--db_name",
    DB_NAME,
]


@pytest.fixture
def data_model_metadata():
    reader = JsonFileReader(DATA_MODEL_FILE)
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
            "madgik/mipenginedb:dev",
            detach=True,
            ports={"50000/tcp": PORT},
            name="mipdb-testing",
            volumes=[f"{ABSOLUTE_PATH_DATA_FOLDER}:{ABSOLUTE_PATH_DATA_FOLDER}"],
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
    dbconfig = get_db_config(IP, PORT, USERNAME, PASSWORD, DB_NAME)
    return MonetDB.from_config(dbconfig)


@pytest.fixture(scope="function")
def cleanup_db(db):
    yield
    schemas = db.get_schemas()
    for schema in schemas:
        if schema not in [user.value for user in User]:
            db.drop_schema(schema)
