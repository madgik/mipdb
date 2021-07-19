import time

import pytest
import docker

from mipdb.database import MonetDB, get_db_config


@pytest.fixture
def schema_data():
    return {
        "code": "schema",
        "label": "The Schema",
        "version": "1.0",
        "variables": [
            {
                "isCategorical": False,
                "code": "var1",
                "sql_type": "text",
                "description": "",
                "label": "",
                "methodology": "",
            },
            {
                "isCategorical": False,
                "code": "var2",
                "sql_type": "text",
                "description": "",
                "label": "",
                "methodology": "",
            },
        ],
        "groups": [
            {
                "name": "group",
                "label": "The Group",
                "variables": [
                    {
                        "isCategorical": False,
                        "code": "var3",
                        "sql_type": "text",
                        "description": "",
                        "label": "",
                        "methodology": "",
                    },
                ],
                "groups": [
                    {
                        "name": "inner_group",
                        "label": "The Inner Group",
                        "variables": [
                            {
                                "isCategorical": False,
                                "code": "var4",
                                "sql_type": "text",
                                "description": "",
                                "label": "",
                                "methodology": "",
                            },
                        ],
                    }
                ],
            }
        ],
    }


@pytest.fixture(scope="session")
def monetdb_container():
    client = docker.from_env()
    try:
        client.containers.get("mipdb-testing")
    except docker.errors.NotFound:
        client.containers.run(
            "madgik/mipenginedb:dev1.4",
            detach=True,
            ports={"50000/tcp": "50123"},
            name="mipdb-testing",
            publish_all_ports=True,
        )
    time.sleep(0.5)  # Container needs to sleep to get its shit together
    yield
    container = client.containers.get("mipdb-testing")
    container.remove(v=True, force=True)


@pytest.fixture(scope='function')
def db():
    dbconfig = get_db_config()
    return MonetDB.from_config(dbconfig)


@pytest.fixture(scope="function")
def cleanup_db(db):
    yield
    schemas = db.get_schemas()
    for schema in schemas:
        db.drop_schema(schema)
