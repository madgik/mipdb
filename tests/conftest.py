import pytest
import docker


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
    yield
    container = client.containers.get("mipdb-testing")
    container.remove(v=True, force=True)
