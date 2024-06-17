import json

from mipdb.exceptions import DataBaseError

import pytest

from mipdb.schema import Schema
from mipdb.tables import (
    DataModelTable,
    DatasetsTable,
    MetadataTable,
    PrimaryDataTable,
)
from mipdb.dataelements import CommonDataElement, flatten_cdes
from tests.mocks import MonetDBMock

@pytest.fixture
def cdes(data_model_metadata):
    return flatten_cdes(data_model_metadata)


def test_get_data_models():
    # Setup
    db = MonetDBMock()
    # Test
    data_models = DataModelTable()
    data_models.get_data_models(db=db, columns=["data_model_id", "code"])


def test_get_data_models_without_valid_columns():
    # Setup
    db = MonetDBMock()
    # Test
    data_models = DataModelTable()
    with pytest.raises(ValueError):
        data_models.get_data_models(
            db=db, columns=["data_model_id", "non-existing column"]
        )


def test_get_datasets():
    # Setup
    db = MonetDBMock()
    # Test
    datasets = DatasetsTable()
    datasets.get_values(db=db, columns=["dataset_id", "data_model_id"])


def test_get_datasets_without_valid_columns():
    # Setup
    db = MonetDBMock()
    # Test
    datasets = DatasetsTable()
    with pytest.raises(ValueError):
        datasets.get_values(db=db, columns=["dataset_id", "non-existing column"])


def test_data_models_table_mockdb():
    # Setup
    db = MonetDBMock()
    # Test
    DataModelTable().create(db)
    assert f"CREATE SEQUENCE data_model_id_seq" == db.captured_queries[0]
    expected_create = (
        f"\nCREATE TABLE data_models ("
        "\n\tdata_model_id INTEGER NOT NULL, "
        "\n\tcode VARCHAR(255) NOT NULL, "
        "\n\tversion VARCHAR(255) NOT NULL, "
        "\n\tlabel VARCHAR(255), "
        "\n\tstatus VARCHAR(255) NOT NULL, "
        "\n\tproperties JSON, "
        "\n\tPRIMARY KEY (data_model_id)"
        "\n)\n\n"
    )
    assert expected_create == db.captured_queries[1]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_data_models_table_realdb(db):
    # Setup
    schema = Schema("schema")
    schema.create(db)
    # Test
    DataModelTable().create(db)
    res = db.execute(
        "SELECT name, type FROM sys.columns WHERE "
        "table_id=(SELECT id FROM sys.tables "
        "WHERE name='data_models' AND system=FALSE)"
    )
    assert res.fetchall() != []

def test_delete_schema():
    # Setup
    db = MonetDBMock()
    data_models_table = DataModelTable()
    # Test
    data_models_table.delete_data_model(code="schema", version="1.0", db=db)
    expected = f"DELETE FROM data_models WHERE code = :code AND version = :version "
    assert expected in db.captured_queries[0]


class TestVariablesMetadataTable:
    def test_create_table_mockdb(self):
        # Setup
        db = MonetDBMock()
        metadata_table = MetadataTable(Schema("schema:1.0"))
        # Test
        metadata_table.create(db)
        assert f'CREATE TABLE "schema:1.0".variables_metadata' in db.captured_queries[0]

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_create_table_with_db(self, db):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(db)
        metadata_table = MetadataTable(schema)
        # Test
        metadata_table.create(db)
        res = db.execute(f'SELECT * FROM "schema:1.0".variables_metadata').fetchall()
        assert res == []

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_insert_values_with_db(self, db, data_model_metadata):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(db)
        metadata_table = MetadataTable(schema)
        metadata_table.create(db)
        # Test
        values = metadata_table.get_values_from_cdes(flatten_cdes(data_model_metadata))
        metadata_table.insert_values(values, db)
        res = db.execute(
            "SELECT code, json.filter(metadata, '$.is_categorical') "
            f'FROM "schema:1.0".variables_metadata'
        )
        result = [(name, json.loads(val)) for name, val in res.fetchall()]
        assert result == [
            ("var1", False),
            ("subjectcode", False),
            ("var2", True),
            ("dataset", True),
            ("var3", False),
            ("var4", False),
        ]

    def test_get_values_from_cdes_full_schema_data(self, data_model_metadata):
        # Setup
        metadata_table = MetadataTable(Schema("schema:1.0"))
        cdes = flatten_cdes(data_model_metadata)
        # Test
        result = metadata_table.get_values_from_cdes(cdes)
        assert len(result) == 6

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_load_from_db(self, data_model_metadata, db):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(db)
        metadata_table = MetadataTable(schema)
        metadata_table.create(db)
        values = metadata_table.get_values_from_cdes(flatten_cdes(data_model_metadata))
        metadata_table.insert_values(values, db)
        # Test
        schema = Schema("schema:1.0")
        metadata_table = MetadataTable.from_db(schema, db)
        assert all(isinstance(cde, str) for cde in metadata_table.table.keys())
        assert all(
            isinstance(cde, CommonDataElement) for cde in metadata_table.table.values()
        )


class TestPrimaryDataTable:
    def test_create_table_mockdb(self, cdes):
        # Setup
        db = MonetDBMock()
        schema = Schema("schema:1.0")
        # Test
        primary_data_table = PrimaryDataTable.from_cdes(schema, cdes)
        primary_data_table.create(db)
        expected = (
            '\nCREATE TABLE "schema:1.0".primary_data ('
            '\n\t"row_id" INTEGER NOT NULL, '
            '\n\t"var1" VARCHAR(255), '
            '\n\t"subjectcode" VARCHAR(255), '
            '\n\t"var2" VARCHAR(255), '
            '\n\t"dataset" VARCHAR(255), '
            '\n\t"var3" FLOAT, '
            '\n\t"var4" INTEGER, '
            '\n\tPRIMARY KEY ("row_id")\n)\n\n'
        )
        assert db.captured_queries[0] == expected

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_create_table_with_db(self, cdes, db):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(db)
        # Test
        primary_data_table = PrimaryDataTable.from_cdes(schema, cdes)
        primary_data_table.create(db)
        res = db.execute('SELECT * FROM "schema:1.0".primary_data').fetchall()
        assert res == []

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_drop_table_with_db(self, cdes, db):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(db)
        primary_data_table = PrimaryDataTable.from_cdes(schema, cdes)
        primary_data_table.create(db)
        # Test

        primary_data_table.drop(db)
        with pytest.raises(DataBaseError):
            db.execute('SELECT * FROM "schema:1.0".primary_data').fetchall()

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_reflect_table_from_db(self, cdes, db):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(db)
        PrimaryDataTable.from_cdes(schema, cdes).create(db)
        # Test
        primary_data_table = PrimaryDataTable.from_db(schema, db)
        column_names = [c.name for c in list(primary_data_table.table.columns)]
        assert column_names != []
