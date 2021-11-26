import json

from mipdb.exceptions import DataBaseError
from mipdb.reader import CSVFileReader

import pytest

from mipdb.schema import Schema
from mipdb.tables import (
    ActionsTable,
    DataModelTable,
    DatasetsTable,
    MetadataTable,
    PrimaryDataTable,
)
from mipdb.dataelements import CommonDataElement, make_cdes
from mipdb.dataset import Dataset
from tests.mocks import MonetDBMock


@pytest.fixture
def metadata():
    return Schema("mipdb_metadata")


@pytest.fixture
def cdes(data_model_data):
    return make_cdes(data_model_data)


def test_data_models_table_mockdb(metadata):
    # Setup
    db = MonetDBMock()
    # Test
    DataModelTable(schema=metadata).create(db)
    assert f"CREATE SEQUENCE mipdb_metadata.data_model_id_seq" == db.captured_queries[0]
    expected_create = (
        f"\nCREATE TABLE mipdb_metadata.data_models ("
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
    DataModelTable(schema=schema).create(db)
    res = db.execute(
        "SELECT name, type FROM sys.columns WHERE "
        "table_id=(SELECT id FROM sys.tables "
        "WHERE name='data_models' AND system=FALSE)"
    )
    assert res.fetchall() != []


def test_actions_table(metadata):
    # Setup
    db = MonetDBMock()
    # Test
    ActionsTable(schema=metadata).create(db)
    assert f"CREATE SEQUENCE mipdb_metadata.action_id_seq" in db.captured_queries[0]
    assert f"CREATE TABLE mipdb_metadata.actions" in db.captured_queries[1]


def test_delete_schema(metadata):
    # Setup
    db = MonetDBMock()
    data_models_table = DataModelTable(schema=metadata)
    # Test
    data_models_table.delete_data_model(code="schema", version="1.0", db=db)
    expected = f"DELETE FROM mipdb_metadata.data_models WHERE code = :code AND version = :version "
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
    def test_insert_values_with_db(self, db, data_model_data):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(db)
        metadata_table = MetadataTable(schema)
        metadata_table.create(db)
        # Test
        values = metadata_table.get_values_from_cdes(make_cdes(data_model_data))
        metadata_table.insert_values(values, db)
        res = db.execute(
            "SELECT code, json.filter(metadata, '$.isCategorical') "
            f'FROM "schema:1.0".variables_metadata'
        )
        result = [(name, json.loads(val)) for name, val in res.fetchall()]
        assert result == [
            ("var1", [False]),
            ("var2", [True]),
            ("dataset", [True]),
            ("var3", [False]),
            ("var4", [False]),
        ]

    def test_get_values_from_cdes_full_schema_data(self, data_model_data):
        # Setup
        metadata_table = MetadataTable(Schema("schema:1.0"))
        cdes = make_cdes(data_model_data)
        # Test
        result = metadata_table.get_values_from_cdes(cdes)
        assert len(result) == 5

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_load_from_db(self, data_model_data, db):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(db)
        metadata_table = MetadataTable(schema)
        metadata_table.create(db)
        values = metadata_table.get_values_from_cdes(make_cdes(data_model_data))
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
            "\n\tvar1 VARCHAR(255), "
            "\n\tvar2 VARCHAR(255), "
            "\n\tdataset VARCHAR(255), "
            "\n\tvar3 FLOAT, "
            "\n\tvar4 FLOAT\n)\n\n"
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

    def test_insert_dataset_mockdb(self, cdes):
        # Setup
        db = MonetDBMock()
        dataset_file = "tests/data/dataset.csv"
        reader = CSVFileReader(dataset_file)
        dataset = Dataset(reader.read())
        schema = Schema("schema:1.0")
        # Test
        primary_data_table = PrimaryDataTable.from_cdes(schema, cdes)
        primary_data_table.insert_dataset(dataset, db=db)
        assert 'INSERT INTO "schema:1.0".primary_data' in db.captured_queries[0]
        assert len(db.captured_multiparams[0][0]) > 0

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_insert_dataset_with_db(self, db, cdes):
        # Setup
        dataset_file = "tests/data/dataset.csv"
        reader = CSVFileReader(dataset_file)
        dataset = Dataset(reader.read())
        schema = Schema("schema:1.0")
        schema.create(db)
        primary_data_table = PrimaryDataTable.from_cdes(schema, cdes)
        primary_data_table.create(db)
        # Test
        primary_data_table.insert_dataset(dataset, db=db)
        res = db.execute('SELECT * FROM "schema:1.0".primary_data').fetchall()
        assert res != []
