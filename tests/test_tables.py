import json

from mipdb.exceptions import DataBaseError

import pytest

from mipdb.monetdb_tables import PrimaryDataTable
from mipdb.schema import Schema
from mipdb.sqlite_tables import (
    DataModelTable,
    DatasetsTable,
    MetadataTable,
)
from mipdb.dataelements import CommonDataElement, flatten_cdes


@pytest.fixture
def cdes(data_model_metadata):
    return flatten_cdes(data_model_metadata)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_data_models_table_realdb(sqlite_db):
    # Test
    DataModelTable().create(sqlite_db)
    assert sqlite_db.get_all_tables() != []


class TestVariablesMetadataTable:
    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_create_table_with_db(self, sqlite_db):
        # Setup

        metadata_table = MetadataTable("data_model:1.0")
        # Test
        metadata_table.create(sqlite_db)
        res = sqlite_db.get_metadata("data_model:1.0")
        assert res == {}

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_insert_values_with_db(self, sqlite_db, data_model_metadata):
        # Setup

        metadata_table = MetadataTable("data_model:1.0")
        metadata_table.create(sqlite_db)
        # Test
        values = metadata_table.get_values_from_cdes(flatten_cdes(data_model_metadata))
        metadata_table.insert_values(values, sqlite_db)
        res = sqlite_db.get_metadata("data_model:1.0")
        result = [(code, metadata["is_categorical"]) for code, metadata in res.items()]
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
        metadata_table = MetadataTable("data_model:1.0")
        cdes = flatten_cdes(data_model_metadata)
        # Test
        result = metadata_table.get_values_from_cdes(cdes)
        assert len(result) == 6

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_load_from_db(self, data_model_metadata, sqlite_db):
        # Setup

        data_model = "data_model:1.0"
        metadata_table = MetadataTable(data_model)
        metadata_table.create(sqlite_db)
        values = metadata_table.get_values_from_cdes(flatten_cdes(data_model_metadata))
        metadata_table.insert_values(values, sqlite_db)
        # Test

        metadata_table = MetadataTable.from_db(data_model, sqlite_db)
        assert all(isinstance(cde, str) for cde in metadata_table.table.keys())
        assert all(
            isinstance(cde, CommonDataElement) for cde in metadata_table.table.values()
        )


class TestPrimaryDataTable:
    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_create_table_with_db(self, cdes, monetdb):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(monetdb)
        # Test
        primary_data_table = PrimaryDataTable.from_cdes(schema, cdes)
        primary_data_table.create(monetdb)
        res = monetdb.execute('SELECT * FROM "schema:1.0".primary_data').fetchall()
        assert res == []

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_drop_table_with_db(self, cdes, monetdb):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(monetdb)
        primary_data_table = PrimaryDataTable.from_cdes(schema, cdes)
        primary_data_table.create(monetdb)
        # Test

        primary_data_table.drop(monetdb)
        with pytest.raises(DataBaseError):
            monetdb.execute('SELECT * FROM "schema:1.0".primary_data').fetchall()

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_reflect_table_from_db(self, cdes, monetdb):
        # Setup
        schema = Schema("schema:1.0")
        schema.create(monetdb)
        PrimaryDataTable.from_cdes(schema, cdes).create(monetdb)
        # Test
        primary_data_table = PrimaryDataTable.from_db(schema, monetdb)
        column_names = [c.name for c in list(primary_data_table.table.columns)]
        assert column_names != []
