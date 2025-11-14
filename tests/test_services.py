import pytest

from mipdb.duckdb import MetadataTable, PrimaryDataTable, Schema
from mipdb.duckdb.metadata_tables import DataModelTable, DatasetsTable
from mipdb.services import (
    add_data_model,
    cleanup_database,
    import_dataset,
    init_database,
    validate_dataset,
)
from tests.conftest import DATA_MODEL_FILE, DATASET_FILE


@pytest.mark.database
def test_add_data_model_creates_primary_and_metadata_tables(duckdb, data_model_metadata):
    init_database(duckdb)
    add_data_model(duckdb, data_model_metadata)

    metadata = MetadataTable.from_db("data_model:1.0", duckdb).table
    assert "dataset" in metadata

    primary_table = PrimaryDataTable.from_db(Schema("data_model:1.0"), duckdb)
    column_names = [col.name for col in primary_table.table.columns]
    assert "dataset" in column_names


@pytest.mark.database
def test_import_dataset_populates_rows(duckdb, data_model_metadata):
    init_database(duckdb)
    add_data_model(duckdb, data_model_metadata)

    validate_dataset(duckdb, DATASET_FILE, "data_model", "1.0")
    import_dataset(duckdb, DATASET_FILE, "data_model", "1.0")

    rows = duckdb.execute_fetchall('SELECT COUNT(*) FROM "data_model_1_0__primary_data"')
    assert rows[0][0] == 5

    datasets = DatasetsTable().get_dataset_codes(duckdb)
    assert datasets == ["dataset"]


@pytest.mark.database
def test_cleanup_database_drops_models(duckdb, data_model_metadata):
    init_database(duckdb)
    add_data_model(duckdb, data_model_metadata)
    import_dataset(duckdb, DATASET_FILE, "data_model", "1.0")

    cleanup_database(duckdb)

    data_models = DataModelTable().get_data_models(duckdb)
    assert data_models == []

    datasets = DatasetsTable().get_datasets(duckdb)
    assert datasets == []
