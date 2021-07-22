from mipdb.dataset import Dataset
import click as cl

from mipdb.database import MonetDB, get_db_config
from mipdb.tables import SchemasTable
from mipdb.reader import CSVFileReader, JsonFileReader
from mipdb.schema import Schema
from mipdb.constants import METADATA_SCHEMA
from mipdb.usecases import (
    DeleteSchema,
    make_cdes,
    AddSchema,
    AddDataset,
    InitDB,
)
from mipdb.exceptions import (
    DataBaseError,
    UserInputError,
    FileContentError,
    handle_errors,
    ExitCode,
)


@cl.group()
def entry():
    pass


@entry.command()
@handle_errors
def init():
    dbconfig = get_db_config()
    db = MonetDB.from_config(dbconfig)
    InitDB(db).execute()


@entry.command()
@cl.argument("file", required=True)
@cl.option("-v", "--version", required=True, help="The schema version")
# @cl.option("--dry-run", is_flag=True)
@handle_errors
def add_schema(file, version):
    reader = JsonFileReader(file)
    dbconfig = get_db_config()
    db = MonetDB.from_config(dbconfig)
    schema_data = reader.read()
    schema_data["version"] = version
    AddSchema(db).execute(schema_data)


@entry.command()
@cl.argument("file", required=True)
@cl.option(
    "-s",
    "--schema",
    required=True,
    help="The schema to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The schema version")
@handle_errors
def add_dataset(file, schema, version):
    reader = CSVFileReader(file)
    dbconfig = get_db_config()
    db = MonetDB.from_config(dbconfig)
    data = reader.read()
    dataset = Dataset(data)
    AddDataset(db).execute(dataset, schema, version)


@entry.command()
@handle_errors
def validate_dataset():
    pass


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The schema version")
@handle_errors
def delete_schema(name, version):
    db = MonetDB.from_config(get_db_config())
    metadata = Schema(METADATA_SCHEMA)
    schemas_table = SchemasTable(schema=metadata)
    DeleteSchema(db).execute(name, version)


@entry.command()
@handle_errors
def delete_dataset():
    pass


@entry.command()
@handle_errors
def enable():
    pass


@entry.command()
@handle_errors
def disable():
    pass


@entry.command()
@handle_errors
def tag():
    pass


@entry.command("list")
@handle_errors
def list_():
    pass
