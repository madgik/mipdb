import click as cl

from mipdb.database import MonetDB, get_db_config
from mipdb.reader import CSVFileReader, JsonFileReader
from mipdb.usecases import DeleteDataset
from mipdb.usecases import DeleteSchema
from mipdb.usecases import AddSchema
from mipdb.usecases import AddDataset
from mipdb.usecases import InitDB
from mipdb.exceptions import handle_errors
from mipdb.usecases import DisableDataset
from mipdb.usecases import DisableSchema
from mipdb.usecases import EnableDataset
from mipdb.usecases import EnableSchema
from mipdb.usecases import ListDatasets
from mipdb.usecases import ListSchemas


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
    schema_data["version"] = version  # schema_data should contain version
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
    dataset_data = reader.read()
    AddDataset(db).execute(dataset_data, schema, version)


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
    DeleteSchema(db).execute(name, version)


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-s",
    "--schema",
    required=True,
    help="The schema to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The schema version")
@handle_errors
def delete_dataset(dataset, schema, version):
    db = MonetDB.from_config(get_db_config())
    DeleteDataset(db).execute(dataset, schema, version)


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