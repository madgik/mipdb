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
from mipdb.usecases import TagSchema
from mipdb.usecases import TagDataset


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
@cl.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force deletion of dataset that are based on the schema",
)
@handle_errors
def delete_schema(name, version, force):
    db = MonetDB.from_config(get_db_config())
    DeleteSchema(db).execute(name, version, force)


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-s", "--schema", required=True, help="The schema to which the dataset is added"
)
@cl.option("-v", "--version", required=True, help="The schema version")
@handle_errors
def delete_dataset(dataset, schema, version):
    db = MonetDB.from_config(get_db_config())
    DeleteDataset(db).execute(dataset, schema, version)


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The schema version")
@handle_errors
def enable_schema(name, version):
    db = MonetDB.from_config(get_db_config())
    EnableSchema(db).execute(name, version)


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The schema version")
@handle_errors
def disable_schema(name, version):
    db = MonetDB.from_config(get_db_config())
    DisableSchema(db).execute(name, version)


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
def enable_dataset(dataset, schema, version):
    db = MonetDB.from_config(get_db_config())
    EnableDataset(db).execute(dataset, schema, version)


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
def disable_dataset(dataset, schema, version):
    db = MonetDB.from_config(get_db_config())
    DisableDataset(db).execute(dataset, schema, version)


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The schema version")
@cl.option(
    "-t",
    "--tag",
    default=None,
    required=False,
    help="A tag to be added/removed at the properties",
)
@cl.option(
    "-kv",
    "--key-value",
    default=None,
    nargs=2,
    required=False,
    help="A key value to be added/removed at the properties",
)
@cl.option(
    "-r",
    "--remove-flag",
    is_flag=True,
    required=False,
    help="A flag that determines if the tag/key_value will be added or removed",
)
@handle_errors
def tag_schema(name, version, tag, key_value, remove_flag):
    db = MonetDB.from_config(get_db_config())
    TagSchema(db).execute(name, version, tag, key_value, remove_flag)


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-s", "--schema", required=True, help="The schema to which the dataset is added"
)
@cl.option("-v", "--version", required=True, help="The schema version")
@cl.option(
    "-t",
    "--tag",
    default=None,
    required=False,
    help="A tag to be added/removed at the properties",
)
@cl.option(
    "-kv",
    "--key-value",
    default=None,
    nargs=2,
    required=False,
    help="A key value to be added/removed at the properties",
)
@cl.option(
    "-r",
    "--remove-flag",
    is_flag=True,
    required=False,
    help="A flag that determines if the tag/key_value will be added or removed",
)
@handle_errors
def tag_dataset(dataset, schema, version, tag, key_value, remove_flag):
    db = MonetDB.from_config(get_db_config())
    TagDataset(db).execute(dataset, schema, version, tag, key_value, remove_flag)


@entry.command("list")
@handle_errors
def list_():
    pass
