import click as cl

from mipdb.database import MonetDB, get_db_config
from mipdb.reader import CSVFileReader, JsonFileReader
from mipdb.usecases import DeleteDataset
from mipdb.usecases import DeleteDataModel
from mipdb.usecases import AddDataModel
from mipdb.usecases import AddDataset
from mipdb.usecases import InitDB
from mipdb.exceptions import handle_errors
from mipdb.usecases import DisableDataset
from mipdb.usecases import DisableDataModel
from mipdb.usecases import EnableDataset
from mipdb.usecases import EnableDataModel
from mipdb.usecases import TagDataModel
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
@cl.option("-v", "--version", required=True, help="The data model version")
@handle_errors
def add_data_model(file, version):
    reader = JsonFileReader(file)
    dbconfig = get_db_config()
    db = MonetDB.from_config(dbconfig)
    schema_data = reader.read()
    schema_data["version"] = version  # schema_data should contain version
    AddDataModel(db).execute(schema_data)


@entry.command()
@cl.argument("file", required=True)
@cl.option(
    "-d",
    "--data-model",
    required=True,
    help="The data model to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The data model version")
@handle_errors
def add_dataset(file, data_model, version):
    reader = CSVFileReader(file)
    dbconfig = get_db_config()
    db = MonetDB.from_config(dbconfig)
    dataset_data = reader.read()
    AddDataset(db).execute(dataset_data, data_model, version)


@entry.command()
@handle_errors
def validate_dataset():
    pass


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@cl.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force deletion of dataset that are based on the data model",
)
@handle_errors
def delete_data_model(name, version, force):
    db = MonetDB.from_config(get_db_config())
    DeleteDataModel(db).execute(name, version, force)


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-d", "--data-model", required=True, help="The data model to which the dataset is added"
)
@cl.option("-v", "--version", required=True, help="The data model version")
@handle_errors
def delete_dataset(dataset, data_model, version):
    db = MonetDB.from_config(get_db_config())
    DeleteDataset(db).execute(dataset, data_model, version)


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@handle_errors
def enable_data_model(name, version):
    db = MonetDB.from_config(get_db_config())
    EnableDataModel(db).execute(name, version)


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@handle_errors
def disable_data_model(name, version):
    db = MonetDB.from_config(get_db_config())
    DisableDataModel(db).execute(name, version)


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-d",
    "--data-model",
    required=True,
    help="The data model to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The data model version")
@handle_errors
def enable_dataset(dataset, data_model, version):
    db = MonetDB.from_config(get_db_config())
    EnableDataset(db).execute(dataset, data_model, version)


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-d",
    "--data-model",
    required=True,
    help="The data model to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The data model version")
@handle_errors
def disable_dataset(dataset, data_model, version):
    db = MonetDB.from_config(get_db_config())
    DisableDataset(db).execute(dataset, data_model, version)


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
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
    "-a",
    "--add",
    is_flag=True,
    required=False,
    help="A flag that determines if the tag/key_value will be added",
)
@cl.option(
    "-r",
    "--remove",
    is_flag=True,
    required=False,
    help="A flag that determines if the tag/key_value will be removed",
)
@handle_errors
def tag_data_model(name, version, tag, key_value, add, remove):
    db = MonetDB.from_config(get_db_config())
    TagDataModel(db).execute(name, version, tag, key_value, add, remove)


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-d", "--data-model", required=True, help="The data model to which the dataset is added"
)
@cl.option("-v", "--version", required=True, help="The data model version")
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
    "-a",
    "--add",
    is_flag=True,
    required=False,
    help="A flag that determines if the tag/key_value will be added",
)
@cl.option(
    "-r",
    "--remove",
    is_flag=True,
    required=False,
    help="A flag that determines if the tag/key_value will be removed",
)
@handle_errors
def tag_dataset(dataset, data_model, version, tag, key_value, add, remove):
    db = MonetDB.from_config(get_db_config())
    TagDataset(db).execute(dataset, data_model, version, tag, key_value, add, remove)


@entry.command("list")
@handle_errors
def list_():
    pass
