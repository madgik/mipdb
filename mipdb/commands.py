import ipaddress

import click as cl
import os
import glob

from mipdb.monetdb import MonetDB, credentials_from_config
from mipdb.reader import JsonFileReader
from mipdb.sqlite import SQLiteDB
from mipdb.usecases import (
    AddDataModel,
    Cleanup,
    ValidateDatasetNoDatabase,
    ValidateDataModel,
)
from mipdb.usecases import AddPropertyToDataModel
from mipdb.usecases import AddPropertyToDataset
from mipdb.usecases import DeleteDataModel
from mipdb.usecases import DeleteDataset
from mipdb.usecases import ImportCSV
from mipdb.usecases import InitDB
from mipdb.exceptions import handle_errors, UserInputError
from mipdb.usecases import DisableDataset
from mipdb.usecases import DisableDataModel
from mipdb.usecases import EnableDataset
from mipdb.usecases import EnableDataModel
from mipdb.usecases import ListDataModels
from mipdb.usecases import ListDatasets
from mipdb.usecases import RemovePropertyFromDataModel
from mipdb.usecases import RemovePropertyFromDataset
from mipdb.usecases import UntagDataModel
from mipdb.usecases import TagDataModel
from mipdb.usecases import TagDataset
from mipdb.usecases import UntagDataset
from mipdb.usecases import ValidateDataset


class NotRequiredIf(cl.Option):
    def __init__(self, *args, **kwargs):
        credentials = credentials_from_config()
        option_to_env_var = {
            "--ip": credentials["DB_IP"],
            "--port": credentials["DB_PORT"],
            "--username": credentials["MONETDB_ADMIN_USERNAME"],
            "--password": credentials["MONETDB_LOCAL_PASSWORD"],
            "--db_name": credentials["DB_NAME"],
            "--sqlite_db_path": credentials["SQLITE_DB_PATH"],
        }
        option = args[0][0]
        if option_to_env_var[option]:
            kwargs["required"] = False
            kwargs["default"] = option_to_env_var[option]
        super(NotRequiredIf, self).__init__(*args, **kwargs)


_db_configs_options = [
    cl.option(
        "--ip",
        "ip",
        required=True,
        help="The ip of the database",
        cls=NotRequiredIf,
    ),
    cl.option(
        "--port",
        "port",
        required=True,
        help="The port for the database",
        cls=NotRequiredIf,
    ),
    cl.option(
        "--username",
        "username",
        required=True,
        help="The username for the database",
        cls=NotRequiredIf,
    ),
    cl.option(
        "--password",
        "password",
        required=True,
        help="The password for the database",
        cls=NotRequiredIf,
    ),
    cl.option(
        "--db_name",
        "db_name",
        required=True,
        help="The name of the database",
        cls=NotRequiredIf,
    ),
    cl.option(
        "--sqlite_db_path",
        "sqlite_db_path",
        required=True,
        help="The path for the sqlite database",
        cls=NotRequiredIf,
    ),
]


def get_monetdb_config(ip, port, username, password, db_name):
    try:
        ipaddress.ip_address(ip)
    except ValueError:
        raise UserInputError("Invalid ip provided")

    config = {
        "ip": ip,
        "port": port,
        "dbfarm": db_name,
        "username": username,
        "password": password,
    }
    return config


def db_configs_options(func):
    for option in reversed(_db_configs_options):
        func = option(func)
    return func


@cl.group()
def entry():
    pass


@entry.command()
@cl.argument("file", required=True)
@cl.option(
    "--copy_from_file",
    required=False,
    default=True,
    help="Copy the csvs from the filesystem instead of copying them through sockets."
    "The same files should exist both in the mipdb script and the db.",
)
@db_configs_options
@handle_errors
def load_folder(
    file, copy_from_file, ip, port, username, password, db_name, sqlite_db_path
):
    dbconfig = get_monetdb_config(ip, port, username, password, db_name)
    monetdb = MonetDB.from_config(dbconfig)
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})

    Cleanup(sqlite_db, monetdb).execute()
    if not os.path.exists(file):
        print(f"The path {file} does not exist.")
        return
    elif not os.listdir(file):
        print(f"The directory {file} is empty.")
        return

    for subdir, dirs, files in os.walk(file):
        if dirs:
            continue
        print(f"Data model '{subdir}' is being loaded...")
        metadata_path = os.path.join(subdir, "CDEsMetadata.json")
        reader = JsonFileReader(metadata_path)
        data_model_metadata = reader.read()
        code = data_model_metadata["code"]
        version = data_model_metadata["version"]
        AddDataModel(sqlite_db=sqlite_db, monetdb=monetdb).execute(data_model_metadata)
        print(f"Data model '{code}' was successfully added.")

        for csv_path in glob.glob(subdir + "/*.csv"):
            print(f"CSV '{csv_path}' is being loaded...")
            ValidateDataset(sqlite_db=sqlite_db, monetdb=monetdb).execute(
                csv_path, copy_from_file, code, version
            )
            ImportCSV(sqlite_db=sqlite_db, monetdb=monetdb).execute(
                csv_path, copy_from_file, code, version
            )
            print(f"CSV '{csv_path}' was successfully added.")


@entry.command()
@cl.argument("file", required=True)
@handle_errors
def validate_folder(file):
    if not os.path.exists(file):
        print(f"The path {file} does not exist.")
        return
    elif not os.listdir(file):
        print(f"The directory {file} is empty.")
        return

    for subdir, dirs, files in os.walk(file):
        if dirs:
            continue
        print(f"Data model '{subdir}' is being validated...")
        metadata_path = os.path.join(subdir, "CDEsMetadata.json")
        reader = JsonFileReader(metadata_path)
        data_model_metadata = reader.read()
        code = data_model_metadata["code"]
        ValidateDataModel().execute(data_model_metadata)
        print(f"Data model '{code}' was successfully validated.")

        for csv_path in glob.glob(subdir + "/*.csv"):
            print(f"CSV '{csv_path}' is being validated...")
            ValidateDatasetNoDatabase().execute(csv_path, data_model_metadata)
            print(f"CSV '{csv_path}' was successfully validated.")


@entry.command()
@cl.option(
    "--sqlite_db_path",
    "sqlite_db_path",
    required=True,
    help="The path for the sqlite database",
    cls=NotRequiredIf,
)
@handle_errors
def init(sqlite_db_path):
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    InitDB(db=sqlite_db).execute()
    print("Database initialized")


@entry.command()
@cl.argument("file", required=True)
@db_configs_options
@handle_errors
def add_data_model(file, ip, port, username, password, db_name, sqlite_db_path):
    print(f"Data model '{file}' is being loaded...")
    dbconfig = get_monetdb_config(ip, port, username, password, db_name)
    reader = JsonFileReader(file)
    monetdb = MonetDB.from_config(dbconfig)
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    data_model_metadata = reader.read()
    ValidateDataModel().execute(data_model_metadata)
    AddDataModel(sqlite_db=sqlite_db, monetdb=monetdb).execute(data_model_metadata)
    print(f"Data model '{file}' was successfully added.")


@entry.command()
@cl.argument("csv_path", required=True)
@cl.option(
    "-d",
    "--data-model",
    required=True,
    help="The data model to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The data model version")
@cl.option(
    "--copy_from_file",
    required=False,
    default=True,
    help="Copy the csvs from the filesystem instead of copying them through sockets."
    "The same files should exist both in the mipdb script and the db.",
)
@db_configs_options
@handle_errors
def add_dataset(
    csv_path,
    data_model,
    version,
    copy_from_file,
    ip,
    port,
    username,
    password,
    db_name,
    sqlite_db_path,
):
    print(f"CSV '{csv_path}' is being loaded...")
    dbconfig = get_monetdb_config(ip, port, username, password, db_name)
    monetdb = MonetDB.from_config(dbconfig)
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    ValidateDataset(sqlite_db=sqlite_db, monetdb=monetdb).execute(
        csv_path, copy_from_file, data_model, version
    )
    ImportCSV(sqlite_db=sqlite_db, monetdb=monetdb).execute(
        csv_path, copy_from_file, data_model, version
    )
    print(f"CSV '{csv_path}' was successfully added.")


@entry.command()
@cl.argument("csv_path", required=True)
@cl.option(
    "-d",
    "--data-model",
    required=True,
    help="The data model to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The data model version")
@cl.option(
    "--copy_from_file",
    required=False,
    default=True,
    help="Copy the csvs from the filesystem instead of copying them through sockets."
    "The same files should exist both in the mipdb script and the db.",
)
@db_configs_options
@handle_errors
def validate_dataset(
    csv_path,
    data_model,
    version,
    copy_from_file,
    ip,
    port,
    username,
    password,
    db_name,
    sqlite_db_path,
):
    print(f"Dataset '{csv_path}' is being validated...")
    dbconfig = get_monetdb_config(ip, port, username, password, db_name)
    monetdb = MonetDB.from_config(dbconfig)
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})

    ValidateDataset(sqlite_db=sqlite_db, monetdb=monetdb).execute(
        csv_path, copy_from_file, data_model, version
    )
    print(f"Dataset '{csv_path}' has a valid structure.")


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@cl.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force deletion of dataset that are based on the data model",
)
@db_configs_options
@handle_errors
def delete_data_model(
    name, version, force, ip, port, username, password, db_name, sqlite_db_path
):
    dbconfig = get_monetdb_config(ip, port, username, password, db_name)
    monetdb = MonetDB.from_config(dbconfig)
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    DeleteDataModel(sqlite_db=sqlite_db, monetdb=monetdb).execute(name, version, force)
    print(f"Data model '{name}' was successfully removed.")


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-d",
    "--data-model",
    required=True,
    help="The data model to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The data model version")
@db_configs_options
@handle_errors
def delete_dataset(
    dataset, data_model, version, ip, port, username, password, db_name, sqlite_db_path
):
    dbconfig = get_monetdb_config(ip, port, username, password, db_name)
    monetdb = MonetDB.from_config(dbconfig)
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    DeleteDataset(sqlite_db=sqlite_db, monetdb=monetdb).execute(
        dataset, data_model, version
    )
    print(f"Dataset {dataset} was successfully removed.")


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@cl.option(
    "--sqlite_db_path",
    "sqlite_db_path",
    required=True,
    help="The path for the sqlite database",
    cls=NotRequiredIf,
)
@handle_errors
def enable_data_model(name, version, sqlite_db_path):
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    EnableDataModel(db=sqlite_db).execute(name, version)
    print(f"Data model {name} was successfully enabled.")


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@cl.option(
    "--sqlite_db_path",
    "sqlite_db_path",
    required=True,
    help="The path for the sqlite database",
    cls=NotRequiredIf,
)
@handle_errors
def disable_data_model(name, version, sqlite_db_path):
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    DisableDataModel(db=sqlite_db).execute(name, version)
    print(f"Data model {name} was successfully disabled.")


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-d",
    "--data-model",
    required=True,
    help="The data model to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The data model version")
@cl.option(
    "--sqlite_db_path",
    "sqlite_db_path",
    required=True,
    help="The path for the sqlite database",
    cls=NotRequiredIf,
)
@handle_errors
def enable_dataset(dataset, data_model, version, sqlite_db_path):
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    EnableDataset(db=sqlite_db).execute(dataset, data_model, version)
    print(f"Dataset {dataset} was successfully enabled.")


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-d",
    "--data-model",
    required=True,
    help="The data model to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The data model version")
@cl.option(
    "--sqlite_db_path",
    "sqlite_db_path",
    required=True,
    help="The path for the sqlite database",
    cls=NotRequiredIf,
)
@handle_errors
def disable_dataset(dataset, data_model, version, sqlite_db_path):
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    DisableDataset(sqlite_db).execute(dataset, data_model, version)
    print(f"Dataset {dataset} was successfully disabled.")


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@cl.option(
    "-t",
    "--tag",
    default=None,
    required=True,
    help="A tag to be added/removed",
)
@cl.option(
    "-r",
    "--remove",
    is_flag=True,
    required=False,
    help="A flag that determines if the tag/key_value will be removed",
)
@cl.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force overwrite on property",
)
@cl.option(
    "--sqlite_db_path",
    "sqlite_db_path",
    required=True,
    help="The path for the sqlite database",
    cls=NotRequiredIf,
)
@handle_errors
def tag_data_model(name, version, tag, remove, force, sqlite_db_path):
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    if "=" in tag:
        key, value = tag.split("=")
        if remove:
            RemovePropertyFromDataModel(db=sqlite_db).execute(name, version, key, value)
            print(f"Property was successfully removed from data model {name}.")
        else:
            AddPropertyToDataModel(db=sqlite_db).execute(
                name, version, key, value, force
            )
            print(f"Property was successfully added to data model {name}.")
    else:
        if remove:
            UntagDataModel(db=sqlite_db).execute(name, version, tag)
            print(f"Data model {name} was successfully untagged.")
        else:
            TagDataModel(db=sqlite_db).execute(name, version, tag)
            print(f"Data model {name} was successfully tagged.")


@entry.command()
@cl.argument("dataset", required=True)
@cl.option(
    "-d",
    "--data-model",
    required=True,
    help="The data model to which the dataset is added",
)
@cl.option("-v", "--version", required=True, help="The data model version")
@cl.option(
    "-t",
    "--tag",
    default=None,
    required=True,
    help="A tag to be added/removed",
)
@cl.option(
    "-r",
    "--remove",
    is_flag=True,
    required=False,
    help="A flag that determines if the tag/key_value will be removed",
)
@cl.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force overwrite on property",
)
@cl.option(
    "--sqlite_db_path",
    "sqlite_db_path",
    required=True,
    help="The path for the sqlite database",
    cls=NotRequiredIf,
)
@handle_errors
def tag_dataset(
    dataset,
    data_model,
    version,
    tag,
    remove,
    force,
    sqlite_db_path,
):
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})

    if "=" in tag:
        key, value = tag.split("=")
        if remove:
            RemovePropertyFromDataset(db=sqlite_db).execute(
                dataset, data_model, version, key, value
            )
            print(f"Property was successfully removed from dataset {dataset}.")
        else:
            AddPropertyToDataset(db=sqlite_db).execute(
                dataset, data_model, version, key, value, force
            )
            print(f"Property was successfully added to dataset {dataset}.")
    else:
        if remove:
            UntagDataset(db=sqlite_db).execute(dataset, data_model, version, tag)
            print(f"Dataset {dataset} was successfully untagged.")
        else:
            TagDataset(db=sqlite_db).execute(dataset, data_model, version, tag)
            print(f"Dataset {dataset} was successfully tagged.")


@entry.command()
@cl.option(
    "--sqlite_db_path",
    "sqlite_db_path",
    required=True,
    help="The path for the sqlite database",
    cls=NotRequiredIf,
)
@handle_errors
def list_data_models(sqlite_db_path):
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    ListDataModels(db=sqlite_db).execute()


@entry.command()
@db_configs_options
@handle_errors
def list_datasets(ip, port, username, password, db_name, sqlite_db_path):
    dbconfig = get_monetdb_config(ip, port, username, password, db_name)
    monetdb = MonetDB.from_config(dbconfig)
    sqlite_db = SQLiteDB.from_config({"db_path": sqlite_db_path})
    ListDatasets(sqlite_db=sqlite_db, monetdb=monetdb).execute()
