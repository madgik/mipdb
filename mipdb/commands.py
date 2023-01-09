import click as cl
import os
import glob

from mipdb.database import MonetDB, get_db_config
from mipdb.reader import JsonFileReader
from mipdb.usecases import AddDataModel, Cleanup
from mipdb.usecases import AddPropertyToDataModel
from mipdb.usecases import AddPropertyToDataset
from mipdb.usecases import DeleteDataModel
from mipdb.usecases import DeleteDataset
from mipdb.usecases import ImportCSV
from mipdb.usecases import InitDB
from mipdb.exceptions import handle_errors
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

_ip_port_options = [
    cl.option("--ip", "ip", required=False, help="The ip of the database"),
    cl.option("--port", "port", required=False, help="The port of the database"),
]


def ip_port_options(func):
    for option in reversed(_ip_port_options):
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
@ip_port_options
@handle_errors
def load_folder(file, copy_from_file, ip, port):
    dbconfig = get_db_config(ip, port)
    db = MonetDB.from_config(dbconfig)

    Cleanup(db).execute()

    for subdir, dirs, files in os.walk(file):
        if dirs:
            continue
        print(f"Data model '{subdir}' is being loaded...")
        metadata_path = os.path.join(subdir, "CDEsMetadata.json")
        reader = JsonFileReader(metadata_path)
        data_model_metadata = reader.read()
        code = data_model_metadata["code"]
        version = data_model_metadata["version"]
        AddDataModel(db).execute(data_model_metadata)
        print(f"Data model '{code}' was successfully added.")

        for csv_path in glob.glob(subdir + "/*.csv"):
            print(f"CSV '{csv_path}' is being loaded...")
            ValidateDataset(db).execute(csv_path, copy_from_file, code, version)
            ImportCSV(db).execute(csv_path, copy_from_file, code, version)
            print(f"CSV '{csv_path}' was successfully added.")


@entry.command()
@ip_port_options
@handle_errors
def init(ip, port):
    dbconfig = get_db_config(ip, port)
    db = MonetDB.from_config(dbconfig)
    InitDB(db).execute()
    print("Database initialized")


@entry.command()
@cl.argument("file", required=True)
@ip_port_options
@handle_errors
def add_data_model(file, ip, port):
    print(f"Data model '{file}' is being loaded...")
    dbconfig = get_db_config(ip, port)
    reader = JsonFileReader(file)
    db = MonetDB.from_config(dbconfig)
    data_model_metadata = reader.read()
    AddDataModel(db).execute(data_model_metadata)
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
@ip_port_options
@handle_errors
def add_dataset(csv_path, data_model, version, copy_from_file, ip, port):
    print(f"CSV '{csv_path}' is being loaded...")
    dbconfig = get_db_config(ip, port)
    db = MonetDB.from_config(dbconfig)
    ValidateDataset(db).execute(csv_path, copy_from_file, data_model, version)
    ImportCSV(db).execute(csv_path, copy_from_file, data_model, version)
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
@ip_port_options
@handle_errors
def validate_dataset(csv_path, data_model, version, copy_from_file, ip, port):
    print(f"Dataset '{csv_path}' is being validated...")
    dbconfig = get_db_config(ip, port)
    db = MonetDB.from_config(dbconfig)
    ValidateDataset(db).execute(csv_path, copy_from_file, data_model, version)
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
@ip_port_options
@handle_errors
def delete_data_model(name, version, force, ip, port):
    db = MonetDB.from_config(get_db_config(ip, port))
    DeleteDataModel(db).execute(name, version, force)
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
@ip_port_options
@handle_errors
def delete_dataset(dataset, data_model, version, ip, port):
    db = MonetDB.from_config(get_db_config(ip, port))
    DeleteDataset(db).execute(dataset, data_model, version)
    print(f"Dataset {dataset} was successfully removed.")


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@ip_port_options
@handle_errors
def enable_data_model(name, version, ip, port):
    db = MonetDB.from_config(get_db_config(ip, port))
    EnableDataModel(db).execute(name, version)
    print(f"Data model {name} was successfully enabled.")


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@ip_port_options
@handle_errors
def disable_data_model(name, version, ip, port):
    db = MonetDB.from_config(get_db_config(ip, port))
    DisableDataModel(db).execute(name, version)
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
@ip_port_options
@handle_errors
def enable_dataset(dataset, data_model, version, ip, port):
    db = MonetDB.from_config(get_db_config(ip, port))
    EnableDataset(db).execute(dataset, data_model, version)
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
@ip_port_options
@handle_errors
def disable_dataset(dataset, data_model, version, ip, port):
    db = MonetDB.from_config(get_db_config(ip, port))
    DisableDataset(db).execute(dataset, data_model, version)
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
@ip_port_options
@handle_errors
def tag_data_model(name, version, tag, remove, force, ip, port):
    db = MonetDB.from_config(get_db_config(ip, port))
    if "=" in tag:
        key, value = tag.split("=")
        if remove:
            RemovePropertyFromDataModel(db).execute(name, version, key, value)
            print(f"Property was successfully removed from data model {name}.")
        else:
            AddPropertyToDataModel(db).execute(name, version, key, value, force)
            print(f"Property was successfully added to data model {name}.")
    else:
        if remove:
            UntagDataModel(db).execute(name, version, tag)
            print(f"Data model {name} was successfully untagged.")
        else:
            TagDataModel(db).execute(name, version, tag)
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
@ip_port_options
@handle_errors
def tag_dataset(dataset, data_model, version, tag, remove, force, ip, port):
    db = MonetDB.from_config(get_db_config(ip, port))
    if "=" in tag:
        key, value = tag.split("=")
        if remove:
            RemovePropertyFromDataset(db).execute(
                dataset, data_model, version, key, value
            )
            print(f"Property was successfully removed from dataset {dataset}.")
        else:
            AddPropertyToDataset(db).execute(
                dataset, data_model, version, key, value, force
            )
            print(f"Property was successfully added to dataset {dataset}.")
    else:
        if remove:
            UntagDataset(db).execute(dataset, data_model, version, tag)
            print(f"Dataset {dataset} was successfully untagged.")
        else:
            TagDataset(db).execute(dataset, data_model, version, tag)
            print(f"Dataset {dataset} was successfully tagged.")


@entry.command()
@ip_port_options
@handle_errors
def list_data_models(ip, port):
    db = MonetDB.from_config(get_db_config(ip, port))
    ListDataModels(db).execute()


@entry.command()
@ip_port_options
@handle_errors
def list_datasets(ip, port):
    db = MonetDB.from_config(get_db_config(ip, port))
    ListDatasets(db).execute()
