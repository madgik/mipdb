import uuid

import click as cl
import os
import glob

import pandas

from mipdb.database import MonetDB, get_db_config
from mipdb.reader import CSVFileReader, JsonFileReader
from mipdb.usecases import AddDataModel
from mipdb.usecases import AddPropertyToDataModel
from mipdb.usecases import AddPropertyToDataset
from mipdb.usecases import DeleteDataModel
from mipdb.usecases import DeleteDataset
from mipdb.usecases import AddDataset
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


@cl.group()
def entry():
    pass


@entry.command()
@cl.argument("file", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@handle_errors
def load_folder(file, version):
    for subdir, dirs, files in os.walk(file):
        if dirs:
            continue
        print(f"Data model {subdir} is being loaded...")
        dbconfig = get_db_config()
        db = MonetDB.from_config(dbconfig)
        reader = JsonFileReader(subdir + "/CDEsMetadata.json")
        data_model_data = reader.read()
        data_model_data["version"] = version
        AddDataModel(db).execute(data_model_data)
        print(
            f"Data model {os.path.basename(os.path.normpath(subdir))} was successfully added."
        )

        for csv in glob.glob(subdir + "/*.csv"):
            print(f"Dataset {csv} is being loaded...")
            dbconfig = get_db_config()
            db = MonetDB.from_config(dbconfig)
            reader = CSVFileReader(csv)
            dataset_data = reader.read()
            data_model = os.path.basename(os.path.normpath(subdir))
            ValidateDataset(db).execute(dataset_data, data_model, version)
            AddDataset(db).execute(dataset_data, data_model, version)
            print(
                f"Dataset {os.path.basename(os.path.normpath(csv))} was successfully added."
            )


@entry.command()
@handle_errors
def init():
    dbconfig = get_db_config()
    db = MonetDB.from_config(dbconfig)
    InitDB(db).execute()


@entry.command()
@cl.argument("file", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
# @cl.option("--dry-run", is_flag=True)
@handle_errors
def add_data_model(file, version):
    print(f"Data model {file} is being loaded...")
    dbconfig = get_db_config()
    reader = JsonFileReader(file)
    db = MonetDB.from_config(dbconfig)
    data_model_data = reader.read()
    data_model_data["version"] = version
    AddDataModel(db).execute(data_model_data)
    print(
        f"Data model {os.path.basename(os.path.normpath(file))} was successfully added."
    )


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
    print(f"Dataset {file} is being loaded...")
    reader = CSVFileReader(file)
    dbconfig = get_db_config()
    db = MonetDB.from_config(dbconfig)
    dataset_data = reader.read()
    ValidateDataset(db).execute(dataset_data, data_model, version)
    AddDataset(db).execute(dataset_data, data_model, version)
    print(f"Dataset {os.path.basename(os.path.normpath(file))} was successfully added.")


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
def validate_dataset(file, data_model, version):
    reader = CSVFileReader(file)
    dbconfig = get_db_config()
    db = MonetDB.from_config(dbconfig)
    dataset_data = reader.read()
    ValidateDataset(db).execute(dataset_data, data_model, version)
    print(
        f"Dataset {os.path.basename(os.path.normpath(file))} was successfully validated."
    )


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
    print(f"Data model {name} was successfully removed.")


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
def delete_dataset(dataset, data_model, version):
    db = MonetDB.from_config(get_db_config())
    DeleteDataset(db).execute(dataset, data_model, version)
    print(f"Dataset {dataset} was successfully removed.")


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@handle_errors
def enable_data_model(name, version):
    db = MonetDB.from_config(get_db_config())
    EnableDataModel(db).execute(name, version)
    print(f"Data model {name} was successfully enabled.")


@entry.command()
@cl.argument("name", required=True)
@cl.option("-v", "--version", required=True, help="The data model version")
@handle_errors
def disable_data_model(name, version):
    db = MonetDB.from_config(get_db_config())
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
@handle_errors
def enable_dataset(dataset, data_model, version):
    db = MonetDB.from_config(get_db_config())
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
@handle_errors
def disable_dataset(dataset, data_model, version):
    db = MonetDB.from_config(get_db_config())
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
@handle_errors
def tag_data_model(name, version, tag, remove, force):
    db = MonetDB.from_config(get_db_config())
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
@handle_errors
def tag_dataset(dataset, data_model, version, tag, remove, force):
    db = MonetDB.from_config(get_db_config())
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
@handle_errors
def list_data_models():
    db = MonetDB.from_config(get_db_config())
    ListDataModels(db).execute()


@entry.command()
@handle_errors
def list_datasets():
    db = MonetDB.from_config(get_db_config())
    ListDatasets(db).execute()
