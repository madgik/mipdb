import json
from dataclasses import dataclass

from mipdb.exceptions import InvalidDataModelError, UserInputError


@dataclass
class CommonDataElement:
    code: str
    metadata: str

    @classmethod
    def from_metadata(cls, metadata):
        code = metadata["code"]
        if not code.isidentifier():
            raise UserInputError(f"CDE: {code} is not a valid python identifier")

        validate_metadata(code, metadata)
        metadata = json.dumps(metadata)

        return cls(
            code,
            metadata,
        )

    def get_enumerations(self):
        metadata = json.loads(self.metadata)
        return metadata["enumerations"] if "enumerations" in metadata else []


def make_cdes(schema_data):
    cdes = []

    if "variables" in schema_data:
        for metadata in schema_data["variables"]:
            metadata = reformat_metadata(metadata)
            cdes += [CommonDataElement.from_metadata(metadata)]
    if "groups" in schema_data:
        cdes += [
            metadata
            for group_data in schema_data["groups"]
            for metadata in make_cdes(group_data)
        ]
    return cdes


def validate_dataset_present_on_cdes_with_proper_format(cdes):
    dataset_cde = [cde for cde in cdes if cde.code == "dataset"]
    if not dataset_cde:
        raise InvalidDataModelError("There is no 'dataset' CDE in the data model.")
    dataset_metadata = json.loads(dataset_cde[0].metadata)
    if not dataset_metadata["is_categorical"]:
        raise InvalidDataModelError(
            "CDE 'dataset' must have the 'isCategorical' property equal to 'true'."
        )
    if dataset_metadata["sql_type"] != "text":
        raise InvalidDataModelError(
            "CDE 'dataset' must have the 'sql_type' property equal to 'text'."
        )


def reformat_metadata(metadata):
    new_key_assign = {
        "isCategorical": "is_categorical",
        "minValue": "min",
        "maxValue": "max",
    }
    for old_key, new_key in new_key_assign.items():
        if old_key in metadata:
            metadata[new_key] = metadata.pop(old_key)

    if "enumerations" in metadata:
        metadata["enumerations"] = {
            enumeration["code"]: enumeration["label"]
            for enumeration in metadata["enumerations"]
        }
    return metadata


def validate_metadata(code, metadata):
    for element in ["is_categorical", "code", "sql_type", "label"]:
        if element not in metadata:
            raise InvalidDataModelError(
                f"Element: {element} is missing from the CDE {code}"
            )
    if metadata["is_categorical"] and "enumerations" not in metadata:
        raise InvalidDataModelError(
            f"The CDE {code} has 'is_categorical' set to True but there are no enumerations."
        )
    if {"min", "max"} < set(metadata) and metadata["min"] >= metadata["max"]:
        raise InvalidDataModelError(f"The CDE {code} has min greater than the max.")
