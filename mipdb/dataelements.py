import json
from dataclasses import dataclass

from mipdb.exceptions import InvalidDataModelError


@dataclass
class CommonDataElement:
    code: str
    metadata: str

    @classmethod
    def from_metadata(cls, metadata):
        code = metadata["code"]
        validate_metadata(code, metadata)
        metadata = json.dumps(metadata)

        return cls(
            code,
            metadata,
        )


def get_system_column_metadata():
    subjectcode = {
        "is_categorical": False,
        "label": "subjectcode",
        "code": "subjectcode",
        "sql_type": "text",
    }
    return CommonDataElement.from_metadata(subjectcode)


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
