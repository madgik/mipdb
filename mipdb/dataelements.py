import json
from dataclasses import dataclass
from typing import List

from mipdb.exceptions import InvalidDataModelError


@dataclass
class CommonDataElement:
    code: str
    metadata: str

    @classmethod
    def from_cde_data(cls, cde_data):
        code = cde_data["code"]
        validate_cde_data(code, cde_data)
        metadata = json.dumps(cde_data)

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
    return CommonDataElement.from_cde_data(subjectcode)


def make_cdes(schema_data):
    cdes = []

    if "variables" in schema_data:
        for cde_data in schema_data["variables"]:
            cde_data = reformat_cde_data(cde_data)
            cdes += [CommonDataElement.from_cde_data(cde_data)]
    if "groups" in schema_data:
        cdes += [
            cde_data
            for group_data in schema_data["groups"]
            for cde_data in make_cdes(group_data)
        ]
    return cdes


def reformat_cde_data(cde_data):
    new_key_assign = {
        "isCategorical": "is_categorical",
        "minValue": "min",
        "maxValue": "max",
    }
    for old_key, new_key in new_key_assign.items():
        if old_key in cde_data:
            cde_data[new_key] = cde_data.pop(old_key)

    if "enumerations" in cde_data:
        print(type(cde_data["enumerations"]))
        cde_data["enumerations"] = [
            {enumeration["code"]: enumeration["label"]}
            for enumeration in cde_data["enumerations"]
        ]
    return cde_data


def validate_cde_data(code, cde_data):
    for element in ["is_categorical", "code", "sql_type", "label"]:
        if element not in cde_data:
            raise InvalidDataModelError(
                f"Element: {element} is missing from the CDE {code}"
            )
    if cde_data["is_categorical"] and "enumerations" not in cde_data:
        raise InvalidDataModelError(
            f"The CDE {code} has 'is_categorical' set to True but there are no enumerations."
        )
    if {"min", "max"} < set(cde_data) and cde_data["min"] >= cde_data["max"]:
        raise InvalidDataModelError(f"The CDE {code} has min greater than the max.")
