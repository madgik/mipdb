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
        for element in ["isCategorical", "code", "sql_type", "label"]:
            if element not in cde_data:
                raise InvalidDataModelError(
                    f"Element: {element} is missing from the CDE {code}"
                )
        if cde_data["isCategorical"] and "enumerations" not in cde_data:
            raise InvalidDataModelError(
                f"The CDE {code} has 'isCategorical' set to True but there are no enumerations."
            )
        if {"minValue", "maxValue"} < set(cde_data) and cde_data[
            "minValue"
        ] >= cde_data["maxValue"]:
            raise InvalidDataModelError(
                f"The CDE {code} has minValue greater than the maxValue."
            )

        metadata = json.dumps(cde_data)
        return cls(
            code,
            metadata,
        )


def get_system_columns_metadata():
    row_id = {
        "isCategorical": False,
        "label": "row_id",
        "code": "row_id",
        "sql_type": "text",
    }
    subjectcode = {
        "isCategorical": False,
        "label": "subjectcode",
        "code": "subjectcode",
        "sql_type": "text",
    }
    return [CommonDataElement.from_cde_data(row_id), CommonDataElement.from_cde_data(subjectcode)]


def make_cdes(schema_data):
    cdes = []

    if "variables" in schema_data:
        cdes += [
            CommonDataElement.from_cde_data(cde_data)
            for cde_data in schema_data["variables"]
        ]
    if "groups" in schema_data:
        cdes += [
            cde_data
            for group_data in schema_data["groups"]
            for cde_data in make_cdes(group_data)
        ]
    return cdes
