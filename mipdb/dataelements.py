import json
from typing import List, Optional
from dataclasses import dataclass, fields


@dataclass
class CommonDataElement:
    code: str
    sql_type: str
    metadata: str
    is_categorical: bool
    enumerations: Optional[dict]
    min_value: Optional[float]
    max_value: Optional[float]

    @classmethod
    def from_cde_data(cls, cde_data):
        code = cde_data["code"]
        sql_type = cde_data["sql_type"]
        is_categorical = cde_data["isCategorical"]
        enumerations = cde_data.get("enumerations", None)
        min_value = cde_data.get("minValue", None)
        max_value = cde_data.get("maxValue", None)

        metadata = json.dumps(cde_data)
        return cls(
            code,
            sql_type,
            metadata,
            is_categorical,
            enumerations,
            min_value,
            max_value,
        )


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
