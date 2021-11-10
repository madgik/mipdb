import json
from typing import List, Optional
from dataclasses import dataclass, fields


@dataclass
class CommonDataElement:
    code: str
    metadata: str

    @classmethod
    def from_cde_data(cls, cde_data):
        code = cde_data["code"]

        metadata = json.dumps(cde_data)
        return cls(
            code,
            metadata,
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
