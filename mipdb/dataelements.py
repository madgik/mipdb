from typing import List, Optional
from dataclasses import dataclass, fields


@dataclass
class CommonDataElement:
    code: str
    label: str
    sql_type: str
    description: str
    methodology: str


@dataclass
class CategoricalCDE(CommonDataElement):
    enumerations: List[dict]


@dataclass
class NumericalCDE(CommonDataElement):
    minValue: Optional[float] = None
    maxValue: Optional[float] = None
    units: Optional[str] = None


def make_cdes(schema_data):
    cdes = []
    if "variables" in schema_data:
        cdes += [make_cde(cde_data) for cde_data in schema_data["variables"]]
    if "groups" in schema_data:
        cdes += [
            cde_data
            for group_data in schema_data["groups"]
            for cde_data in make_cdes(group_data)
        ]
    return cdes


def make_cde(cde_data):
    if is_categorical(cde_data):
        fields_ = [field.name for field in fields(CategoricalCDE)]
        args = {key: val for key, val in cde_data.items() if key in fields_}
        return CategoricalCDE(**args)
    if is_numerical(cde_data):
        fields_ = [field.name for field in fields(NumericalCDE)]
        args = {key: val for key, val in cde_data.items() if key in fields_}
        # Some numerical CDEs don't have minValue, maxValue fields
        # Some minValue, maxValue fields are strings
        # Some units are empty strings
        args["minValue"] = float(args["minValue"]) if "minValue" in args else None
        args["maxValue"] = float(args["maxValue"]) if "maxValue" in args else None
        args["units"] = args.get("units", None) or None
        return NumericalCDE(**args)
    fields_ = [field.name for field in fields(CommonDataElement)]
    args = {key: val for key, val in cde_data.items() if key in fields_}
    return CommonDataElement(**args)


def is_categorical(cde_data):
    return cde_data["isCategorical"]


def is_numerical(cde_data):
    return not cde_data["isCategorical"] and cde_data["sql_type"] in ("int", "real")
