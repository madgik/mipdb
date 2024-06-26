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


def flatten_cdes(schema_data):
    cdes = []

    if "variables" in schema_data:
        for metadata in schema_data["variables"]:
            metadata = reformat_metadata(metadata)
            cdes += [CommonDataElement.from_metadata(metadata)]
    if "groups" in schema_data:
        cdes += [
            metadata
            for group_data in schema_data["groups"]
            for metadata in flatten_cdes(group_data)
        ]
    return cdes


def get_sql_type_per_column(cdes):
    return {code: json.loads(cde.metadata)["sql_type"] for code, cde in cdes.items()}


def get_cdes_with_min_max(cdes, columns):
    cdes_with_min_max = {}
    for code, cde in cdes.items():
        if code not in columns:
            continue
        metadata = json.loads(cde.metadata)
        max_value = metadata["max"] if "max" in metadata else None
        min_value = metadata["min"] if "min" in metadata else None
        if code in columns and min_value or max_value:
            cdes_with_min_max[code] = (min_value, max_value)
    return cdes_with_min_max


def get_cdes_with_enumerations(cdes, columns):
    return {
        code: [
            enum_code
            for enum_code, enum_label in json.loads(cde.metadata)[
                "enumerations"
            ].items()
        ]
        for code, cde in cdes.items()
        if json.loads(cde.metadata)["is_categorical"] and code in columns
    }


def get_dataset_enums(cdes):
    return json.loads(cdes["dataset"].metadata)["enumerations"]


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


def validate_longitudinal_data_model(cdes):
    subject_id_metadata = None
    visit_id_metadata = None

    for cde in cdes:
        if cde.code == "subjectid":
            subject_id_metadata = json.loads(cde.metadata)
        elif cde.code == "visitid":
            visit_id_metadata = json.loads(cde.metadata)

    if not subject_id_metadata:
        raise InvalidDataModelError(
            "There is no 'subjectid' CDE in the longitudinal data model."
        )
    if not visit_id_metadata:
        raise InvalidDataModelError(
            "There is no 'visitid' CDE in the longitudinal data model."
        )

    validate_visitid_cde(visit_id_metadata)


def validate_visitid_cde(metadata):
    if not metadata["is_categorical"]:
        raise InvalidDataModelError(
            "CDE 'visitid' must have the 'isCategorical' property equal to 'true'."
        )
    if metadata["sql_type"] != "text":
        raise InvalidDataModelError(
            "CDE 'visitid' must have the 'sql_type' property equal to 'text'."
        )
    if "enumerations" not in metadata:
        raise InvalidDataModelError(
            "CDE 'visitid' must contain the 'enumerations' property."
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
    for element in ["is_categorical", "code", "sql_type", "label", "type"]:
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

    valid_metadata_types = ["nominal", "real", "integer", "text"]

    if metadata["type"] not in valid_metadata_types:
        raise InvalidDataModelError(
            f"The CDE {code} has an 'type' the only valid types are:{valid_metadata_types} "
        )
