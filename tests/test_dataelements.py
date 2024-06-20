import pytest

from mipdb.dataelements import (
    flatten_cdes,
    CommonDataElement,
    validate_dataset_present_on_cdes_with_proper_format,
    validate_longitudinal_data_model,
    validate_metadata,
)
from mipdb.exceptions import InvalidDataModelError


def test_dataelements_type_is_not_valid():
    with pytest.raises(InvalidDataModelError):
        metadata = {
            "code": "dataset",
            "sql_type": "int",
            "description": "",
            "enumerations": {
                "dataset": "Dataset",
                "dataset1": "Dataset 1",
                "dataset2": "Dataset 2",
            },
            "label": "Dataset",
            "methodology": "",
            "is_categorical": True,
        }
        validate_metadata("dataset", metadata)


def test_make_cdes_empty():
    root = []
    cdes = flatten_cdes(root)
    assert cdes == []


def test_make_cdes(data_model_metadata):
    cdes = flatten_cdes(data_model_metadata)
    assert all(isinstance(cde, CommonDataElement) for cde in cdes)
    assert len(cdes) == 6


def test_validate_dataset_present_on_cdes_with_proper_format(data_model_metadata):
    cdes = flatten_cdes(data_model_metadata)
    validate_dataset_present_on_cdes_with_proper_format(cdes)


def test_validate_dataset_is_not_present_on_cdes(data_model_metadata):
    cdes = flatten_cdes(data_model_metadata)
    cdes = [cde for cde in cdes if cde.code != "dataset"]
    with pytest.raises(InvalidDataModelError):
        validate_dataset_present_on_cdes_with_proper_format(cdes)


def test_validate_dataset_is_present_on_cdes_with_invalid_sql_type(data_model_metadata):
    cdes = [
        CommonDataElement(
            code="dataset",
            metadata='{"code": "dataset", "sql_type": "int", "description": "", "enumerations": {"dataset": "Dataset", "dataset1": "Dataset 1", "dataset2": "Dataset 2"}, "label": "Dataset", "methodology": "", "is_categorical": true}',
        )
    ]
    with pytest.raises(InvalidDataModelError):
        validate_dataset_present_on_cdes_with_proper_format(cdes)


def test_validate_dataset_is_present_on_cdes_with_invalid_is_categorical(
    data_model_metadata,
):
    cdes = [
        CommonDataElement(
            code="dataset",
            metadata='{"code": "dataset", "sql_type": "text", "description": "", "enumerations": {"dataset": "Dataset", "dataset1": "Dataset 1", "dataset2": "Dataset 2"}, "label": "Dataset", "methodology": "", "is_categorical": false}',
        )
    ]
    with pytest.raises(InvalidDataModelError):
        validate_dataset_present_on_cdes_with_proper_format(cdes)


def test_make_cdes_full_schema(data_model_metadata):
    cdes = flatten_cdes(data_model_metadata)
    assert all(isinstance(cde, CommonDataElement) for cde in cdes)


def test_make_cde():
    metadata = {
        "is_categorical": False,
        "code": "code",
        "sql_type": "text",
        "description": "",
        "label": "",
        "methodology": "",
        "type": "nominal",
    }
    cde = CommonDataElement.from_metadata(metadata)
    assert hasattr(cde, "code")
    assert hasattr(cde, "metadata")


def test_missing_nessesary_variables():
    metadata = {
        "is_categorical": False,
        "code": "code",
        "sql_type": "text",
        "description": "",
        "methodology": "",
    }
    with pytest.raises(InvalidDataModelError):
        CommonDataElement.from_metadata(metadata)


def test_min_greater_than_max():
    metadata = {
        "is_categorical": False,
        "code": "code",
        "sql_type": "text",
        "label": "",
        "min": 55,
        "max": 50,
        "description": "",
        "methodology": "",
    }
    with pytest.raises(InvalidDataModelError):
        CommonDataElement.from_metadata(metadata)


def test_is_categorical_without_enumerations():
    metadata = {
        "is_categorical": True,
        "code": "code",
        "sql_type": "text",
        "description": "",
        "label": "label",
        "methodology": "",
    }
    with pytest.raises(InvalidDataModelError):
        CommonDataElement.from_metadata(metadata)


@pytest.mark.parametrize(
    "cdes",
    [
        pytest.param(
            [],
            id="subjectid and visitid are missing",
        ),
        pytest.param(
            [
                CommonDataElement(
                    code="visitid",
                    metadata='{"code": "visitid", "sql_type": "text", "description": "", "enumerations": {"BL": "Base line", "FL1": "Follow up 1", "FL2": "Follow up 2"}, "label": "visitid", "methodology": "", "is_categorical": true}',
                )
            ],
            id="subjectid is missing",
        ),
        pytest.param(
            [
                CommonDataElement(
                    code="subjectid",
                    metadata='{"code": "subjectid", "sql_type": "text", "description": "", "label": "subjectid", "methodology": "", "is_categorical": false}',
                )
            ],
            id="visitid is missing",
        ),
        pytest.param(
            [
                CommonDataElement(
                    code="subjectid",
                    metadata='{"code": "subjectid", "sql_type": "text", "description": "", "label": "subjectid", "methodology": "", "is_categorical": false}',
                ),
                CommonDataElement(
                    code="visitid",
                    metadata='{"code": "visitid", "sql_type": "text", "description": "", "label": "visitid", "methodology": "", "is_categorical": false}',
                ),
            ],
            id="visitid is not categorical",
        ),
        pytest.param(
            [
                CommonDataElement(
                    code="subjectid",
                    metadata='{"code": "subjectid", "sql_type": "text", "description": "", "label": "subjectid", "methodology": "", "is_categorical": false}',
                ),
                CommonDataElement(
                    code="visitid",
                    metadata='{"code": "visitid", "sql_type": "int", "description": "", "enumerations": {"BL": "Base line", "FL1": "Follow up 1", "FL2": "Follow up 2"}, "label": "visitid", "methodology": "", "is_categorical": true}',
                ),
            ],
            id="visitid's sql type is not 'text'",
        ),
        pytest.param(
            [
                CommonDataElement(
                    code="subjectid",
                    metadata='{"code": "subjectid", "sql_type": "text", "description": "", "label": "subjectid", "methodology": "", "is_categorical": false}',
                ),
                CommonDataElement(
                    code="visitid",
                    metadata='{"code": "visitid", "sql_type": "text", "description": "", "label": "visitid", "methodology": "", "is_categorical": true}',
                ),
            ],
            id="visitid is categorical but it does not contain enumerations",
        ),
    ],
)
def test_validate_longitudinal_data_model(cdes):
    with pytest.raises(InvalidDataModelError):
        validate_longitudinal_data_model(cdes)
