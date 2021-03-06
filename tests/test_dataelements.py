import pytest

from mipdb.dataelements import make_cdes, CommonDataElement
from mipdb.exceptions import InvalidDataModelError


def test_make_cdes_empty():
    root = []
    cdes = make_cdes(root)
    assert cdes == []


def test_make_cdes(data_model_metadata):
    cdes = make_cdes(data_model_metadata)
    assert all(isinstance(cde, CommonDataElement) for cde in cdes)
    assert len(cdes) == 5


def test_make_cdes_full_schema(data_model_metadata):
    cdes = make_cdes(data_model_metadata)
    assert all(isinstance(cde, CommonDataElement) for cde in cdes)


def test_make_cde():
    metadata = {
        "is_categorical": False,
        "code": "code",
        "sql_type": "text",
        "description": "",
        "label": "",
        "methodology": "",
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
