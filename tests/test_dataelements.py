import pytest

from mipdb.dataelements import make_cdes, CommonDataElement
from mipdb.exceptions import InvalidDatasetError
from mipdb.exceptions import UserInputError


def test_make_cdes_empty():
    root = []
    cdes = make_cdes(root)
    assert cdes == []


def test_make_cdes(data_model_data):
    cdes = make_cdes(data_model_data)
    assert all(isinstance(cde, CommonDataElement) for cde in cdes)
    assert len(cdes) == 5


def test_make_cdes_full_schema(data_model_data):
    cdes = make_cdes(data_model_data)
    assert all(isinstance(cde, CommonDataElement) for cde in cdes)


def test_make_cde():
    cde_data = {
        "isCategorical": False,
        "code": "code",
        "sql_type": "text",
        "description": "",
        "label": "",
        "methodology": "",
    }
    cde = CommonDataElement.from_cde_data(cde_data)
    assert hasattr(cde, "code")
    assert hasattr(cde, "metadata")


def test_missing_nessesary_variables():
    cde_data = {
        "isCategorical": False,
        "code": "code",
        "sql_type": "text",
        "description": "",
        "methodology": "",
    }
    with pytest.raises(InvalidDatasetError):
        CommonDataElement.from_cde_data(cde_data)


def test_is_categorical_without_enumerations():
    cde_data = {
        "isCategorical": True,
        "code": "code",
        "sql_type": "text",
        "description": "",
        "label": "label",
        "methodology": "",
    }
    with pytest.raises(InvalidDatasetError):
        CommonDataElement.from_cde_data(cde_data)
