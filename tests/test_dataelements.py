from mipdb.dataelements import (
    make_cdes,
    make_cde,
    CommonDataElement,
    CategoricalCDE,
    NumericalCDE,
)


def test_flatten_cdes_empty():
    root = []
    cdes = make_cdes(root)
    assert cdes == []


def test_flatten_cdes(schema_data):
    cdes = make_cdes(schema_data)
    assert all(isinstance(cde, CommonDataElement) for cde in cdes)
    assert len(cdes) == 4


def test_make_cde():
    cde_data = {
        "isCategorical": False,
        "code": "code",
        "sql_type": "text",
        "description": "",
        "label": "",
        "methodology": "",
    }
    cde = make_cde(cde_data)
    assert isinstance(cde, CommonDataElement)


def test_make_categorical_cde():
    cde_data = {
        "isCategorical": True,
        "code": "code",
        "sql_type": "text",
        "description": "",
        "enumerations": [
            {"code": "level1", "label": "Level 1"},
            {"code": "level2", "label": "Level 2"},
        ],
        "label": "",
        "methodology": "",
    }
    cde = make_cde(cde_data)
    assert isinstance(cde, CategoricalCDE)
    assert hasattr(cde, "enumerations")


def test_make_numerical_cde_with_domain():
    cde_data = {
        "isCategorical": False,
        "code": "code",
        "sql_type": "real",
        "description": "",
        "label": "",
        "methodology": "",
        "minValue": 0,
        "maxValue": 100,
    }
    cde = make_cde(cde_data)
    assert isinstance(cde, NumericalCDE)
    assert hasattr(cde, "minValue")
    assert hasattr(cde, "maxValue")


def test_make_numerical_cde_with_units():
    cde_data = {
        "isCategorical": False,
        "code": "code",
        "sql_type": "real",
        "description": "",
        "label": "",
        "methodology": "",
        "units": "yards",
    }
    cde = make_cde(cde_data)
    assert isinstance(cde, NumericalCDE)
    assert hasattr(cde, "units")


def test_make_numerical_cde_with_empty_units():
    cde_data = {
        "isCategorical": False,
        "code": "code",
        "sql_type": "real",
        "description": "",
        "label": "",
        "methodology": "",
        "units": "",
    }
    cde = make_cde(cde_data)
    assert isinstance(cde, NumericalCDE)
    assert cde.units is None


def test_make_numerical_cde_with_empty_units():
    cde_data = {
        "isCategorical": False,
        "code": "string_domain",
        "sql_type": "real",
        "description": "",
        "label": "5",
        "methodology": "chilin",
        "minValue": "0",
        "maxValue": "100",
    }
    cde = make_cde(cde_data)
    assert isinstance(cde, NumericalCDE)
    assert cde.minValue == 0
    assert cde.maxValue == 100
