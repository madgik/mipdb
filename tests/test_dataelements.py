from mipdb.dataelements import make_cdes, CommonDataElement


def test_make_cdes_empty():
    root = []
    cdes = make_cdes(root)
    assert cdes == []


def test_make_cdes(schema_data):
    cdes = make_cdes(schema_data)
    assert all(isinstance(cde, CommonDataElement) for cde in cdes)
    assert len(cdes) == 4


def test_make_cdes_full_schema(schema_data):
    cdes = make_cdes(schema_data)
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
    assert hasattr(cde, "sql_type")
    assert hasattr(cde, "metadata")
