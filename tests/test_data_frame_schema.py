from mipdb.dataelements import CommonDataElement
from mipdb.dataelements import make_cdes
from mipdb.data_frame_schema import DataFrameSchema
from mipdb.exceptions import InvalidDatasetError
import pytest
import pandas as pd

dataframes = [
    pytest.param(
        pd.DataFrame(
            {
                "var4": [22, None],
                "dataset": ["dataset1", "dataset1"],
            },
        ),
        {
            "dataset": CommonDataElement(
                code="dataset",
                metadata="""
                    {
                        "is_categorical": true,
                        "code": "dataset",
                        "sql_type": "text",
                        "description": "",
                        "enumerations": {"dataset1": "Dataset 1", "dataset2": "Dataset 2"},
                        "label": "Dataset",
                        "methodology": ""
                    }
                """,
            ),
            "var4": CommonDataElement(
                code="var4",
                metadata="""
                {
                    "is_categorical": false,
                    "code": "var4",
                    "sql_type": "int",
                    "min": 10,
                    "units": "years",
                    "description": "",
                    "label": "Variable 4",
                    "methodology": ""
                }
                """,
            ),
        },
        id="validate with nan values integer column with minValue",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, None],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        {
            "dataset": CommonDataElement(
                code="dataset",
                metadata="""
                        {
                            "is_categorical": true,
                            "code": "dataset",
                            "sql_type": "text",
                            "description": "",
                            "enumerations": {"dataset1": "Dataset 1", "dataset2": "Dataset 2"},
                            "label": "Dataset", "methodology": ""
                        }
                    """,
            ),
            "var4": CommonDataElement(
                code="var4",
                metadata="""
                    {
                        "is_categorical": false,
                        "code": "var4",
                        "sql_type": "int",
                        "max": 100,
                        "units": "years",
                        "description": "",
                        "label": "Variable 4",
                        "methodology": ""
                    }
                    """,
            ),
        },
        id="validate with nan values integer column with only maxValue",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, None],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        {
            "dataset": CommonDataElement(
                code="dataset",
                metadata="""
                        {
                            "is_categorical": true,
                            "code": "dataset",
                            "sql_type": "text",
                            "description": "",
                            "enumerations": {"dataset1": "Dataset 1", "dataset2": "Dataset 2"},
                            "label": "Dataset", "methodology": ""
                        }
                    """,
            ),
            "var4": CommonDataElement(
                code="var4",
                metadata="""
                    {
                        "is_categorical": false,
                        "code": "var4",
                        "sql_type": "int",
                        "max": 100,
                        "units": "years",
                        "description": "",
                        "label": "Variable 4",
                        "methodology": ""
                    }
                    """,
            ),
        },
        id="validate with nan values integer column without min max",
    ),
]


@pytest.mark.parametrize("dataframe,metadata", dataframes)
def test_validate_dataframe(dataframe, metadata):
    dataframe_schema = DataFrameSchema(
        metadata_table=metadata, columns=dataframe.columns.to_list()
    )
    dataframe_schema.validate_dataframe(dataframe=dataframe)


invalid_dataframes = [
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, 1.1],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var4'",
        id="int with float",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var3": [1.1, "not a float"],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var3'",
        id="float with text",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, "not a int"],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var4'",
        id="int with text",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var2": ["1", "l1"],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var2'",
        id="text with non existing enumeration",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var5": [1.0, 2.0],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var5'",
        id="text with int/float enumerations(1,2.0)  and 1.0 was given",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var3": [4, 5],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var3'",
        id="enumeration exceeds min",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var3": [5, 65],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the csv on column: 'var3'",
        id="enumeration exceeds max",
    ),
]


@pytest.mark.parametrize("dataframe,exception_message", invalid_dataframes)
def test_invalid_dataset_error_cases(dataframe, exception_message):
    data_model_metadata = {
        "code": "data_model",
        "label": "The Data Model",
        "version": "1.0",
        "variables": [
            {
                "isCategorical": False,
                "code": "var1",
                "sql_type": "text",
                "description": "",
                "label": "Variable 1",
                "methodology": "",
            },
            {
                "isCategorical": False,
                "label": "subjectcode",
                "code": "subjectcode",
                "sql_type": "text",
                "description": "",
                "methodology": "",
            },
            {
                "isCategorical": False,
                "code": "var2",
                "sql_type": "text",
                "description": "",
                "enumerations": [
                    {"code": "l1", "label": "Level1"},
                    {"code": "l2", "label": "Level2"},
                ],
                "label": "Variable 2",
                "methodology": "",
            },
            {
                "isCategorical": True,
                "code": "dataset",
                "sql_type": "text",
                "description": "",
                "enumerations": [
                    {"code": "dataset1", "label": "dataset1"},
                    {"code": "dataset2", "label": "dataset2"},
                ],
                "label": "Dataset",
                "methodology": "",
            },
        ],
        "groups": [
            {
                "name": "group",
                "label": "The Group",
                "variables": [
                    {
                        "isCategorical": False,
                        "code": "var3",
                        "sql_type": "real",
                        "minValue": 5,
                        "maxValue": 60,
                        "description": "",
                        "label": "Variable 3",
                        "methodology": "",
                    }
                ],
                "groups": [
                    {
                        "name": "inner_group",
                        "label": "The Inner Group",
                        "variables": [
                            {
                                "isCategorical": False,
                                "code": "var4",
                                "sql_type": "int",
                                "units": "years",
                                "description": "",
                                "label": "Variable 4",
                                "methodology": "",
                            },
                            {
                                "isCategorical": False,
                                "code": "var5",
                                "sql_type": "text",
                                "description": "",
                                "enumerations": [
                                    {"code": "1", "label": "Level1"},
                                    {"code": "2.0", "label": "Level2"},
                                ],
                                "label": "Variable 5",
                                "methodology": "",
                            },
                        ],
                    }
                ],
            }
        ],
    }
    cdes = make_cdes(data_model_metadata)
    metadata = {cde.code: cde for cde in cdes}
    with pytest.raises(InvalidDatasetError, match=exception_message):
        dataframe_schema = DataFrameSchema(
            metadata_table=metadata, columns=dataframe.columns.to_list()
        )
        dataframe_schema.validate_dataframe(dataframe=dataframe)
