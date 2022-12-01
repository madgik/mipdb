from mipdb.dataelements import CommonDataElement
from mipdb.dataelements import make_cdes
from mipdb.exceptions import InvalidDatasetError
import pytest
import pandas as pd

from mipdb.dataset import Dataset
from mipdb.reader import CSVDataFrameReader
from tests.conftest import DATASET_FILE


def test_valid_dataset_name():
    data = pd.DataFrame(
        {
            "var1": [1, 2],
            "dataset": ["dataset1", "dataset1"],
        }
    )
    dataset = Dataset(data)


def test_invalid_dataset_no_dataset_field():
    data = pd.DataFrame(
        {
            "var1": [1, 2],
            "var2": [3, 4],
        }
    )
    with pytest.raises(InvalidDatasetError):
        dataset = Dataset(data)


def test_to_dict():
    with CSVDataFrameReader(DATASET_FILE, 5).get_reader() as reader:
        for dataset_data in reader:
            dataset = Dataset(dataset_data)
            result = dataset.to_dict()
            print(result)
            assert result == [
                {
                    "subjectcode": "2",
                    "var1": "1",
                    "var2": None,
                    "var3": None,
                    "var4": None,
                    "dataset": "dataset",
                },
                {
                    "subjectcode": "2",
                    "var1": "1",
                    "var2": "2.0",
                    "var3": "12",
                    "var4": "22",
                    "dataset": "dataset",
                },
                {
                    "subjectcode": "2",
                    "var1": "1",
                    "var2": "1",
                    "var3": "13",
                    "var4": "23",
                    "dataset": "dataset",
                },
                {
                    "subjectcode": "3",
                    "var1": "1",
                    "var2": "1",
                    "var3": "14",
                    "var4": "24",
                    "dataset": "dataset",
                },
                {
                    "subjectcode": "3",
                    "var1": "1",
                    "var2": "2.0",
                    "var3": "15",
                    "var4": "25",
                    "dataset": "dataset",
                },
            ]


def test_validate_with_nan_values_integer_column_with_minValue():
    data = pd.DataFrame(
        {
            "var4": [22, None],
            "dataset": ["dataset1", "dataset1"],
        }
    )
    dataset = Dataset(data)
    metadata = {
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
    }

    dataset.validate_dataset(metadata)


def test_validate_with_nan_values_integer_column_with_only_maxValue():
    data = pd.DataFrame(
        {
            "var4": [1, None],
            "dataset": ["dataset1", "dataset1"],
        }
    )
    dataset = Dataset(data)
    metadata = {
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
    }

    dataset.validate_dataset(metadata)


def test_validate_with_nan_values_integer_column_without_min_max():
    data = pd.DataFrame(
        {
            "var4": [1, None],
            "dataset": ["dataset1", "dataset1"],
        }
    )
    dataset = Dataset(data)
    metadata = {
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
    }

    dataset.validate_dataset(metadata)


def test_validate():
    data = pd.DataFrame(
        {
            "subjectcode": [2, 2],
            "var1": [1, 2],
            "var2": [1, 2],
            "var3": [50, 20],
            "var4": [1, None],
            "dataset": ["dataset1", "dataset1"],
        }
    )
    dataset = Dataset(data)
    metadata = {
        "var1": CommonDataElement(
            code="var1",
            metadata="""
                {
                "is_categorical": false,
                "code": "var1",
                "sql_type": "text",
                "description": "",
                "label": "Variable 1",
                "methodology": ""}
            """,
        ),
        "var2": CommonDataElement(
            code="var2",
            metadata="""
                {
                    "is_categorical": true,
                    "code": "var2",
                    "sql_type": "text",
                    "description": "",
                    "enumerations":
                        {
                            "1": "Number1",
                            "2": "Number2"
                        },
                    "label": "Variable 2",
                    "methodology": ""
                }
            """,
        ),
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
        "var3": CommonDataElement(
            code="var3",
            metadata="""
            {
                "is_categorical": false,
                "code": "var3",
                "sql_type": "real",
                "min": 0,
                "max": 100,
                "description": "",
                "label": "Variable 3",
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
                "units": "years",
                "description": "",
                "label": "Variable 4",
                "methodology": ""
            }
            """,
        ),
        "subjectcode": CommonDataElement(
            code="subjectcode",
            metadata="""
            {
                "is_categorical": false,
                "code": "subjectcode",
                "sql_type": "text",
                "description": "",
                "label": "subjectcode",
                "methodology": ""
            }
            """,
        ),
    }

    dataset.validate_dataset(metadata)


dataframes = [
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, 1.1],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the dataset: 'dataset1' and column: 'var4'",
        id="int with float",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var3": [1.1, "not a float"],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the dataset: 'dataset1' and column: 'var3'",
        id="float with text",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, "not a int"],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the dataset: 'dataset1' and column: 'var4'",
        id="int with text",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var2": ["1", "l1"],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the dataset: 'dataset1' and column: 'var2'",
        id="text with non existing enumeration",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var5": [1.0, 2.0],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the dataset: 'dataset1' and column: 'var5'",
        id="text with int/float enumerations(1,2.0)  and 1.0 was given",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var3": [4, 5],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the dataset: 'dataset1' and column: 'var3'",
        id="enumeration exceeds min",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var3": [5, 65],
                "dataset": ["dataset1", "dataset1"],
            }
        ),
        "An error occurred while validating the dataset: 'dataset1' and column: 'var3'",
        id="enumeration exceeds max",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, 2],
                "dataset": ["dataset2", "dataset1"],
            }
        ),
        "The dataset field contains multiple values.",
        id="more that one dataset",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "var4": [1, None],
            }
        ),
        "The 'dataset' column is required to exist in the csv.",
        id="missing dataset",
    ),
]


@pytest.mark.parametrize("dataframe,exception_message", dataframes)
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
        dataset = Dataset(dataframe)
        dataset.validate_dataset(metadata)
