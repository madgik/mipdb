from mipdb.dataelements import CommonDataElement
from mipdb.dataelements import make_cdes
from mipdb.exceptions import InvalidDatasetError
import pytest
import pandas as pd

from mipdb.dataset import Dataset
from mipdb.reader import JsonFileReader
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


def test_to_dict(data_model_metadata):
    data = pd.read_csv(DATASET_FILE)
    dataset = Dataset(data)
    result = dataset.to_dict()

    assert result == [
        {
            "subjectcode": 2,
            "var1": 1,
            "var2": "l1",
            "var3": 11,
            "var4": None,
            "dataset": "dataset",
        },
        {
            "subjectcode": 2,
            "var1": 1,
            "var2": "l2",
            "var3": 12,
            "var4": 22.0,
            "dataset": "dataset",
        },
        {
            "subjectcode": 2,
            "var1": 1,
            "var2": "l1",
            "var3": 13,
            "var4": 23.0,
            "dataset": "dataset",
        },
        {
            "subjectcode": 3,
            "var1": 1,
            "var2": "l1",
            "var3": 14,
            "var4": 24.0,
            "dataset": "dataset",
        },
        {
            "subjectcode": 3,
            "var1": 1,
            "var2": "l2",
            "var3": 15,
            "var4": 25.0,
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


dataset_files = [
    (
        "tests/data/fail/data_model_v_1_0/dataset_exceeds_max.csv",
        "An error occurred while validating the dataset: 'valid_dataset' and column: 'var3'",
    ),
    (
        "tests/data/fail/data_model_v_1_0/dataset_exceeds_min.csv",
        "An error occurred while validating the dataset: 'valid_dataset' and column: 'var3'",
    ),
    (
        "tests/data/fail/data_model_v_1_0/invalid_enum.csv",
        "An error occurred while validating the dataset: 'valid_dataset' and column: 'var2'",
    ),
    (
        "tests/data/fail/data_model_v_1_0/invalid_type1.csv",
        "An error occurred while validating the dataset: 'valid_dataset' and column: 'var3'",
    ),
    (
        "tests/data/fail/data_model_v_1_0/invalid_type2.csv",
        "An error occurred while validating the dataset: 'valid_dataset' and column: 'var4'",
    ),
    (
        "tests/data/fail/data_model_v_1_0/missing_column_dataset.csv",
        "The 'dataset' column is required to exist in the csv.",
    ),
]


@pytest.mark.parametrize("dataset_file,exception_message", dataset_files)
def test_invalid_dataset_error_cases(dataset_file, exception_message):
    reader = JsonFileReader("tests/data/fail/data_model_v_1_0/CDEsMetadata.json")
    data_model_metadata = reader.read()
    cdes = make_cdes(data_model_metadata)

    dataset_data = pd.read_csv(dataset_file, dtype=object)
    metadata = {cde.code: cde for cde in cdes}

    with pytest.raises(InvalidDatasetError, match=exception_message):
        dataset = Dataset(dataset_data)
        dataset.validate_dataset(metadata)
