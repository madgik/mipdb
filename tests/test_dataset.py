from mipdb.dataelements import CommonDataElement
from mipdb.dataelements import make_cdes
from mipdb.exceptions import InvalidDatasetError
import pytest
import pandas as pd

from mipdb.dataset import Dataset
from mipdb.reader import CSVFileReader
from mipdb.reader import JsonFileReader


def test_valid_dataset_name():
    data = pd.DataFrame(
        {
            "var1": [1, 2],
            "dataset": ["dataset1", "dataset1"],
        }
    )
    dataset = Dataset(data)


def test_invalid_dataset_name_value_not_unique():
    data = pd.DataFrame(
        {
            "var1": [1, 2],
            "dataset": ["dataset1", "another_dataset"],
        }
    )
    with pytest.raises(InvalidDatasetError):
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
    data = pd.DataFrame(
        {
            "var1": [1, 2],
            "dataset": ["dataset1", "dataset1"],
        }
    )
    dataset = Dataset(data)
    result = dataset.to_dict()
    assert result == [
        {"var1": 1, "dataset": "dataset1"},
        {"var1": 2, "dataset": "dataset1"},
    ]


def test_validate_with_nan_values_integer_column_with_minValue():
    data = pd.DataFrame(
        {
            "row_id": [1, 2],
            "subjectcode": [2, 2],
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
                    "isCategorical": true,
                    "code": "dataset",
                    "sql_type": "text",
                    "description": "",
                    "enumerations": [{"code": "dataset1", "label": "Dataset 1"}, {"code": "dataset2", "label": "Dataset 2"}],
                    "label": "Dataset", "methodology": ""
                }
            """,
        ),
        "var4": CommonDataElement(
            code="var4",
            metadata="""
            {
                "isCategorical": false,
                "code": "var4",
                "sql_type": "int",
                "minValue": 10,
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
            "row_id": [1, 2],
            "subjectcode": [2, 2],
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
                    "isCategorical": true,
                    "code": "dataset",
                    "sql_type": "text",
                    "description": "",
                    "enumerations": [{"code": "dataset1", "label": "Dataset 1"}, {"code": "dataset2", "label": "Dataset 2"}],
                    "label": "Dataset", "methodology": ""
                }
            """,
        ),
        "var4": CommonDataElement(
            code="var4",
            metadata="""
            {
                "isCategorical": false,
                "code": "var4",
                "sql_type": "int",
                "maxValue": 100,
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
            "row_id": [1, 2],
            "subjectcode": [2, 2],
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
                    "isCategorical": true,
                    "code": "dataset",
                    "sql_type": "text",
                    "description": "",
                    "enumerations": [{"code": "dataset1", "label": "Dataset 1"}, {"code": "dataset2", "label": "Dataset 2"}],
                    "label": "Dataset", "methodology": ""
                }
            """,
        ),
        "var4": CommonDataElement(
            code="var4",
            metadata="""
            {
                "isCategorical": false,
                "code": "var4",
                "sql_type": "int",
                "maxValue": 100,
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
            "row_id": [1, 2],
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
                "isCategorical": false,
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
                    "isCategorical": true,
                    "code": "var2",
                    "sql_type": "text",
                    "description": "",
                    "enumerations":
                        [
                            {"code": "1", "label": "Number1"},
                            {"code": "2", "label": "Number2"}
                        ],
                    "label": "Variable 2",
                    "methodology": ""
                }
            """,
        ),
        "dataset": CommonDataElement(
            code="dataset",
            metadata="""
                {
                    "isCategorical": true,
                    "code": "dataset",
                    "sql_type": "text",
                    "description": "",
                    "enumerations": [{"code": "dataset1", "label": "Dataset 1"}, {"code": "dataset2", "label": "Dataset 2"}],
                    "label": "Dataset", "methodology": ""
                }
            """,
        ),
        "var3": CommonDataElement(
            code="var3",
            metadata="""
            {
                "isCategorical": false,
                "code": "var3",
                "sql_type": "real",
                "minValue": 0,
                "maxValue": 100,
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
                "isCategorical": false,
                "code": "var4",
                "sql_type": "int",
                "units": "years",
                "description": "",
                "label": "Variable 4",
                "methodology": ""
            }
            """,
        ),
    }

    dataset.validate_dataset(metadata)


dataset_files = [
    (
        "tests/data/fail/data_model_v_1_0/dataset_exceeds_max.csv",
        "On dataset valid_dataset and column var3 has error",
    ),
    (
        "tests/data/fail/data_model_v_1_0/dataset_exceeds_min.csv",
        "On dataset valid_dataset and column var3 has error",
    ),
    (
        "tests/data/fail/data_model_v_1_0/dataset_is_not_unique.csv",
        "The dataset field contains multiple values.",
    ),
    (
        "tests/data/fail/data_model_v_1_0/duplication_column_row_id.csv",
        "There are duplicated values in the column row_id",
    ),
    (
        "tests/data/fail/data_model_v_1_0/invalid_enum.csv",
        "On dataset valid_dataset and column var2 has error",
    ),
    (
        "tests/data/fail/data_model_v_1_0/invalid_type1.csv",
        "On dataset valid_dataset and column var3 has error",
    ),
    (
        "tests/data/fail/data_model_v_1_0/invalid_type2.csv",
        "On dataset valid_dataset and column var4 has error",
    ),
    (
        "tests/data/fail/data_model_v_1_0/missing_column_dataset.csv",
        "There is no dataset field in the Dataset",
    ),
    (
        "tests/data/fail/data_model_v_1_0/missing_column_subjectcode.csv",
        "Error inserting dataset without the column subjectcode into the database",
    ),
    (
        "tests/data/fail/data_model_v_1_0/missing_column_row_id.csv",
        "Error inserting dataset without the column row_id into the database",
    ),
]


@pytest.mark.parametrize("dataset_file,exception_message", dataset_files)
def test_invalid_dataset_error_cases(dataset_file, exception_message):
    reader = JsonFileReader("tests/data/fail/data_model_v_1_0/CDEsMetadata.json")
    data_model_data = reader.read()
    cdes = make_cdes(data_model_data)

    dataset_reader = CSVFileReader(dataset_file)
    dataset_data = dataset_reader.read()
    metadata = {cde.code: cde for cde in cdes}

    with pytest.raises(InvalidDatasetError, match=exception_message):
        dataset = Dataset(dataset_data)
        dataset.validate_dataset(metadata)
