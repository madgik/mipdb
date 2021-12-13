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


def test_validate():
    data = pd.DataFrame(
        {
            "subjectcode": [1, 2],
            "var1": [1, 2],
            "var2": [1, 2],
            "var3": [50, 20],
            "var4": [1.1, None],
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


def test_validate1():
    data = pd.DataFrame(
        {
            "subjectcode": [1, 2],
            "var2": [5, None],
            "dataset": ["dataset1", "dataset1"],
        }
    )
    dataset = Dataset(data)
    metadata = {
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
    }
    with pytest.raises(InvalidDatasetError):
        dataset.validate_dataset(metadata)


dataset_files = [
    "tests/data/fail/data_model/dataset_exceeds_max.csv",
    "tests/data/fail/data_model/dataset_exceeds_min.csv",
    "tests/data/fail/data_model/dataset_is_not_unique.csv",
    "tests/data/fail/data_model/duplication_column_subjectcode.csv",
    "tests/data/fail/data_model/invalid_enum.csv",
    "tests/data/fail/data_model/invalid_type1.csv",
    "tests/data/fail/data_model/invalid_type2.csv",
    "tests/data/fail/data_model/missing_column_dataset.csv",
    "tests/data/fail/data_model/missing_column_subjectcode.csv",
]


@pytest.mark.parametrize("dataset_file", dataset_files)
def test_invalid_dataset_error_cases(dataset_file):

    reader = JsonFileReader("tests/data/fail/data_model/CDEsMetadata.json")
    data_model_data = reader.read()
    cdes = make_cdes(data_model_data)

    dataset_reader = CSVFileReader(dataset_file)
    dataset_data = dataset_reader.read()
    metadata = {cde.code: cde for cde in cdes}

    with pytest.raises(InvalidDatasetError):
        dataset = Dataset(dataset_data)
        dataset.validate_dataset(metadata)
