from mipdb.dataelements import CommonDataElement
from mipdb.dataelements import make_cdes
from mipdb.exceptions import InvalidDatasetError
import pytest
import pandas as pd

from mipdb.data_frame import DataFrame
from mipdb.reader import CSVDataFrameReader
from tests.conftest import DATASET_FILE


def test_valid_dataset_name():
    data = pd.DataFrame(
        {
            "var1": [1, 2],
            "dataset": ["dataset1", "dataset1"],
        }
    )
    dataset = DataFrame(data)


def test_invalid_dataset_no_dataset_field():
    data = pd.DataFrame(
        {
            "var1": [1, 2],
            "var2": [3, 4],
        }
    )
    with pytest.raises(InvalidDatasetError):
        dataset = DataFrame(data)


def test_to_dict():
    with CSVDataFrameReader(DATASET_FILE, 5).get_reader() as reader:
        for dataset_data in reader:
            dataset = DataFrame(dataset_data)
            result = dataset.to_dict()
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
