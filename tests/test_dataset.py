from mipdb.exceptions import InvalidDatasetError
import pytest
import pandas as pd

from mipdb.dataset import Dataset


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
