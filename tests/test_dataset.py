from mipdb.exceptions import InvalidDatasetError
import pytest
import pandas as pd

from mipdb.schema import Schema
from mipdb.tables import PrimaryDataTable
from mipdb.dataset import Dataset
from mipdb.dataelements import make_cdes


def test_valid_dataset_name():
    data = pd.DataFrame(
        {
            "var1": [1, 2],
            "dataset": ["a_dataset", "a_dataset"],
        }
    )
    dataset = Dataset(data)


def test_invalid_dataset_name_value_not_unique():
    data = pd.DataFrame(
        {
            "var1": [1, 2],
            "dataset": ["a_dataset", "another_dataset"],
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
