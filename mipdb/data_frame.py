import pandera as pa
import pandas as pd

from mipdb.exceptions import InvalidDatasetError

DATASET_COLUMN_NAME = "dataset"


class DataFrame:
    _data: pd.DataFrame
    _datasets: list

    def __init__(self, data: pd.DataFrame) -> None:
        # Pandas will insert nan values where there is an empty value in the csv.
        # In order to be able to insert the values through the sqlalchemy we need to replace nan with None.
        self._data = data
        self._data = self._data.astype(object).where(pd.notnull(self._data), None)
        self._verify_dataset_field()
        self._datasets = self._data[DATASET_COLUMN_NAME].unique()

    @property
    def data(self):
        return self._data

    @property
    def datasets(self):
        return self._datasets

    def _verify_dataset_field(self):
        if DATASET_COLUMN_NAME not in self.data.columns:
            raise InvalidDatasetError(
                "The 'dataset' column is required to exist in the csv."
            )

    def to_dict(self):
        return self._data.to_dict("records")
