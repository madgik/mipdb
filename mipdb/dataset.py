import pandas as pd

from mipdb.exceptions import InvalidDatasetError


class Dataset:
    _data: pd.DataFrame
    _name: str

    def __init__(self, data: pd.DataFrame) -> None:
        self._data = data
        self._verify_dataset_field()
        self._name = self._data["dataset"][0]

    @property
    def data(self):
        return self._data

    @property
    def name(self):
        return self._name

    def _verify_dataset_field(self):
        if "dataset" not in self.data.columns:
            raise InvalidDatasetError("There is no dataset field in the Dataset")
        if len(set(self.data["dataset"])) > 1:
            raise InvalidDatasetError("The dataset field contains multiple values.")

    def to_dict(self):
        return self._data.to_dict("records")
