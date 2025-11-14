import pandas as pd


DATASET_COLUMN_NAME = "dataset"


class DataFrame:
    _data: pd.DataFrame
    _datasets: list

    def __init__(self, data: pd.DataFrame) -> None:
        # Pandas will insert nan values where there is an empty value in the csv.
        # In order to be able to insert the values through the sqlalchemy we need to replace nan with None.
        self._data = data.astype(object).where(pd.notnull(data), None)
        self._datasets = self._data[DATASET_COLUMN_NAME].unique()

    @property
    def data(self):
        return self._data

    @property
    def datasets(self):
        return self._datasets

    def to_dict(self, columns: list[str] | None = None):
        data = self._data
        if columns:
            missing_columns = [col for col in columns if col not in data.columns]
            if missing_columns:
                data = data.assign(**{col: None for col in missing_columns})
            data = data[columns]
        return data.to_dict("records")
