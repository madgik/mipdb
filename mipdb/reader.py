from abc import ABC, abstractmethod
import json

import pandas as pd

from mipdb.exceptions import FileContentError

PANDAS_DATAFRAME_CHUNK_SIZE = 500


class Reader(ABC):
    @abstractmethod
    def read(self):
        pass


class JsonFileReader(Reader):
    def __init__(self, file) -> None:
        self.file = file

    def read(self):
        with open(self.file, "r") as stream:
            try:
                return json.load(stream)
            except json.JSONDecodeError as exc:
                orig_msg = exc.args[0]
                raise FileContentError(f"Unable to decode json file. {orig_msg}")


class CSVDataFrameReader:
    def __init__(self, file, dataframe_chunk_size=None) -> None:
        self.file = file
        self.dataframe_chunk_size = dataframe_chunk_size
        if not self.dataframe_chunk_size:
            self.dataframe_chunk_size = PANDAS_DATAFRAME_CHUNK_SIZE

    def get_reader(self):
        return pd.read_csv(
            self.file,
            dtype=object,
            chunksize=self.dataframe_chunk_size,
        )
