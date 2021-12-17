from abc import ABC, abstractmethod
import json

import pandas as pd

from mipdb.exceptions import FileContentError


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


class CSVFileReader(Reader):
    def __init__(self, file) -> None:
        self.file = file

    def read(self):
        return pd.read_csv(self.file, dtype=object)
