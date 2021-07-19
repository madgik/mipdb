from abc import ABC, abstractmethod
import json

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
