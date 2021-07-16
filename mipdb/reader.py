from abc import ABC, abstractmethod
from typing import TextIO
import json
import toml

import pandas

from mipdb.exceptions import FileContentError


class FileReader(ABC):
    stream: TextIO

    @abstractmethod
    def read(self):
        pass


class JsonFileReader(FileReader):
    def __init__(self, stream: TextIO) -> None:
        self.stream = stream

    def read(self):
        try:
            return json.load(self.stream)
        except json.JSONDecodeError as exc:
            orig_msg = exc.args[0]
            raise FileContentError(f"Unable to decode json file. {orig_msg}")


class TomlFileReader(FileReader):
    def __init__(self, stream: TextIO) -> None:
        self.stream = stream

    def read(self):
        return toml.load(self.stream)


class CsvFileReader(FileReader):
    def __init__(self, stream: TextIO) -> None:
        self.stream = stream

    def read(self):
        return pandas.read_csv(self.stream)


# ~~~~~~~~~~~~~~~~~~~~~~~ Tests ~~~~~~~~~~~~~~~~~~~~~~~~~~ #

from io import StringIO


def test_json_file_reader():
    stream = StringIO('{"some": ["content"]}')
    reader = JsonFileReader(stream)
    assert reader.read_file() == {"some": ["content"]}


def test_toml_file_reader():
    stream = StringIO('some="content"')
    reader = TomlFileReader(stream)
    assert reader.read_file() == {"some": "content"}


def test_csv_file_reader():
    stream = StringIO("some,content\n0,1")
    reader = CsvFileReader(stream)
    expected = pandas.DataFrame({"some": [0], "content": [1]})
    result = reader.read_file()
    assert all(result == expected)
