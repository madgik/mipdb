from unittest.mock import mock_open, patch

import pytest

from mipdb.reader import JsonFileReader
from mipdb.exceptions import FileContentError


def test_json_reader():
    mock = mock_open(read_data='{"some": ["content"]}')
    with patch("mipdb.reader.open", mock):
        reader = JsonFileReader(file="no_file")
        assert reader.read() == {"some": ["content"]}


def test_json_reader_error():
    mock = mock_open(read_data="wrong json content")
    with patch("mipdb.reader.open", mock):
        reader = JsonFileReader(file="no_file")
        with pytest.raises(FileContentError):
            reader.read()
