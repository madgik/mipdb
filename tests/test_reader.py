from unittest.mock import mock_open, patch

import pytest

from mipdb.reader import JsonFileReader, CSVDataFrameReader
from mipdb.exceptions import FileContentError
from tests.conftest import DATASET_FILE


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


def test_csv_dataframe_reader():
    with CSVDataFrameReader(DATASET_FILE).get_reader() as reader:
        for data in reader:
            assert data.values.shape == (5, 6)


def test_csv_dataframe_reader_with_chunks():
    with CSVDataFrameReader(DATASET_FILE, 1).get_reader() as reader:
        for data in reader:
            assert data.values.shape == (1, 6)


def test_csv_dataframe_reader_with_chunks_of_two_rows():
    expected_len_of_each_chunk = [2, 2, 1]
    with CSVDataFrameReader(DATASET_FILE, 2).get_reader() as reader:
        for data, expected_length in zip(reader, expected_len_of_each_chunk):
            assert data.values.shape == (expected_length, 6)
