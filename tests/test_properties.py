import pytest

from mipdb.exceptions import UserInputError
from mipdb.properties import Properties


@pytest.fixture
def properties():
    return Properties(
        '{"tags": ["tag1", "tag2"], "properties": {"key1": "value1", "key2": "value2"}}'
    )


def test_add_property(properties):
    properties.add_property(key="key", value="value", force=False)
    assert (
        properties.properties
        == '{"tags": ["tag1", "tag2"], "properties": {"key1": "value1", "key2": "value2", "key": "value"}}'
    )


def test_add_property_with_existing_key_with_force(properties):
    properties.add_property(key="key2", value="value1", force=True)
    assert (
        properties.properties
        == '{"tags": ["tag1", "tag2"], "properties": {"key1": "value1", "key2": "value1"}}'
    )


def test_add_tag(properties):
    properties.add_tag(tag="tag")
    assert (
        properties.properties
        == '{"tags": ["tag1", "tag2", "tag"], "properties": {"key1": "value1", "key2": "value2"}}'
    )


def test_remove_property(properties):
    properties.remove_property(key="key1", value="value1")
    assert (
        properties.properties
        == '{"tags": ["tag1", "tag2"], "properties": {"key2": "value2"}}'
    )


def test_remove_tag(properties):
    properties.remove_tag(tag="tag1")
    assert (
        properties.properties
        == '{"tags": ["tag2"], "properties": {"key1": "value1", "key2": "value2"}}'
    )


def test_tag_already_exists(properties):
    with pytest.raises(UserInputError):
        properties.add_tag(tag="tag1")


def test_property_already_exists(properties):
    with pytest.raises(UserInputError):
        properties.add_property(key="key1", value="value1", force=False)


def test_property_non_existant(properties):
    with pytest.raises(UserInputError):
        properties.remove_property(key="key1", value="value2")


def test_tag_non_existant(properties):
    with pytest.raises(UserInputError):
        properties.remove_tag(tag="tag")
