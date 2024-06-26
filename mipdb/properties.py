import json

from mipdb.exceptions import UserInputError


class Properties:
    def __init__(self, properties) -> None:
        self.properties = properties
        if not self.properties:
            self.properties = {"tags": [], "properties": {}}

    def remove_tag(self, tag):
        if tag in self.properties["tags"]:
            self.properties["tags"].remove(tag)
        else:
            raise UserInputError("Tag does not exist")

    def add_tag(self, tag):
        if tag not in self.properties["tags"]:
            self.properties["tags"].append(tag)
        else:
            raise UserInputError("Tag already exists")

    def remove_property(self, key, value):
        if (key, value) in self.properties["properties"].items():
            self.properties["properties"].pop(key)
        else:
            raise UserInputError("Property does not exist")

    def add_property(self, key, value, force):
        if key in self.properties["properties"] and not force:
            raise UserInputError(
                "Property already exists.\n"
                "If you want to force override the property, please use the  '--force' flag"
            )
        else:
            self.properties["properties"][key] = value
