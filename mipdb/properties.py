import json

from mipdb.exceptions import UserInputError


class Properties:
    def __init__(self, properties) -> None:
        self.properties = properties
        if not self.properties:
            self.properties = json.dumps({"tags": [], "properties": {}})

    def remove_tag(self, tag):
        properties_dict = json.loads(self.properties)
        if tag in properties_dict["tags"]:
            properties_dict["tags"].remove(tag)
            self.properties = json.dumps(properties_dict)
        else:
            raise UserInputError("Tag does not exist")

    def add_tag(self, tag):
        properties_dict = json.loads(self.properties)
        if tag not in properties_dict["tags"]:
            properties_dict["tags"].append(tag)
            self.properties = json.dumps(properties_dict)
        else:
            raise UserInputError("Tag already exists")

    def remove_property(self, key, value):
        properties_dict = json.loads(self.properties)
        if (key, value) in properties_dict["properties"].items():
            properties_dict["properties"].pop(key)
            self.properties = json.dumps(properties_dict)
        else:
            raise UserInputError("Property does not exist")

    def add_property(self, key, value, force):
        properties_dict = json.loads(self.properties)
        if key in properties_dict["properties"] and not force:
            raise UserInputError(
                "Property already exists.\n"
                "If you want to force override the property, please use the  '--force' flag"
            )
        else:
            properties_dict["properties"][key] = value
            self.properties = json.dumps(properties_dict)
