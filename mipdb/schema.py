import sqlalchemy as sql

from mipdb.exceptions import UserInputError


class Schema:
    name: str
    schema: sql.MetaData

    def __init__(self, name) -> None:
        self.name = name
        self._validate_schema_name()
        self.schema = sql.MetaData(schema=self.name)

    def __repr__(self) -> str:
        return f"Schema(name={self.name})"

    def create(self, db):
        db.create_schema(self.name)

    def drop(self, db):
        db.drop_schema(self.name)

    def _validate_schema_name(self):
        if '"' in self.name:
            raise UserInputError(
                f"Data model's name: {self.name} contains prohibited character double quotes (\")"
            )
