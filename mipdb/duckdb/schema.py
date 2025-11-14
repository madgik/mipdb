from mipdb.exceptions import UserInputError


def _sanitize_schema_name(name: str) -> str:
    sanitized = name
    for ch in (":", "-", "."):
        sanitized = sanitized.replace(ch, "_")
    return sanitized


class Schema:
    name: str
    db_name: str

    def __init__(self, name: str) -> None:
        self.name = name
        self._validate_schema_name()
        self.db_name = _sanitize_schema_name(name)

    def __repr__(self) -> str:
        return f"Schema(name={self.name})"

    def create(self, db) -> None:  # pragma: no cover - kept for API compatibility
        del db

    def drop(self, db) -> None:  # pragma: no cover - kept for API compatibility
        del db

    def _validate_schema_name(self) -> None:
        if '"' in self.name:
            raise UserInputError(
                f"Data model's name: {self.name} contains prohibited character double quotes (\")"
            )
