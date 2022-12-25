import sys
from enum import IntEnum
from functools import wraps
from contextlib import contextmanager

sys.tracebacklimit = 0


class DataBaseError(Exception):
    """Is raised when the DB raises an OperationalError."""

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(message)


class UserInputError(Exception):
    """Is raised when the user inputs invalid arguments."""

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(message)


class FileContentError(Exception):
    """Is raised when file content doesn't have the expected format."""

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(message)


class InvalidDatasetError(Exception):
    """Is raised when a dataset violates the constraints imposed by the data model."""

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(message)


class InvalidDataModelError(Exception):
    """Is raised when the data model doesn't have the expected schema."""

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(message)


class ForeignKeyError(Exception):
    """Is raised when a table is deleted while there is a foreign key constrain from another table."""

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(message)


class ExitCode(IntEnum):
    OK = 0
    USER_ERROR = 64
    DB_ERROR = 65
    FILE_ERROR = 66


def handle_errors(func):
    @contextmanager
    def _handle_errors():
        try:
            yield
        except UserInputError as exc:
            print("User input error:\n")
            print(f"\t{exc.message}")
            sys.exit(ExitCode.USER_ERROR)
        except DataBaseError as exc:
            print("Database error:\n")
            print(f"\t{exc.message}")
            sys.exit(ExitCode.DB_ERROR)
        except FileContentError as exc:
            print("File error:\n")
            print(f"\t{exc.message}")
            sys.exit(ExitCode.FILE_ERROR)
        except ForeignKeyError as exc:
            print("Foreign key error:\n")
            print(f"\t{exc.message}")
        except InvalidDatasetError as exc:
            print(f"\nDataset error: {exc.message}")
            sys.exit(ExitCode.FILE_ERROR)
        # except Exception as exc:
        #     print("Something went wrong:\n")
        #     raise exc

    @wraps(func)
    def wrapper(*args, **kwargs):
        with _handle_errors():
            return func(*args, **kwargs)

    return wrapper
