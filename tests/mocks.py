from contextlib import contextmanager
import sqlalchemy as sql

from mipdb.database import MonetDB


class MonetDBMock(MonetDB):
    """Mock version of MonetDB used for testing. Objects of this class have
    exactly the same functionality as objects of the MonetDB class except that
    queries are not actually executed against an external Monet DB service but
    are stored in the captured_queries instance attribute. All parameters for
    prepared SQL queries are stored in captured_multiparams and
    captured_params."""

    def __init__(self) -> None:
        self.captured_queries = []
        self.captured_multiparams = []
        self.captured_params = []

        def mock_executor(sql, *multiparams, **params):
            self.captured_queries.append(str(sql))
            self.captured_multiparams.append(multiparams)
            self.captured_params.append(params)

        url = "monetdb://mock:mock@mock:0/mock"
        self._executor = sql.create_engine(url, strategy="mock", executor=mock_executor)

    @contextmanager
    def begin(self):
        # Mock engine in SQLAlchemy doesn't have a begin method probably
        # because it makes no sense beginning a transaction on a mock engine.
        # However, in order to have unit tests without having to use an
        # external database, I do the following trick and it works!
        yield self

    def table_exists(self, table):
        return True

    def get_current_user(self):
        return "test_user"

    def get_data_model_id(self, code, version):
        return 1

    def get_dataset_id(self, code, data_model_id):
        return 1

    def get_executor(self):
        return None

    def get_datasets(self, data_model_id=None, columns=None):
        if columns:
            return [list(range(1, len(columns) + 1))]
        return [[1, 2]]

    def get_data_models(self, columns=None):
        if columns:
            return [list(range(1, len(columns) + 1))]
        return [[1, 2]]

    def get_dataset_status(self, dataset_id):
        return "WrongStatus"

    def get_metadata(self, schema):
        return {
            "dataset": '[{"code": "dataset", "sql_type": "text", "description": "", "enumerations": {"dataset1": "Dataset 1"}, "label": "Dataset", "units": "", "type": "nominal", "methodology": "", "is_categorical": true}]',
        }

    def get_data_model_status(self, data_model_id):
        return "WrongStatus"

    def get_dataset_properties(self, dataset_id):
        return '{"tags":["tag1"], "properties": {"key1": "value1"}}'

    def get_data_model_properties(self, dataset_id):
        return '{"tags":["tag1"], "properties": {"key1": "value1"}}'

    def get_data_model(self, data_model_id, columns):
        return "code", "version", "label"

    def get_dataset(self, dataset_id, columns):
        return "code", "label"


