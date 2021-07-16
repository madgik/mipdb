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
        # because it makes no sense beggining a transaction on a mock engine.
        # However, in order to have unit tests without having to use an
        # external database, I do the following trick and it works!
        yield self
