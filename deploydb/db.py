
import sys
from contextlib import contextmanager
import pyodbc

from .model import DbCreds


class Database:
    """ Represents a database connection """

    def __init__(self, creds: DbCreds) -> None:
        self.creds = creds.__dict__
        self._conn_str = 'APP=deploydb;DRIVER={driver};SERVER={server};DATABASE=master;UID={user};PWD={passw}' # noqa
        self._conn_builder()

    def _conn_builder(self) -> str:
        self._conn_str = self._conn_str.format(**self.creds)

    @contextmanager
    def connect(self, db_name='master'):
        connection = pyodbc.connect(
            str=self._conn_str,
            autocommit=True
        )
        connection.timeout = self.creds.get('timeout', 30)  # default timeout 30 sec.
        cursor = connection.cursor()
        try:
            cursor.execute(f"USE [{db_name}];")
            yield cursor
        except pyodbc.DatabaseError as err:
            error, = err.args
            sys.stderr.write(error.message)
            raise error.message
        except pyodbc.ProgrammingError as prg:
            raise prg
        finally:
            connection.close()
