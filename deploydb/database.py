import sys
from contextlib import contextmanager

import pyodbc
from model import Server


class Database:
    """ Represents a database connection """

    def __init__(self, creds: Server) -> None:
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
        connection.timeout = 5  # default timeout 5 sec.
        cursor = connection.cursor()
        try:
            cursor.execute(f"USE {db_name};")
            yield cursor
        except pyodbc.DatabaseError as err:
            error, = err.args
            sys.stderr.write(error.message)
        except pyodbc.ProgrammingError as prg:
            raise prg
        finally:
            connection.close()
