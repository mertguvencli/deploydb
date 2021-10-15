"""Top-level package for deploydb."""
__author__ = """Mert Güvençli"""
__email__ = 'guvenclimert@gmail.com'
__version__ = '0.1.4'

import os
import sys
from datetime import datetime
import json
import csv
from contextlib import contextmanager
from typing import Any, List, Optional

from git import Repo, Git
import pyodbc
from pydantic import BaseModel


class Server(BaseModel):
    driver: str
    server: str
    server_alias: str
    user: str
    passw: str


class Config(BaseModel):
    local_path: str
    https_url: Optional[str] = None
    ssh_url: Optional[str] = None
    target_branch: str
    servers: List[Server]


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


class Base:
    """ Check and validate configuration. """
    def __init__(self, config) -> None:
        """Returns validated configuration model.

        Args:
            config : json file or a dict for global variables.
        """
        self.config = config
        self._config: Config = None
        self._handle_config()

    def _is_file_path(self):
        try:
            return os.path.exists(self.config)
        except:  # noqa
            return False

    def _handle_config(self) -> str:
        if self._is_file_path():
            with open(self.config) as json_file:
                self._config = Config(**json.load(json_file))
        elif isinstance(self.config, dict):
            self._config = Config(**self.config)
        else:
            raise ValueError(
                'Invalid Config argument: "{0}". Config argument must be a file path, '
                'or a dict containing the parsed file contents.'.format(self.config)
            )

        if self._config:
            for server in self._config.servers:
                _db = Database(server)
                with _db.connect() as db:
                    print("Database connection successfully created!")
                    db.execute("SELECT 1").fetchone()


def _save_csv(path, columns, rows):
    file_exists = os.path.exists(path)
    mode = 'a' if file_exists else 'w'
    with open(path, mode) as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(columns)
        writer.writerows(rows)


def _set_commit_log(hexsha, path):
    columns = ['hash', 'time']
    row = [hexsha, datetime.now()]
    _save_csv(path=path, columns=columns, rows=row)


def _last_commit_hash(path):
    logs = []
    file_exists = os.path.exists(path)
    if file_exists:
        with open(path, 'r') as f:
            logs = [line for line in csv.reader(f)]
    if len(logs) > 1:
        return logs[-1][0]  # commit_id
    return None


_SUB_FOLDERS = (
    'Tables',
    'Views',
    'Functions',
    'Stored-Procedures',
    'Triggers',
    'Types',
    'DMLs',
    'DDLs'
)

_QUERIES = dict(
    DATABASES = """
        SELECT name AS DB_NAME
        FROM sys.databases
        WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
    """,  # noqa
    TABLES = """
        SELECT
            SCHEMA_NAME = schemas.name
        ,   TABLE_NAME  = tables.name
        FROM sys.tables, sys.schemas
        WHERE tables.schema_id = schemas.schema_id
    """, # noqa
    CREATE_TABLE = """
        DECLARE
            @schema_name    NVARCHAR(200) = ?
        ,	@table_name     NVARCHAR(300) = ?

        DECLARE
            @object_name    SYSNAME
        ,   @object_id      INT

        SELECT
            @object_name    = '[' + s.name + '].[' + o.name + ']'
        ,   @object_id      = o.[object_id]
        FROM sys.objects o WITH (NOWAIT)
            JOIN sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id]
        WHERE s.name = @schema_name
        AND o.name = @table_name
        AND o.[type] = 'U'
        AND o.is_ms_shipped = 0

        DECLARE @SQL NVARCHAR(MAX) = ''

        ;WITH index_column AS
        (
            SELECT
                ic.[object_id]
                , ic.index_id
                , ic.is_descending_key
                , ic.is_included_column
                , c.name
            FROM sys.index_columns ic WITH (NOWAIT)
            JOIN sys.columns c WITH (NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
            WHERE ic.[object_id] = @object_id
        ),
        fk_columns AS
        (
            SELECT
                k.constraint_object_id
                , cname = c.name
                , rcname = rc.name
            FROM sys.foreign_key_columns k WITH (NOWAIT)
            JOIN sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id
            JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
            WHERE k.parent_object_id = @object_id
        )
        SELECT @SQL = 'CREATE TABLE ' + @object_name + CHAR(13) + '(' + CHAR(13) + STUFF((
            SELECT CHAR(9) + ', [' + c.name + '] ' +
                CASE WHEN c.is_computed = 1
                    THEN 'AS ' + cc.[definition]
                    ELSE (tp.name) +
                        CASE WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary', 'text')
                            THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
                            --WHEN tp.name IN ('nvarchar', 'nchar', 'ntext')
                            WHEN tp.name IN ('nvarchar', 'nchar')
                            THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
                            WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')
                            THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
                            WHEN tp.name = 'decimal'
                            THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'
                            ELSE ''
                        END +
                        --CASE WHEN c.collation_name IS NOT NULL THEN ' COLLATE ' + c.collation_name ELSE '' END +
                        CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END +
                        CASE WHEN dc.[definition] IS NOT NULL THEN ' DEFAULT' + dc.[definition] ELSE '' END +
                        CASE WHEN ic.is_identity = 1 THEN ' IDENTITY(' + CAST(ISNULL(ic.seed_value, '0') AS CHAR(1)) + ',' + CAST(ISNULL(ic.increment_value, '1') AS CHAR(1)) + ')' ELSE '' END
                END + CHAR(13)
            FROM sys.columns c WITH (NOWAIT)
            JOIN sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id
            LEFT JOIN sys.computed_columns cc WITH (NOWAIT) ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
            LEFT JOIN sys.default_constraints dc WITH (NOWAIT) ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
            LEFT JOIN sys.identity_columns ic WITH (NOWAIT) ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
            WHERE c.[object_id] = @object_id
            ORDER BY c.column_id
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, CHAR(9) + ' ')
            + ISNULL((SELECT CHAR(9) + ', CONSTRAINT [' + k.name + '] PRIMARY KEY (' +
                            (SELECT STUFF((
                                SELECT ', [' + c.name + '] ' + CASE WHEN ic.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END
                                FROM sys.index_columns ic WITH (NOWAIT)
                                JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
                                WHERE ic.is_included_column = 0
                                    AND ic.[object_id] = k.parent_object_id
                                    AND ic.index_id = k.unique_index_id
                                FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''))
                    + ')' + CHAR(13)
                    FROM sys.key_constraints k WITH (NOWAIT)
                    WHERE k.parent_object_id = @object_id
                        AND k.[type] = 'PK'), '') + ')'  + CHAR(13)
            + ISNULL((SELECT (
                SELECT CHAR(13) +
                    'ALTER TABLE ' + @object_name + ' WITH'
                    + CASE WHEN fk.is_not_trusted = 1
                        THEN ' NOCHECK'
                        ELSE ' CHECK'
                    END +
                    ' ADD CONSTRAINT [' + fk.name  + '] FOREIGN KEY('
                    + STUFF((
                        SELECT ', [' + k.cname + ']'
                        FROM fk_columns k
                        WHERE k.constraint_object_id = fk.[object_id]
                        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
                    + ')' +
                    ' REFERENCES [' + SCHEMA_NAME(ro.[schema_id]) + '].[' + ro.name + '] ('
                    + STUFF((
                        SELECT ', [' + k.rcname + ']'
                        FROM fk_columns k
                        WHERE k.constraint_object_id = fk.[object_id]
                        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
                    + ')'
                    + CASE
                        WHEN fk.delete_referential_action = 1 THEN ' ON DELETE CASCADE'
                        WHEN fk.delete_referential_action = 2 THEN ' ON DELETE SET NULL'
                        WHEN fk.delete_referential_action = 3 THEN ' ON DELETE SET DEFAULT'
                        ELSE ''
                    END
                    + CASE
                        WHEN fk.update_referential_action = 1 THEN ' ON UPDATE CASCADE'
                        WHEN fk.update_referential_action = 2 THEN ' ON UPDATE SET NULL'
                        WHEN fk.update_referential_action = 3 THEN ' ON UPDATE SET DEFAULT'
                        ELSE ''
                    END
                    + CHAR(13) + 'ALTER TABLE ' + @object_name + ' CHECK CONSTRAINT [' + fk.name  + ']' + CHAR(13)
                FROM sys.foreign_keys fk WITH (NOWAIT)
                JOIN sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id
                WHERE fk.parent_object_id = @object_id
                FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')), '')
            + ISNULL(((SELECT
                CHAR(13) + 'CREATE' + CASE WHEN i.is_unique = 1 THEN ' UNIQUE' ELSE '' END
                        + ' NONCLUSTERED INDEX [' + i.name + '] ON ' + @object_name + ' (' +
                        STUFF((
                        SELECT ', [' + c.name + ']' + CASE WHEN c.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                        FROM index_column c
                        WHERE c.is_included_column = 0
                            AND c.index_id = i.index_id
                        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')'
                        + ISNULL(CHAR(13) + 'INCLUDE (' +
                            STUFF((
                            SELECT ', [' + c.name + ']'
                            FROM index_column c
                            WHERE c.is_included_column = 1
                                AND c.index_id = i.index_id
                            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')', '')  + CHAR(13)
                FROM sys.indexes i WITH (NOWAIT)
                WHERE i.[object_id] = @object_id
                    AND i.is_primary_key = 0
                    AND i.[type] = 2
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
            ), '')

        SELECT @SQL AS SQL
    """,  # noqa
    OBJECTS = """
        SELECT
            SUB_FOLDER		= CASE all_objects.type
                                WHEN 'FN' THEN 'Functions'			-- SQL_SCALAR_FUNCTION
                                WHEN 'V ' THEN 'Views'				-- VIEW
                                WHEN 'IF' THEN 'Functions'			-- SQL_INLINE_TABLE_VALUED_FUNCTION
                                WHEN 'TF' THEN 'Functions'			-- SQL_TABLE_VALUED_FUNCTION
                                WHEN 'P ' THEN 'Stored-Procedures'	-- SQL_STORED_PROCEDURE
                                WHEN 'TR' THEN 'Triggers'			-- SQL_TRIGGER
                            END
        ,	OBJECT_ID	= all_objects.object_id
        ,	SCHEMA_NAME	= schemas.name
        ,	OBJECT_NAME	= all_objects.name
        ,	SQL		    = all_sql_modules.definition

        FROM sys.all_objects, sys.schemas, sys.all_sql_modules
        WHERE all_objects.schema_id = schemas.schema_id
        AND all_sql_modules.object_id = all_objects.object_id
        AND all_objects.object_id > 0
        ORDER BY
            CASE all_objects.type
                WHEN 'FN' THEN 1	-- SQL_SCALAR_FUNCTION
                WHEN 'V ' THEN 2	-- VIEW
                WHEN 'IF' THEN 3	-- SQL_INLINE_TABLE_VALUED_FUNCTION
                WHEN 'TF' THEN 4	-- SQL_TABLE_VALUED_FUNCTION
                WHEN 'P ' THEN 5	-- SQL_STORED_PROCEDURE
                WHEN 'TR' THEN 6	-- SQL_TRIGGER
            END
        ,	all_objects.object_id
    """  # noqa
)


class RepoGenerator(Base):
    """Generator will create all the database object that you need."""
    def __init__(self, config, export_path, err_file_name="errors.csv") -> None:
        """

        Args:
            config (Any): config file or a `dict`.
            export_path (str): where the exported files locate
            err_file_name (str, optional): where the errors locate. Defaults to "errors.csv".
        """
        super().__init__(config)
        self.path = export_path
        self.err_file_name = err_file_name
        self._failure = []

    def _handle_server(self, server: Server) -> str:
        return server.server_alias if len(server.server_alias) > 0 else server.server

    def _create_folder(self, _server: Server, db_name):
        # server folder
        server = self._handle_server(_server)
        project_path = os.path.join(self.path, server)
        if not os.path.exists(project_path):
            os.mkdir(project_path)

        # db folder
        project_path = os.path.join(project_path, db_name)
        if not os.path.exists(project_path):
            os.mkdir(project_path)

        # objects folder
        for folder in _SUB_FOLDERS:
            os.mkdir(os.path.join(self.path, project_path, folder))
        return project_path

    def _write_script(self, parent, sub, object_name, script):
        safe_name = str(object_name).replace('.', '_')
        path = os.path.join(parent, sub, f'{safe_name}.sql')
        with open(path, 'w') as f:
            f.write(script)

    def _init_project(self, _server: Server, db_name):
        project_path = self._create_folder(_server, db_name)

        _db = Database(creds=_server)
        with _db.connect(db_name) as db:
            tables = db.execute(_QUERIES["TABLES"]).fetchall()
            print(f'{len(tables)} tables found on {db_name}. Generating table script...')  # noqa

            for index, table in enumerate(tables, start=1):
                try:
                    script = db.execute(
                        _QUERIES["CREATE_TABLE"],
                        table.SCHEMA_NAME,
                        table.TABLE_NAME
                    ).fetchone().SQL

                    self._write_script(project_path, "Tables", table.TABLE_NAME, script)

                    print(f'--->{index}/{len(tables)} {table.TABLE_NAME} on {db_name}')  # noqa
                except Exception as ex:
                    print(f'Failed--->{index}/{len(tables)} {table.TABLE_NAME} on {db_name}')  # noqa
                    self._failure.append([_server.server, db_name, "Tables", table.TABLE_NAME, str(ex)])

            objects = db.execute(_QUERIES["OBJECTS"]).fetchall()
            print(f'{len(objects)} objects found on {db_name}. Generating object script...')  # noqa
            for index, item in enumerate(objects, start=1):
                try:
                    self._write_script(project_path, item.SUB_FOLDER, item.OBJECT_NAME, item.SQL)
                    print(f'--->{index}/{len(objects)} {item.OBJECT_NAME} on {db_name}')
                except Exception as ex:
                    print(f'Failed--->{index}/{len(objects)} {item.OBJECT_NAME} on {db_name}')  # noqa
                    self._failure.append(_server.server, db_name, item.SUB_FOLDER, item.OBJECT_NAME, str(ex))

    def _generate(self):
        for index, server in enumerate(self._config.servers, start=1):
            _db = Database(server)
            with _db.connect("master") as db:
                databases = db.execute(_QUERIES["DATABASES"]).fetchall()
                for ix, item in enumerate(databases, start=1):
                    print(f'Server: {server.server} {index}/{len(self._config.servers)} Database: {item.DB_NAME} {ix}/{len(databases)}')  # noqa
                    self._init_project(server, item.DB_NAME)

    def run(self):
        self._generate()
        if self._failure:
            _save_csv(
                path=os.path.join(self.path, self.err_file_name),
                columns=['SERVER', 'DB_NAME', 'SUB_FOLDER', 'OBJECT_NAME', 'ERROR'],
                rows=self._failure
            )


"""
def main():
    "Console script for deploydb."
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c',
        '--config',
        type=str,
        action='store',
        help='config file path',
        required=True
    )

    parser.add_argument(
        '-e',
        '--export',
        type=str,
        action='store',
        help='export folder path',
        required=True
    )

    args = vars(parser.parse_args())
    config = args.get('config')
    export = args.get('export')

    if not config:
        raise ValueError("config parameter is required")
    if not export:
        raise ValueError("export parameter is required")
    if config and export:
        RepoGenerator(config, export).run()

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
"""


class Listener(Base):
    def __init__(
        self,
        config,
        *,
        ssh_path="~/.ssh/id_rsa",
        changelog_path="changelog.csv",
        err_path="errors.csv"
    ) -> None:
        super().__init__(config)
        self.ssh_path = ssh_path
        self.changelog_path = changelog_path
        self.err_path = err_path

    def _prep_cmd(self, file) -> Any:
        path = os.path.join(self._config.local_path, file)
        with open(path, 'r') as f:
            return f.read()

    def _run_cmd(self, server, db_name, file) -> None:
        cmd = self._prep_cmd(file)
        creds = None
        for x in self._config.servers:
            if not creds and x.server_server_alias == server:
                creds = x
            if not creds and x.server == server:
                creds = x
        if not creds:
            raise Exception(f'Server: {server} was not found in configuration!')

        _db = Database(creds)
        with _db.connect(db_name) as db:
            db.execute(cmd)

    def _pull(self):
        if self._config.ssh_url:
            print("SSH connection starting...")
            cmd = f'ssh -i {os.path.expanduser(self.ssh_path)}'
            with Git().custom_environment(GIT_SSH_COMMAND=cmd):
                Repo.clone_from(
                    self._config.ssh_url,
                    self._config.local_path,
                    branch=self._config.target_branch
                )
        elif self._config.https_url:
            print("HTTPS connection starting...")
            Repo.clone_from(
                self._config.https_url,
                self._config.local_path,
                branch=self._config.target_branch
            )
        else:
            raise Exception('No found repository!')

    def _extract_creds(self, changed_file):
        x = changed_file.split('/')
        server = x[1]
        db_name = x[2]
        return server, db_name

    def sync(self):
        if not os.path.exists(self._config.local_path):
            print(f"Initial pulling branch: {self._config.target_branch}")
            os.mkdir(self._config.local_path)
            self._pull()

        print("Checking changes...")
        repo = Repo(self._config.local_path)
        origin = repo.remotes.origin
        origin._pull()

        source_hash = _last_commit_hash(path=self.changelog_path)
        target_hash = repo.head.commit.hexsha

        failure = []
        if source_hash != target_hash:
            print("Change was detected, deploying...")
            source_commit = repo.commit(source_hash)
            target_commit = repo.commit(target_hash)
            git_diff = source_commit.diff(target_commit)

            changed_files = [f.a_path for f in git_diff]
            for item in changed_files:
                server, db_name = self._extract_creds(item)
                try:
                    self._run_cmd(server, db_name, item)
                except:  # noqa
                    failure.append(item)

            _set_commit_log(hexsha=target_hash, path=self.changelog_path)
        else:
            print("There is no new target.")

        if failure:
            columns = ['commit_hexsha', 'time', 'error']
            rows = [[target_hash, datetime.now(), str(x)] for x in failure]
            _save_csv(self.err_path, columns, rows)
