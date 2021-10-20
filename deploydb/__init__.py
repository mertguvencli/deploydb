"""Top-level package for deploydb."""
__author__ = 'Mert Guvencli'
__email__ = 'guvenclimert@gmail.com'
__version__ = '0.2.1'

import os
import sys
import traceback
from datetime import datetime
import time
import json
from typing import Any

from git import Repo, Git
from .model import Server, Config
from .db import Database
from .utils import _save_csv, _set_commit_log, _last_commit_hash
from .script import DATABASES, OBJECTS, TABLES, CREATE_TABLE, GET_OBJECT


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


class RepoGenerator(Base):
    """It will create your database object's script that you need.

    Args:
        config (Any): config file or a `dict`.
        export_path (str): where the exported files locate
        databases (list, optional): default takes all databases from the given credential.
        err_file_name (str, optional): where the errors locate. Defaults to "errors.csv".

    Example:
        from deploydb import RepoGenerator

        scripter = RepoGenerator(
            config="config.json",
            export_path="path-to-export",
            databases=['MyDbName']
        )
        scripter.run()
    """
    def __init__(
        self,
        *,
        config,
        export_path,
        databases=[],
        err_file_name="errors.csv"
    ) -> None:
        super().__init__(config)
        self.path = export_path
        self.databases = databases
        self.err_file_name = err_file_name
        self._failure = []

        self.sub_folders = (
            'Tables',
            'Views',
            'Functions',
            'Stored-Procedures',
            'Triggers',
            'Types',
            'DMLs',
            'DDLs'
        )

    def _handle_server(self, server: Server) -> str:
        return server.server_alias if len(server.server_alias) > 0 else server.server

    def _create_folder(self, _server: Server, db_name):
        # server folder
        server = self._handle_server(_server)

        if not os.path.exists(self.path):
            os.mkdir(self.path)
        project_path = os.path.join(self.path, server)

        if not os.path.exists(project_path):
            os.mkdir(project_path)

        # db folder
        project_path = os.path.join(project_path, db_name)
        if not os.path.exists(project_path):
            os.mkdir(project_path)

        # objects folder
        for folder in self.sub_folders:
            os.mkdir(os.path.join(self.path, project_path, folder))
        return project_path

    def _safe_file_name(self, schema_name, object_name) -> str:
        allowed_chars = 'abcdefghijklmnopqrstuvwxyz_0123456789'

        found = False
        for char in str(object_name).lower():
            if char not in allowed_chars:
                found = True
                break

        if found or schema_name != "dbo":
            object_name = f"[{object_name}]"

        if schema_name == "dbo":
            schema_name = ""
        else:
            schema_name = f"[{schema_name}]."

        return f"{schema_name}{object_name}.sql"

    def _write_script(self, parent, sub, schema_name, object_name, script):
        safe_name = self._safe_file_name(schema_name, object_name)
        path = os.path.join(parent, sub, safe_name)
        with open(path, mode='w', encoding='utf-8') as f:
            f.write(script)

    def _init_project(self, _server: Server, db_name):
        project_path = self._create_folder(_server, db_name)

        _db = Database(creds=_server)
        with _db.connect(db_name) as db:
            tables = db.execute(TABLES).fetchall()
            print(f'{len(tables)} tables found on {db_name}. Generating table script...')  # noqa

            for index, table in enumerate(tables, start=1):
                try:
                    script = db.execute(
                        CREATE_TABLE,
                        table.SCHEMA_NAME,
                        table.TABLE_NAME
                    ).fetchone().SQL

                    self._write_script(project_path, "Tables", table.SCHEMA_NAME, table.TABLE_NAME, script)

                    print(f'--->{index}/{len(tables)} {table.TABLE_NAME} on {db_name}')
                except:  # noqa
                    error = str(traceback.format_exception(*sys.exc_info()))
                    print(f'Failed--->{index}/{len(tables)} {table.TABLE_NAME} on {db_name}')
                    self._failure.append([_server.server, db_name, "Tables", table.TABLE_NAME, error])

            objects = db.execute(OBJECTS).fetchall()
            print(f'{len(objects)} objects found on {db_name}. Generating object script...')  # noqa
            for index, item in enumerate(objects, start=1):
                try:
                    self._write_script(project_path, item.SUB_FOLDER, item.SCHEMA_NAME, item.OBJECT_NAME, item.SQL)
                    print(f'--->{index}/{len(objects)} {item.OBJECT_NAME} on {db_name}')
                except:  # noqa
                    error = str(traceback.format_exception(*sys.exc_info()))
                    print(f'Failed--->{index}/{len(objects)} {item.OBJECT_NAME} on {db_name}')  # noqa
                    self._failure.append(_server.server, db_name, item.SUB_FOLDER, item.OBJECT_NAME, error)

    def _generate(self):
        for index, server in enumerate(self._config.servers, start=1):
            _db = Database(server)
            with _db.connect("master") as db:
                db_list = None
                if self.databases:
                    db_list = self.databases
                else:
                    db_list = [x.DB_NAME for x in db.execute(DATABASES).fetchall()]

                for ix, db_name in enumerate(db_list, start=1):
                    print(f'Server: {server.server} {index}/{len(self._config.servers)} Database: {db_name} {ix}/{len(db_list)}')  # noqa
                    self._init_project(server, db_name)

    def run(self):
        self._generate()
        if self._failure:
            _save_csv(
                path=os.path.join(self.path, self.err_file_name),
                columns=['SERVER', 'DB_NAME', 'SUB_FOLDER', 'OBJECT_NAME', 'ERROR'],
                rows=self._failure
            )


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

    def _server_creds(self, server):
        creds = None
        for x in self._config.servers:
            if not creds and x.server_alias == server:
                creds = x
            if not creds and x.server == server:
                creds = x
        if not creds:
            raise Exception(f'Server: {server} was not found in configuration!')
        return creds

    def _db(self, server):
        return Database(self._server_creds(server))

    def _prep_cmd(self, file) -> Any:
        path = os.path.join(self._config.local_path, file)
        with open(path, mode='r', encoding='utf-8') as f:
            return f.read()

    def _run_cmd(self, server, db_name, file) -> None:
        cmd = self._prep_cmd(file)
        with self._db(server).connect(db_name) as db:
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
        server = str(x[0])
        db_name = str(x[1])
        object_type = str(x[2])
        object_name = str(x[3]).split('.')[0]
        return server, db_name, object_type, object_name

    def _is_object_exists(self, server, db_name, object_type, object_name):
        print("Is object exists", server, db_name, object_type, object_name)
        with self._db(server).connect(db_name) as db:
            res = any(db.execute(GET_OBJECT, object_type, object_name).fetchall())
            print(f"{object_name} is exists : {res}")
            return res

    def policy(self, file):
        """ Determine if the script be able to execute ? """
        server, db_name, object_type, object_name = self._extract_creds(file)

        # If table already created, script wont execute.
        if object_type == "Tables":
            if self._is_object_exists(server, db_name, object_type, object_name):
                print("Item rejected!")
                return False

        return True

    def sync(self, loop=False, sleep=5, retry=3):
        """Handles changes and deploy to your server automatically.

        Args:
            loop (bool, optional): creates infinite loop to handle changes. Defaults to False.
            sleep (int, optional): determines how many seconds will run. Defaults to 5.
            retry (int, optional): if any error occurs how many times will retry. Defaults to 3.
        """
        if not os.path.exists(self._config.local_path):
            print(f"Initial pulling branch: {self._config.target_branch}")
            os.mkdir(self._config.local_path)
            self._pull()

        def changes():
            print("Checking changes...", datetime.now())
            repo = Repo(self._config.local_path)
            origin = repo.remotes.origin
            origin.pull()

            source_hash = _last_commit_hash(path=self.changelog_path)
            target_hash = repo.head.commit.hexsha

            failure = []
            if source_hash != target_hash:
                print("Changes detected...")
                source_commit = repo.commit(source_hash)
                target_commit = repo.commit(target_hash)
                git_diff = source_commit.diff(target_commit)

                changed_files = [f.a_path for f in git_diff]
                for item in changed_files:
                    if str(item).endswith('.sql'):
                        print("Changed file:", item)
                        server, db_name, object_type, object_name = self._extract_creds(item)

                        # Refers customized applied policies.
                        # Pre-defined rules are listed. You may customize that.
                        # Say for instance:
                        # Prevent DDL commands side affects over existing table.
                        if self.policy(file=item):
                            try:
                                self._run_cmd(server, db_name, file=item)
                            except:  # noqa
                                error = str(traceback.format_exception(*sys.exc_info()))
                                failure.append([item, error])

                _set_commit_log(hexsha=target_hash, path=self.changelog_path)

            if failure:
                columns = ['commit_hexsha', 'time', 'object', 'error']
                rows = [[target_hash, datetime.now(), x[0], x[1]] for x in failure]
                _save_csv(self.err_path, columns, rows)

        changes()
        while loop:
            if retry > 0:
                try:
                    changes()
                    time.sleep(sleep)
                except:  # noqa
                    retry -= 1
                    print(f"Remaining retry: {retry}")
