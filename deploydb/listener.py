import os
import sys
import traceback
from datetime import datetime
import time
from typing import Any

import pyodbc
from git import Repo, Git
from .base import Base
from .db import Database
from .model import ChangedFile
# from .utils import _set_commit_log, _last_commit_hash
from .script import (
    EXECUTION_LOG_INSERT,
    INIT_DEPLOYDB,
    GET_OBJECT,
    DUPLICATE_CONTROL,
    CHANGELOG_INSERT,
    LAST_CHANGELOG_SHA,
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
        self._init_deploydb_objects()

    def _init_deploydb_objects(self):
        with self._db().connect(self._config.db_creds.default_db) as db:
            db.execute(INIT_DEPLOYDB)

    def _is_executed(self, commit, file_path):
        with self._db().connect(self._config.db_creds.default_db) as db:
            return True if db.execute(DUPLICATE_CONTROL, commit, file_path).fetchone() else False

    def _set_changelog(self, commit) -> None:
        with self._db().connect(self._config.db_creds.default_db) as db:
            db.execute(CHANGELOG_INSERT, commit)
    
    def _last_changelog_hash(self) -> str:
        with self._db().connect(self._config.db_creds.default_db) as db:
            x = db.execute(LAST_CHANGELOG_SHA).fetchone()
            return x[0] if x else ""

    def _db(self):
        return Database(creds=self._config.db_creds)

    def _prep_cmd(self, file: ChangedFile) -> Any:
        path = os.path.join(self._config.local_path, file.path)
        command = ''
        with open(path, mode='r', encoding='utf-8') as f:
            command = f.read()

        if file.object_type == 'DMLs':
            command = 'SET NOCOUNT ON;\n' + command

        return command

    def _add_execution_log(self, commit_id, file, is_failed, error):
        with self._db().connect(self._config.db_creds.default_db) as db:
            db.execute(EXECUTION_LOG_INSERT, commit_id, file, is_failed, error)

    def _run_cmd(self, file: ChangedFile, target_hash):
        _failed = False
        _message = None
        start_time = time.time()
        with self._db().connect(file.db_name) as db:
            if self._is_executed(target_hash, file.path):
                print('Item already executed!')
            else:
                print('Executing commands ...')
                try:
                    db.execute(self._prep_cmd(file))
                    self._add_execution_log(target_hash, file.path, False, None)
                except pyodbc.ProgrammingError as ex:
                    _failed = True
                    err, _message = ex.args
                    self._add_execution_log(target_hash, file.path, True, str(_message))
                except:  # noqa
                    _failed = True
                    _message = str(traceback.format_exception(*sys.exc_info()))
                    self._add_execution_log(target_hash, file.path, True, _message)
                print('Finished commands... Elapsed Time:', time.time()-start_time)

        return _failed, _message

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
        db_name = str(x[1])
        object_type = str(x[2])
        object_name = str(x[3]).split('.sql')[0]
        return db_name, object_type, object_name

    def _is_object_exists(self, db_name, object_type, object_name):
        with self._db().connect(db_name) as db:
            exist = any(db.execute(GET_OBJECT, object_type, object_name).fetchall())
            print(f"Db:{db_name} Type:{object_type} Name:{object_name} Exists:{exist}")
            return exist

    def policy(self, file):
        """ Determine if the script be able to execute ? """
        file = ChangedFile(file)

        # If table already created, script wont execute.
        if file.object_type == "Tables":
            if self._is_object_exists(file.db_name, file.object_type, file.object_name):
                print("Item rejected!")
                return False

        return True

    def handle_changes(self, executable=True):
        """Handles changes and deploys to your server automatically.

        Args:
            executable (bool, optional): every file included in the changes is executable

        Returns:
            changes_detected
            commit_id
            is_failed
            failure_list
        """
        if not os.path.exists(self._config.local_path):
            print(f"Initial pulling branch: {self._config.target_branch}")
            os.mkdir(self._config.local_path)
            self._pull()

        print("Checking changes...", datetime.now())
        repo = Repo(self._config.local_path)
        origin = repo.remotes.origin
        origin.pull()

        source_hash = self._last_changelog_hash()
        target_hash = repo.head.commit.hexsha

        failure_list = []

        if source_hash != target_hash:
            self._set_changelog(target_hash)

            if executable:
                print("Changes detected...")
                source_commit = repo.commit(source_hash)
                target_commit = repo.commit(target_hash)
                git_diff = target_commit.diff(source_commit)

                changes = [ChangedFile(f.a_path) for f in git_diff if str(f.a_path).lower().endswith('.sql')]

                for file in sorted(changes, key=lambda x: x.sequence):
                    # Some files may be removed the folders therefore checking...
                    file_exists = os.path.exists(
                        os.path.join(self._config.local_path, file.path)
                    )

                    if file_exists:
                        print("Changed file:", file.path)
                        # Refers customized applied policies.
                        # Pre-defined rules are listed. You may customize that.
                        # Say for instance:
                        # Prevent DDL commands side affects over existing table.
                        if self.policy(file=file.path):
                            failed, msg = self._run_cmd(file, target_hash)

                            if failed:
                                failure_list.append([file.path, msg])

                return target_hash, True if failure_list else False, failure_list
