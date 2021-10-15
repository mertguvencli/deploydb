"""Top-level package for deploydb."""

from datetime import datetime
import os
import json
from typing import Any
from git import Repo, Git

from model import Config
from utils import FONT, save_csv, set_commit_log, last_commit_hash
from database import Database


class Base:
    """ Check and validate configuration. """
    def __init__(self, config) -> None:
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
                    print(f"{FONT['SUCCESS']}Database connection successfully created!{FONT['END']}")
                    db.execute("SELECT 1").fetchone()


class Listener(Base):
    def __init__(
        self,
        config,
        *,
        ssh_path="~/.ssh/id_rsa",
        err_path="error.txt"
    ) -> None:
        super().__init__(config)
        self.ssh_path = ssh_path
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

    def pull(self):
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

    def extract_creds(self, changed_file):
        x = changed_file.split('/')
        server = x[1]
        db_name = x[2]
        return server, db_name

    def sync(self):
        if not os.path.exists(self._config.local_path):
            os.mkdir(self._config.local_path)
            self.pull()

        repo = Repo(self._config.local_path)
        origin = repo.remotes.origin
        origin.pull()

        source_hash = last_commit_hash()
        target_hash = repo.head.commit.hexsha

        failure = []
        if source_hash != target_hash:
            source_commit = repo.commit(source_hash)
            target_commit = repo.commit(target_hash)
            git_diff = source_commit.diff(target_commit)

            changed_files = [f.a_path for f in git_diff]
            print("Checking changes...")
            for item in changed_files:
                server, db_name = self.extract_creds(item)
                try:
                    self._run_cmd(server, db_name, item)
                except:  # noqa
                    failure.append(item)

            set_commit_log(target_hash)
        else:
            print("There is no new target.")

        if failure:
            columns = ['commit_hexsha', 'time', 'error']
            rows = [[target_hash, datetime.now(), str(x)] for x in failure]
            save_csv(path=self.err_path, columns=columns, rows=rows)
            print(f"{FONT['FAIL']}Failed Items:\n {failure}{FONT['END']}")
