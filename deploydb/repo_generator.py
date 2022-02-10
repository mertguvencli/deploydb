import os
import sys
import traceback
from tqdm import tqdm

from .base import Base
from .db import Database
from .utils import _save_csv
from .script import DATABASES, OBJECTS, CREATE_TABLE


class RepoGenerator(Base):
    """It will create your database object's script that you need.

    Args:
        config (Any): config file or a `dict`.
        export_path (str): where the exported files locate
        includes (list, optional): default takes all databases from the given credential.
        excludes (list, optional): exclude databases from the given credential.
        err_file_path (str, optional): where the errors locate. Defaults to "errors.csv".

    Example:
        from deploydb import RepoGenerator

        scripter = RepoGenerator(
            config="config.json",
            export_path="path-to-export",
            includes=[],
            excludes=[]
        )
        scripter.run()
    """
    def __init__(
        self,
        *,
        config,
        export_path,
        includes=[],
        excludes=[],
        err_file_path="errors.csv"
    ) -> None:
        super().__init__(config)
        self.path = export_path
        self.includes = includes
        self.excludes = excludes
        self.err_file_path = err_file_path
        self._failure = []

        self.sub_folders = (
            {'Tables': '# Tables'},
            {'Views': '# Views'},
            {'Functions': '# Functions'},
            {'Stored-Procedures': '# Stored-Procedures'},
            {'Triggers': '# Triggers'},
            {'Types': '# User Defined Data Types'},
            {'DMLs': '# DMLs - Data Manipulations'},
            {'DDLs': '# DDLs - Data Definitions'}
        )
        self._export_path_check()

    def _export_path_check(self):
        if os.path.exists(self.path):
            raise ValueError(f'<export_path> folder exists! Please type a does not exist folder name.\nPath: {self.path}')  # noqa

    def _create_folder(self, db_name):
        # base folder
        if not os.path.exists(self.path):
            os.mkdir(self.path)

        # wrapper folder
        project_path = os.path.join(self.path, 'Databases')
        if not os.path.exists(project_path):
            os.mkdir(project_path)

        # db folder
        project_path = os.path.join(project_path, db_name)
        if not os.path.exists(project_path):
            os.mkdir(project_path)

        # objects folder
        for folder in self.sub_folders:
            # add README.md file
            _folder = list(folder.keys())[0]
            os.mkdir(os.path.join(self.path, project_path, _folder))

            with open(os.path.join(self.path, project_path, _folder, f'{_folder}_README.md'), 'w') as f:
                f.write(folder.get(_folder))

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
            for line in str(script).split('\n'):
                f.write(line)

        total_rows = None
        with open(path, mode='r', encoding='utf-8') as x:
            total_rows = len(x.readlines())

        if total_rows == 1:
            with open(path, mode='w', encoding='utf-8') as f:
                for line in str(script).split('\n'):
                    f.write(line + '\n')

    def _init_project(self, db_name, max_name_len):
        project_path = self._create_folder(db_name)
        progress = db_name + (" " * (max_name_len - len(db_name)))
        _db = Database(creds=self._config.db_creds)
        with _db.connect(db_name) as db:
            objects = db.execute(OBJECTS).fetchall()
            for item in tqdm(objects, desc=progress, colour="green"):
                try:
                    if item.SUB_FOLDER == "Tables":
                        script = db.execute(
                            CREATE_TABLE,
                            item.SCHEMA_NAME,
                            item.OBJECT_NAME
                        ).fetchone().SQL
                        self._write_script(project_path, item.SUB_FOLDER, item.SCHEMA_NAME, item.OBJECT_NAME, script)
                    else:
                        self._write_script(project_path, item.SUB_FOLDER, item.SCHEMA_NAME, item.OBJECT_NAME, item.SQL)
                except:  # noqa
                    error = str(traceback.format_exception(*sys.exc_info()))
                    self._failure.append([db_name, item.SUB_FOLDER, item.OBJECT_NAME, error])

    def _generate(self):
        _db = Database(creds=self._config.db_creds)
        with _db.connect("master") as db:
            databases = None

            if self.includes:
                databases = self.includes
            else:
                databases = [x.DB_NAME for x in db.execute(DATABASES).fetchall()]

            max_name_len = max([len(x) for x in databases])
            databases = [x for x in databases if x not in self.excludes]

            for x in databases:
                self._init_project(x, max_name_len)

    def run(self):
        self._generate()
        if self._failure:
            _save_csv(
                path=os.path.join(self.path, self.err_file_path),
                columns=['DB_NAME', 'SUB_FOLDER', 'OBJECT_NAME', 'ERROR'],
                rows=self._failure
            )
