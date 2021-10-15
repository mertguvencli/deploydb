"""Console script for deploydb."""
import os

from listener import Base
from model import Server
from database import Database
from utils import FONT, QUERIES, SUB_FOLDERS, save_csv


class RepoGenerator(Base):
    def __init__(self, config, export_path) -> None:
        super().__init__(config)
        self.path = export_path
        self._failure = []

    def _handle_server(self, server: Server) -> str:
        return server.server_alias if len(server.server_alias) > 0 else server.server

    def create_folder(self, _server: Server, db_name):
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
        for folder in SUB_FOLDERS:
            os.mkdir(os.path.join(self.path, project_path, folder))
        return project_path

    def init_project(self, _server: Server, db_name):
        project_path = self.create_folder(_server, db_name)

        _db = Database(creds=_server)
        with _db.connect(db_name) as db:
            tables = db.execute(QUERIES["TABLES"]).fetchall()
            print(f'{FONT["HEADER"]}Current DB: {db_name} {len(tables)} tables found. Generating script... {FONT["END"]}')  # noqa

            for index, table in enumerate(tables):
                try:
                    print(f'--->{index+1}/{len(tables)} {table.TABLE_NAME} {FONT["SUCCESS"]}({db_name}){FONT["END"]}')  # noqa
                    script = db.execute(
                        QUERIES["CREATE_TABLE"],
                        table.SCHEMA_NAME,
                        table.TABLE_NAME
                    ).fetchone().SQL

                    safe_file = str(table.TABLE_NAME).replace('.', '_')
                    with open(os.path.join(project_path, "Tables", f'{safe_file}.sql'), 'w') as f:
                        f.write(str(script))
                except Exception as ex:
                    print(f'{FONT["FAIL"]}--->{index+1}/{len(tables)} {table.TABLE_NAME} {FONT["SUCCESS"]}({db_name}){FONT["END"]}')  # noqa
                    self._failure.append([_server.server, db_name, "Tables", table.TABLE_NAME, str(ex)])

            objects = db.execute(QUERIES["OBJECTS"]).fetchall()
            for index, item in enumerate(objects):
                try:
                    print(f'--->{index+1}/{len(objects)} {item.OBJECT_NAME} {FONT["SUCCESS"]}({db_name}){FONT["END"]}')
                    with open(os.path.join(project_path, item.SUB_FOLDER, f'{item.OBJECT_NAME}.sql'), 'w') as f:
                        f.write(str(item.SQL))
                except Exception as ex:
                    print(f'{FONT["FAIL"]}--->{index+1}/{len(objects)} {item.OBJECT_NAME} {FONT["SUCCESS"]}({db_name}){FONT["END"]}')  # noqa
                    self._failure.append([_server.server, db_name, item.SUB_FOLDER, item.OBJECT_NAME, str(ex)])

    def generate(self):
        for server in self._config.servers:
            _db = Database(server)
            with _db.connect("master") as db:
                for item in db.execute(QUERIES["DATABASES"]).fetchall():
                    self.init_project(server, item.DB_NAME)

    def run(self):
        self.generate()
        if self._failure:
            save_csv(
                path="errors.csv",
                columns=['SERVER', 'DB_NAME', 'SUB_FOLDER', 'OBJECT_NAME', 'ERROR'],
                rows=self._failure
            )


# def main():
#     """Console script for deploydb."""
#     parser = argparse.ArgumentParser()

#     parser.add_argument(
#         '-c',
#         '--config',
#         type=str,
#         action='store',
#         help='config file path',
#         required=True
#     )

#     parser.add_argument(
#         '-e',
#         '--export',
#         type=str,
#         action='store',
#         help='export folder path',
#         required=True
#     )

#     args = vars(parser.parse_args())
#     config = args.get('config')
#     export = args.get('export')

#     if not config:
#         raise ValueError("config parameter is required")
#     if not export:
#         raise ValueError("export parameter is required")
#     if config and export:
#         RepoGenerator(config, export).run()

#     return 0


# if __name__ == "__main__":
#     sys.exit(main())  # pragma: no cover
