import os
import json
from .model import Config
from .db import Database


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
            try:
                _db = Database(self._config.db_creds)
                with _db.connect() as db:
                    db.execute("SELECT NULL").fetchone()
            except:  # noqa
                raise ValueError('Database connection failed!')
