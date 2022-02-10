from pydantic import BaseModel
from typing import Optional


class DbCreds(BaseModel):
    driver: str
    server: str
    user: str
    passw: str
    default_db: str
    timeout: int


class Config(BaseModel):
    local_path: str
    https_url: Optional[str] = None
    ssh_url: Optional[str] = None
    target_branch: str
    db_creds: DbCreds


class ChangedFile:
    def __init__(self, path: str) -> None:
        execution_sequence = ('Types', 'Tables', 'DDLs', 'Functions', 'Views', 'Stored-Procedures', 'Triggers', 'DMLs')  # noqa

        if path.endswith('.sql'):
            self.items = [str(x) for x in path.split('/')]
            self.db_name = self.items[1]
            self.object_type = self.items[2]
            self.object_name = self.items[3].split('.sql')[0]
            self.sequence = execution_sequence.index(self.object_type)
            self.path = path
