from pydantic import BaseModel
from typing import List, Optional


class DbCreds(BaseModel):
    driver: str
    server: str
    user: str
    passw: str


class Config(BaseModel):
    local_path: str
    https_url: Optional[str] = None
    ssh_url: Optional[str] = None
    target_branch: str
    db_creds: DbCreds
