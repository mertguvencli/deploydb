from pydantic import BaseModel
from typing import List, Optional


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
