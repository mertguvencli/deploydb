# deploydb

Deploy your database objects automatically when the git branch is updated.

* Production-ready! ⚙️
* Easy-to-use 🔨
* Customizable 🔧


## Getting started..
1- Install the latest package. 

`pip install deploydb`

2- Add Configurations ( *json file or `dict`* )

|Property|Description|
|------------|-----------|
|`local_path`|where the local repository will be located|
|`https_url` or `ssh_url`|*https or ssh url is required*|
|`target_branch`|to trigger branch name|
|`server`|a list of server credentials|

`config.json`
```json
{
    "local_path": "E:\\deployment",
    "https_url": "",
    "ssh_url": "git@github.com:****/****.git",
    "target_branch": "main",
    "servers": [
        {
            "driver": "ODBC Driver 17 for SQL Server",
            "server": "127.0.0.1",
            "server_alias": "Staging",
            "user": "your-db-user",
            "passw": "your-password"
        },
    ]
}
```

3- Export your database objects then `push / upload` the exported files to your repository.
```python
from deploydb.repo_generator import RepoGenerator

RepoGenerator("config.json", "path-to-export").run()
```
**Repo-Generator** will extract objects structure as below.

```
root-project-folder
│
└───DataCenter-X
    │    └───DB-001
    │    │   └───Tables
    │    │   └───Views
    │    │   └───Functions
    │    │   └───Stored-Procedures
    │    │   └───Triggers
    │    │   └───Types
    │    │   └───DMLs
    │    │   └───DDLs
    |    |
    │    N-Database
    N-Server
```

4- Pull the target branch from remote and initiate `sync`.

```python
from deploydb import Listener

deploy = Listener('config.json')
deploy.sync()
```
