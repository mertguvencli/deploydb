# deploydb

Deploy your database objects automatically when the git branch is updated.

* Production-ready! ⚙️
* Easy-to-use 🔨
* Customizable 🔧


## Installation
Install the latest package. `pip install deploydb`


## Usage
1- Create configuration file ( *json file or `dict`* )

|Property|Description|
|------------|-----------|
|`local_path`|where the local repository will be located|
|`https_url` or `ssh_url`|address to be listen|
|`target_branch`|branch to handle changes|
|`servers`|a list of server credentials|

Example: `config.json`
```json
{
    "local_path": "",
    "https_url": "",
    "ssh_url": "",
    "target_branch": "",
    "servers": [
        {
            "driver": "ODBC Driver 17 for SQL Server",
            "server": "server-address-or-instance-name",
            "server_alias": "Staging",
            "user": "your-username",
            "passw": "your-password"
        }
    ]
}
```

2- Listener will listen every changes with `sync` method.

```python
from deploydb import Listener

deploy = Listener('config.json')
deploy.sync(loop=True)
```


### Repo Generator
If you does not have any existing repository. You can easily export your database objects then create your repository.
```python
from deploydb import RepoGenerator

generator = RepoGenerator(config="config.json", export_path="path-to-export")
generator.run()
```
`RepoGenerator` will extract objects structure as below.

```
path-to-export
│
└───Server-X
    │    └───DB-X
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

*(TODO) Creating a Windows Service*
