deploydb
========

Deploy your database objects automatically when the git branch is
updated.

-  Production-ready! ⚙️
-  Easy-to-use 🔨
-  Customizable 🔧

Installation
------------

Install the latest package. ``pip install deploydb``

Usage
-----

1- Create configuration file ( *json file or ``dict``* )

============================ ==========================================
Property                     Description
============================ ==========================================
``local_path``               where the local repository will be located
``https_url`` or ``ssh_url`` address to be listen
``target_branch``            branch to handle changes
``db_creds``                 a list of server credentials
============================ ==========================================

Example: ``config.json``

.. code:: json

   {
       "local_path": "",
       "https_url": "",
       "ssh_url": "",
       "target_branch": "main",
       "db_creds": {
           "driver": "ODBC Driver 17 for SQL Server",
           "server": "server-address-or-instance-name",
           "user": "your-username",
           "passw": "your-password"
       }
   }

2- Listener will listen every changes with ``sync`` method.

.. code:: python

   from deploydb import Listener

   deploy = Listener('config.json')
   deploy.sync(loop=True)

Repo Generator
~~~~~~~~~~~~~~

If you does not have any existing repository. You can easily export your
database objects then create your repository.

.. code:: python

   from deploydb import RepoGenerator

   scripter = RepoGenerator(
       config="config.json",
       export_path="path-to-export",
       includes=[],  # Default takes all databases from the given credential if not specified.
       excludes=[]
   )
   scripter.run()

``RepoGenerator`` will extract objects structure as below.

::

   .
   ├── Databases
   │   ├── Your-Db-Name
   │   │   ├── DDLs
   │   │   ├── DMLs
   │   │   ├── Functions
   │   │   ├── Stored-Procedures
   │   │   ├── Tables
   │   │   ├── Triggers
   │   │   ├── Types
   │   │   └── Views
   │   └── Database-N
   └── README.md

TODO
----

-  Creating Services for Continuous Integration

   -  Windows Service
   -  Linux Systemd Service

-  Getting Notifications

   -  Microsoft Teams Webhook Integration
   -  Slack Webhook Integration