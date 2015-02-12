vsc-cluster-db
==============
Create a sqlite database with information on a compute cluster derived 
from PBS torque tools

Functionality
-------------
* `create_node_db.py`: creates the tables in a SQLite 3.x database to
    store information on the resources and features of compute nodes
* `load_node_db.py`: populate the database using the output of PBS torque
    `pbsnodes` command, and a configuration file
* `dump_node_states.py`: prints node status

Dependencies
------------
* https://github.com/gjbex/vsc-tools-lib : the `lib` directory should be
    in the `PYTHONPATH` variable
* Python 2.7.x

Reverse dependencies
--------------------
* database is used by qlint (https://github.com/gjbex/qlint)
