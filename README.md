# VoltDB Example App

Use Case
--------

This is a template with a very simple single-table use case.  It is meant to be replaced with your own DDL and data generation code to make it easier to create a benchmark or test concepts using VoltDB.


Code organization
-----------------
The code is divided into projects:

- "db": the database project, which contains the schema, stored procedures and other configurations that are compiled into a catalog and run in a VoltDB database.
- "client": a java client that generates data and submits calls to the database.

See below for instructions on running these applications.  For any questions,
please contact bballard@voltdb.com.

Pre-requisites
--------------

Before running these scripts you need to have VoltDB 5.0 or later installed.  If you choose the .tar.gz file distribution, simply untar it to a directory such as your $HOME directory, then add the bin subdirectory to your PATH environment variable.  For example:

    export PATH="$PATH:$HOME/voltdb-ent-5.3/bin"

You may choose to add this to your .bashrc file.

If you installed the .deb or .rpm distribution, the binaries should already be in your PATH.  To verify this, the following command should return a version number:

    voltdb --version

Instructions
------------

Start the database

    cd db
    voltdb create -d deployment-demo.xml -B

Load the schema

    cd db
    ./compile_procs.sh
    sqlcmd < ddl.sql

Run the client

    cd client
    ./run_client.sh


Stop the database

    voltadmin shutdown
