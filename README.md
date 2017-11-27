# 14848
14848 Spring 17

ScrewyDB NoSQL Database

Overview:
    This repository contains the implementation of a columnar NoSQL database
    that we call ScrewyDB. Its namesake refers to our level of confidence in
    the db as a production database.

    A query language is not provided for this DB. Instead, users may access
    the DB using API calls provided.

Usage:
    To run the db, run the following commands:
    1) `make`
    2) `./col`

    This will run all of the tests specified in the test.cpp file.

File Hierarchy:

    - inc/
        Contains all header files for class and function prototypes.

    - column.cpp
        Contains definitions for all classes related to the implementation of
        a single column.

    - db.cpp
        Contains definitions for all classes related to the higher level
        layers and user level interactions with our db.

    - test.cpp
        Contains definitions for all classes related to automated, random
        testing of our database.

        This file also contains the main() entrypoint for the application.

API:

    The user facing API can be found in inc/db.h A summary of the API is
    below:

    - new_column_family:
        Produces a new column family in the database. A
        column family must have a defined schema at creation time. It is not
        legal to add or remove columns from the column family at any time.

    - delete_column_family:
        Deletes an existing column family and removes all associated storage
        on disk.

    - join:
        Provides SQL-like functionality for joins. Join only produces a
        "table-view" for a user. Backend data structures are not modified
        when a join is performed. Currently, join will merge every column
        in all of the column families to be joined. WHERE queries are not
        supported on JOIN operations at this time.

    - select
        Provides SQL-like funcitonality for selects. SELECT only produces
        a "table-view" for a user. Select can only perform WHERE queries that
        specify some range of the row keyspace. WHERE queries on specific
        values within column families are not supported.

    - insert
        Adds a value to each of the columns in a column family using the same
        row key for each column.

    - delete
        Deletes a value from each column using the same row key.

    - compare
        This operation allows the user to specify a comparison function and
        to search an entire column using the function. Based on the fxn,
        a user can find the min, max, or a specific value out of the column.

    - aggregate
        This operation allows the user to perform a reduce function across
        all entries in a specific column.

    - cross
        This operation allows the user to perform an operation across two
        columns in a db and see a view of the result. For example, if the user
        were to specify a multiplication function, the user could query a view
        of two columns multipled against eachother.
