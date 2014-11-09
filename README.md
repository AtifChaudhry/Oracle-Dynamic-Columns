

An Oracle PL/SQL package to support packing variable number of virtual columns inside a single SQL table column.

This package implements functionality equivalent to that offered by MariaDB-5.3 Dynamic Columns:

    "Dynamic columns allows one to store different sets of columns for each row in a table. It works by storing a set of columns in a blob and having a small set of functions to manipulate it. Dynamic columns should be used when it is not possible to use regular columns. A typical use case is when one needs to store items that may have many different attributes (like size, color, weight, etc), and the set of possible attributes is very large and/or unknown in advance. In that case, attributes can be put into dynamic columns."

