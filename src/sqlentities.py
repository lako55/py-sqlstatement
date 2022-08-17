# Copyright (c) 2022 SQL Statement author,  see LICENSE file. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

"""SQL entities as namedtuples describing the SQL statement for both DDL and DML actions.

Illustrative structure of SQLTable entity:
    SQLTable
        ├── columns
        │   ├── SQLColumn
        │   └── SQLColumn
        │       ├── name (column name)
        │       ├── action (SQLDDLAction or SQLDMLAction)
        │       ├── type (filled for DDL statements)
        │       ├── size (filled for DDL statements, varchar type)
        │       ├── constraints (filled for DDL statements)
        │       │   ├── SQLConstraintPrimaryKey
        │       │   ├── SQLConstraintUnique
        │       │   └── SQLConstraintNotNull
        │       └── value (filled for DML statements such as INSERT or UPDATE)
        └── where
            ├── SQLAnd
            │   └── filter (Contains either:
                            1. a list with one SQLColumn with name and value. Supported simple equal operator. 
                            2. or the list contains nested condition in parentheses)
            ├── SQLAnd
            └── SQLOr
                └── filter (Contains either:
                            1. a list with one SQLColumn with name and value. Supported simple equal operator. 
                            2. or the list contains nested condition in parentheses)
"""

from collections import namedtuple

SQLEntity = namedtuple('SQLEntity', 'name action',)
SQLDatabase = namedtuple('SQLDatabase', SQLEntity._fields)
SQLTable = namedtuple('SQLTable', SQLEntity._fields + ('columns', 'where',), defaults=(None,))
SQLColumn = namedtuple('SQLColumn', SQLEntity._fields + ('type', 'size', 'constraints', 'value'), 
    defaults=(None, None, None))

SQLConstraint = namedtuple('SQLConstraint', SQLEntity._fields)
SQLConstraintPrimaryKey = namedtuple('SQLConstraintPrimaryKey', SQLConstraint._fields)
SQLConstraintUnique = namedtuple('SQLConstraintUnique', SQLConstraint._fields)
SQLConstraintNotNull = namedtuple('SQLConstraintNotNull', SQLConstraint._fields)
SQLConstraintDefault = namedtuple('SQLConstraintDefault', SQLConstraint._fields + ('value',))

SQLAnd = namedtuple('SQLAnd', 'filter',)
SQLOr = namedtuple('SQLOr', 'filter',)
