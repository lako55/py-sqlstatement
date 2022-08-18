## About sqlstatement python library
Python library sqlparse is a great start when it comes to processing SQL statements programatically. However it is more complex to leverage the output since it is a tree form of every word in SQL statement called tokens. The intention of this project is to wrap the sqlparse output with the conscise structure of the entities describing the specific sql statement in a form of a table with specific action such as CREATE or SELECT FROM while providing all the meta data (column names, types, values to be inserted) related to that sql action.

Deliverable of this project is a python library called SQLStatement.

## Getting Started

Follow these steps to try out sqlstatement library in a safe environment:
1. clone this repo
2. open terminal and change directory to the repository folder
3. type following commands in terminal
```bash
$ python -m venv env
$ source env/bin/activate
$ pip install sqlparse
$ python
```
4. run following statements in python shell 
```python
>>> from sqlstatement.sql import SQLEntityFactory

>>> sql = "SELECT firstname, age FROM persons WHERE lastname = 'Doe';"
>>> sqlentity = SQLEntityFactory.create_entity(sql)

>>> print(sqlentity)
```
5. Formatted output in bash. Every entity is a namedtuple such as SQLTable or SQLColumn. See sqlentities python module for more details about all the entities. 
```bash
SQLTable(
    name='persons', 
    action=<SQLDMLAction.SELECT: 'SELECT'>, 
    columns=[
        SQLColumn(
            name='firstname', 
            action=<SQLDMLAction.SELECT: 'SELECT'>, 
            type=None, 
            size=None, 
            constraints=[], 
            value=None), 
        SQLColumn(
            name='age', 
            action=<SQLDMLAction.SELECT: 'SELECT'>, 
            type=None, 
            size=None, 
            constraints=[], 
            value=None)
    ], 
    where=[
        SQLAnd(
            filter=[
                SQLColumn(
                    name='lastname', 
                    action=<SQLDMLAction.WHEREEQUAL: 'WHEREEQUAL'>, 
                    type=None, 
                    size=None, 
                    constraints=None, 
                    value='Doe')
            ])
    ])
```

### Install from pypi.org
Library can be installed also as a python package. Open terminal windeos and type
```bash
$ pip install sqlstatement
```

## Supported SQL Statements
Please refer to test_sql python module in this repository for the list of all the sql statements which are supported and passed the test.

## Future Features

1. support of all the different SQL flavours => TBD
2. parsing the foreign key in CREATE/ALTER TABLE
3. enriching parsing of WHERE clause with different operators. Currently only equal operator is supported

## License
BSD 3-Clause License
