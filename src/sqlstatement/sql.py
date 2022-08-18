# Copyright (c) 2022 SQL Statement author, see LICENSE file. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

"""Core of the SQLStatement functionality. SQLEntityFactory handles parsing and 
analysis of the sql statement string."""

from io import UnsupportedOperation
from typing import List
from sqlparse import parse
from sqlparse.sql import Statement, Token, Parenthesis, Comparison, Where
import sqlparse.tokens as TType

from .sqlentities import (SQLDatabase, SQLTable, SQLColumn, SQLConstraint, SQLConstraintUnique, 
    SQLConstraintNotNull, SQLConstraintPrimaryKey, SQLAnd, SQLOr)
from .sqlactions import SQLDDLAction, SQLDMLAction
from . import sqlparseutils


actionmap = {
    "IdentifierList": SQLDMLAction.INSERT,
    "Comparison": SQLDMLAction.UPDATE
}

class SQLEntityFactory:

    """Factory creating structure of SQL entities as metadata based on analysis
    of the sqlparse library output.

    Usage:
    entity: SQLEntity = SQLEntityFactory.create_entity("<SQL statement string>")

    The output is either SQLDatabase or SQLTable. Please see doc string in module sqlentities
    for more details about the structure.
    """

    @classmethod
    def __normalize_funcname(cls, funcname: str):

        funcname = funcname.upper()
        if funcname.startswith("CREATETABLE"):
            funcname = "CREATETABLE"
        elif funcname.startswith("ALTERTABLEADD") and not funcname in [
                "ALTERTABLEADDCONSTRAINTUNIQUE",
                "ALTERTABLEADDCONSTRAINTPRIMARYKEY"]:
            funcname = "ALTERTABLEADD"

        return funcname

    @classmethod
    def create_entity(cls, sql: str):
        """Creates SQLDatabase or SQLTable by analysis of provided SQL string.
        Please see doc string in module sqlentities
        for more details about the structure."""
        statement: Statement = parse(sql)[0]
        keywords = list(map(lambda token: token.value, 
                filter(lambda token: token.is_keyword, statement.tokens)
            )
        )
        funcname = cls.__normalize_funcname("".join(keywords).replace(' ', '').upper())

        return SQLProcessor[funcname](statement)

    @classmethod
    def __getnamesfrom(cls, func, sql: Statement):

        return list(
            map(lambda token: token.value,
                list(filter(func, sql.flatten()))
            )
        )

    @classmethod
    def __gettypesfrom(cls, func, sql: Statement):

        return list(
            map(sqlparseutils.getcolumntype,
                list(filter(func, sql.flatten()))
            )
        )

    @classmethod
    def __getvaluesfrom(cls, func, sql: Statement):

        return list(
            map(lambda token: token.value,
                list(filter(func, sql.flatten()))
            )
        )

    @classmethod
    def __getwhereconditionoperator(cls, token: Token):

        operators = list(filter(lambda t: t.ttype == TType.Operator.Comparison, token.tokens))
        if operators:
            match operators[0].normalized:
                case "=":
                    return SQLDMLAction.WHEREEQUAL
                case "LIKE":
                    return SQLDMLAction.WHERELIKE
                case _:
                    raise UnsupportedOperation()

        return None

    @classmethod
    def __mapwherecondition(cls, andortoken: Token):

        token: Token = sqlparseutils.getnexttoken(andortoken)
        if isinstance(token, (Comparison)):
            condition=[SQLColumn(name=token.left.value,
                action=cls.__getwhereconditionoperator(token),
                type=None, size=None, constraints=None, value=str(token.right.value).strip("'"))
            ]
        elif isinstance(token, (Parenthesis)):
            condition=cls.__getfilterconditions(token.tokens)

        match andortoken.normalized:
            case "WHERE":
                return SQLAnd(filter=condition)
            case "AND":
                return SQLAnd(filter=condition)
            case "(":
                return SQLAnd(filter=condition)
            case "OR":
                return SQLOr(filter=condition)
            case _:
                return (None, None)

    @classmethod
    def __getfilterconditions(cls, tokens: List[Token]):

        return list(
            map(cls.__mapwherecondition,
                list(filter(sqlparseutils.isandor, tokens))
            )
        )

    @classmethod
    def __getactionfrom(cls, func, sql: Statement):        

        return list(
            map(lambda token: actionmap[type(token.parent).__name__],
                list(filter(func, sql.flatten()))
            )
        )

    @classmethod
    def __getwhere(cls, sql: Statement):

        wheretokens: List[Token] = list(filter(lambda token: isinstance(token, (Where)), sql.tokens))
        if wheretokens:
            return cls.__getfilterconditions(wheretokens[0])

        return None

    @classmethod
    def __map_constraints(cls, colname: str, sql: Statement):

        tokens: List = list(sql.flatten())

        def isconstraint(token: Token):
            if token.normalized in ["PRIMARY", "NOT NULL", "UNIQUE"]:
                idx = tokens.index(token)
                subtokens = tokens[:idx-1]
                tkn: Token

                for tkn in reversed(subtokens):
                    if sqlparseutils.iscolumnname(tkn):
                        break

                return tkn.value == colname

            return False       

        def mapconstraint(token: Token):
            match token.normalized:
                case "UNIQUE":
                    return SQLConstraintUnique(name="unique", action=SQLDDLAction.ADDCONSTRAINT)
                case "PRIMARY":
                    return SQLConstraintPrimaryKey(name="primarykey", action=SQLDDLAction.ADDCONSTRAINT)
                case "NOT NULL":
                    return SQLConstraintNotNull(name="notnull", action=SQLDDLAction.ADDCONSTRAINT)
                case _:
                    raise Exception("Unsupported constraint")

        constraints = list(map(mapconstraint, filter(isconstraint, tokens)))
        return constraints

    @classmethod
    def create_sqldatabase(cls, sql: Statement):

        dbname, *_ = cls.__getnamesfrom(sqlparseutils.is_db_or_tablename, sql)
        return SQLDatabase(name=dbname, action=SQLDDLAction.CREATE)

    @classmethod
    def drop_sqldatabase(cls, sql: Statement):

        dbname, *_ = cls.__getnamesfrom(sqlparseutils.is_db_or_tablename, sql)
        return SQLDatabase(name=dbname, action=SQLDDLAction.DROP)

    @classmethod
    def __map_sqltable(cls, sql: Statement, tableaction: SQLDDLAction, columnaction: SQLDDLAction):
        """Extracts and maps the sql data to the SQLStatement data structure"""
        tablename, *_ = cls.__getnamesfrom(sqlparseutils.is_db_or_tablename, sql)
        columnnames = cls.__getnamesfrom(sqlparseutils.iscolumnname, sql)

        types = cls.__gettypesfrom(sqlparseutils.iscolumntype, sql)
        if not types:
            types = [(None, None)] * len(columnnames)

        zipped = list(zip(columnnames, types))

        columns = list(map(
                lambda column: SQLColumn(name=column[0], action=columnaction, type=column[1][0], 
                    size=column[1][1], constraints=cls.__map_constraints(column[0], sql)),
                zipped
            )
        )

        where = cls.__getwhere(sql)

        return SQLTable(name=tablename, action=tableaction, columns=columns, where=where)

    @classmethod
    def create_sqltable(cls, sql: Statement):
        """Analyzes the CREATE TABLE sql statement."""
        return cls.__map_sqltable(sql, SQLDDLAction.CREATE, SQLDDLAction.CREATE)

    @classmethod
    def drop_sqltable(cls, sql: Statement):
        """Analyzes the DROP TABLE sql statement."""
        tablename, *_ = cls.__getnamesfrom(sqlparseutils.is_db_or_tablename, sql)

        return SQLTable(name=tablename, action=SQLDDLAction.DROP, columns=[]) 

    @classmethod
    def alter_sqltableaddcolumn(cls, sql: Statement):
        """Analyzes the ALTER TABLE ADD COLUMN sql statement."""
        return cls.__map_sqltable(sql, SQLDDLAction.ALTER, SQLDDLAction.ADDCOLUMN)

    @classmethod
    def alter_sqltabledropcolumn(cls, sql: Statement):
        """Analyzes the ALTER TABLE DROP COLUMN sql statement."""
        return cls.__map_sqltable(sql, SQLDDLAction.ALTER, SQLDDLAction.DROPCOLUMN)


    @classmethod
    def alter_sqltablemodifycolumn(cls, sql: Statement):
        """Analyzes the ALTER TABLE MODIFY COLUMN sql statement."""
        return cls.__map_sqltable(sql, SQLDDLAction.ALTER, SQLDDLAction.MODIFYCOLUMN)

    @classmethod
    def alter_sqltablemodifynotnull(cls, sql: Statement):
        """Analyzes the ALTER TABLE MODIFY NOT NULL sql statement."""
        tablename, *_ = cls.__getnamesfrom(sqlparseutils.is_db_or_tablename, sql)
        columnnames = cls.__getnamesfrom(sqlparseutils.iscolumnname, sql)

        types = cls.__gettypesfrom(sqlparseutils.iscolumntype, sql)

        zipped = list(zip(columnnames, types))

        columns = list(map(
                lambda column: SQLColumn(name=column[0], action=SQLDDLAction.ADDCONSTRAINT, 
                    type=column[1][0], size=column[1][1], constraints=[
                        SQLConstraintNotNull(name='notnull', action=SQLDDLAction.ADDCONSTRAINT)
                    ]),
                zipped
            )
        )

        return SQLTable(name=tablename, action=SQLDDLAction.ADDCONSTRAINT, columns=columns)


    @classmethod
    def __map_sqltableconstraint(cls, columnnames: List[str], action: SQLDDLAction, constraint):

        return list(map(
                lambda name: SQLColumn(name=name, action=action, 
                    type=None, size=None, constraints=[constraint]),
                columnnames
            )
        )

    @classmethod
    def alter_sqltableaddconstraint(cls, sql: Statement, constrainttype: type):
        """Analyzes the ALTER TABLE ADD CONSTRAINT sql statement."""
        tablename, constraintname = cls.__getnamesfrom(sqlparseutils.is_db_or_tablename, sql)
        columnnames = cls.__getnamesfrom(sqlparseutils.iscolumnname, sql)

        columns = cls.__map_sqltableconstraint(columnnames, SQLDDLAction.ADDCONSTRAINT, 
            constrainttype(name=constraintname, action=SQLDDLAction.ADDCONSTRAINT))
        return SQLTable(name=tablename, action=SQLDDLAction.ADDCONSTRAINT, columns=columns)

    @classmethod
    def alter_sqltableaddconstraintunique(cls, sql: Statement):
        """Analyzes the ALTER TABLE ADD CONSTRAINT UNIQUE sql statement."""
        return cls.alter_sqltableaddconstraint(sql, SQLConstraintUnique)

    @classmethod
    def alter_sqltableaddconstraintprimarykey(cls, sql: Statement):
        """Analyzes the ALTER TABLE ADD CONSTRAINT PRIMARY KEY sql statement."""
        return cls.alter_sqltableaddconstraint(sql, SQLConstraintPrimaryKey)

    @classmethod
    def alter_sqltabledropconstraint(cls, sql: Statement):
        """Analyzes the ALTER TABLE DROP CONSTRAINT sql statement."""
        tablename, constraintname = cls.__getnamesfrom(sqlparseutils.is_db_or_tablename, sql)
        columns = cls.__map_sqltableconstraint(['*'],
            SQLDDLAction.DROPCONSTRAINT, 
            SQLConstraint(name=constraintname, action=SQLDDLAction.DROPCONSTRAINT)
        )

        return SQLTable(name=tablename, action=SQLDDLAction.DROPCONSTRAINT, columns=columns)

    @classmethod
    def __map_sqldata(cls, sql: Statement, tableaction: SQLDMLAction):

        tablename, *_ = cls.__getnamesfrom(sqlparseutils.is_db_or_tablename, sql)
        columnnames = cls.__getnamesfrom(sqlparseutils.iscolumnname, sql)
        data = cls.__getvaluesfrom(sqlparseutils.isdata, sql)
        dataactions = cls.__getactionfrom(sqlparseutils.isdata, sql)

        if not columnnames:
            columnnames = [None] * len(data)        

        zipped = list(zip(columnnames, data, dataactions))

        columns = list(map(
                lambda column: SQLColumn(name=column[0], action=column[2], type=None,
                    size=None, value=str(column[1]).strip("'"), constraints=[]),
                zipped
            )
        )

        where = cls.__getwhere(sql)

        return SQLTable(name=tablename, action=tableaction, columns=columns, where=where)

    @classmethod
    def insert_into_sqltable(cls, sql: Statement):
        """Analyzes the INSERT INTO sql statement."""
        return cls.__map_sqldata(sql, SQLDMLAction.INSERT)

    @classmethod
    def update_sqltable(cls, sql: Statement):
        """Analyzes the UPDATE sql statement."""
        return cls.__map_sqldata(sql, SQLDMLAction.UPDATE)

    @classmethod
    def selectfrom_sqltable(cls, sql: Statement):
        """Analyzes the SELECT FROM sql statement."""
        return cls.__map_sqltable(sql, SQLDMLAction.SELECT, SQLDMLAction.SELECT)

    @classmethod
    def deletefrom_sqltable(cls, sql: Statement):
        """Analyzes the DELETE FROM sql statement."""
        return cls.__map_sqltable(sql, SQLDMLAction.DELETE, SQLDMLAction.DELETE)


SQLProcessor = {
    "CREATEDATABASE": SQLEntityFactory.create_sqldatabase,
    "DROPDATABASE": SQLEntityFactory.drop_sqldatabase,
    "CREATETABLE": SQLEntityFactory.create_sqltable,
    "ALTERTABLEMODIFYCOLUMN": SQLEntityFactory.alter_sqltablemodifycolumn,
    "ALTERTABLEMODIFYNOTNULL": SQLEntityFactory.alter_sqltablemodifynotnull,
    "ALTERTABLEADD": SQLEntityFactory.alter_sqltableaddcolumn,
    "ALTERTABLEADDCONSTRAINTUNIQUE": SQLEntityFactory.alter_sqltableaddconstraintunique,
    "ALTERTABLEADDCONSTRAINTPRIMARYKEY": SQLEntityFactory.alter_sqltableaddconstraintprimarykey,
    "ALTERTABLEDROPCONSTRAINT": SQLEntityFactory.alter_sqltabledropconstraint,
    "ALTERTABLEDROPCOLUMN": SQLEntityFactory.alter_sqltabledropcolumn,
    "DROPTABLE": SQLEntityFactory.drop_sqltable,
    "INSERTINTO": SQLEntityFactory.insert_into_sqltable,
    "UPDATESET": SQLEntityFactory.update_sqltable,
    "UPDATESETWHERE": SQLEntityFactory.update_sqltable,
    "SELECTFROM": SQLEntityFactory.selectfrom_sqltable,
    "DELETEFROM": SQLEntityFactory.deletefrom_sqltable
}
