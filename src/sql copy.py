from ast import keyword
from functools import reduce
from collections import namedtuple
from enum import Enum
from sqlparse import parse
from sqlparse.sql import Statement, Token, TokenList, Identifier
import sqlparse.tokens as TType

class SQLDDLAction(str, Enum):
    """
    Enum listting SQL DDL actions.
    """

    CREATE = "CREATE"
    ALTER = "ALTER"
    ADD = "ADD"
    DROP = "DROP"

class SQLDMLAction(str, Enum):
    """
    Enum listting SQL DML actions.
    """

    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

SQLEntity = namedtuple('SQLEntity', 'name action',)
SQLDatabase = namedtuple('SQLDatabase', SQLEntity._fields)
SQLTable = namedtuple('SQLTable', SQLEntity._fields + ('columns',))
SQLColumn = namedtuple('SQLColumn', SQLEntity._fields + ('type', 'size', 'constraints',), defaults=(None, None))
SQLConstraint = namedtuple('SQLConstraint', SQLEntity._fields + ('action',), defaults=[SQLDDLAction.ADD])
SQLConstraintUnique = namedtuple('SQLConstraintUnique', SQLConstraint._fields)
SQLConstraintNotNull = namedtuple('SQLConstraintNotNull', SQLConstraint._fields)
SQLConstraintDefault = namedtuple('SQLConstraintDefault', SQLConstraint._fields + ('value',))

class SQLEntityFactory:

    # def __init__(self, sql: str):
    #     self._sql: Statement = parse(sql)[0]
        
    #     # DB or table
    #     self._entity = self._create_entity()
    #     # SQLAction
    #     self._action = self._sql.get_type()

    def create_entity(self, sql):
        sql: Statement = parse(sql)[0]
        keywords = list(map(lambda token: token.value, filter(lambda token: token.is_keyword, self._sql.tokens)))
        funcname = "".join(keywords).replace(' ', '').upper()
        return SQLEntityFactory[funcname](self)

    def _getnames(self):
        return list(
            map(lambda token: token.value,
                filter(lambda token: token.ttype == TType.Name, self._sql.flatten())
            )
        )

    @property
    def isddl(self) -> bool:
        return self._sql.get_type() in SQLDDLAction

    """
    Create/drop DB
    """

    def create_sqldatabase(self):
        dbname, *_ = self._getnames()
        return SQLDatabase(name=dbname)

    def drop_sqldatabase(self):
        dbname, *_ = self._getnames()
        return SQLDatabase(name=dbname)

    """
    Create/drop table
    """

    def create_sqltable(self):
        return SQLTable()

    def drop_sqltable(self):
        tablename, *_ = self._getnames()
        
        return SQLTable(name=tablename, columns=[])

    """
    Add/modify column in table
    """

    def alter_sqltableaddcolumn(self):
        pass

    def alter_sqltablemodify(self):
        pass

    """
    Constraints
    """

    def alter_sqltablemodifynotnull(self):
        tablename, columnname, *_ = self._getnames()
        # Token.Name.Builtin
        types = list(
            map(lambda token: token.value,
                filter(lambda token: token.ttype == TType.Name.Builtin, self._sql.flatten())
            )
        )

        column = SQLColumn(name=columnname, type=types[0], size=0, constraints=[SQLConstraintNotNull('notnull', SQLDDLAction.ADD)])

        return SQLTable(name=tablename, columns=[column])

    def alter_sqltableconstraintunique(self, action: SQLDDLAction):
        names = self._getnames()
        tablename, constraintname = names[:2]
        columnnames = names[2:]

        columns = list(map(
                lambda name: SQLColumn(name=name, type=None, size=None, constraints=[SQLConstraintUnique(constraintname, action)]),
                columnnames
            )
        )        

        return SQLTable(name=tablename, columns=columns)

    def alter_sqltableaddconstraintunique(self):
        return self.alter_sqltableconstraintunique(SQLDDLAction.ADD)
        
    def alter_sqltabledropconstraintunique(self):
        return self.alter_sqltableconstraintunique(SQLDDLAction.DROP)


SQLEntityFactory = {
    "CREATEDATABASE": SQLStatement.create_sqldatabase,
    "DROPDATABASE": SQLStatement.drop_sqldatabase,
    "CREATETABLE": SQLStatement.create_sqltable,
    "ALTERTABLEMODIFY": SQLStatement.alter_sqltablemodify,
    "ALTERTABLEMODIFYNOTNULL": SQLStatement.alter_sqltablemodifynotnull,
    "ALTERTABLEADD": SQLStatement.alter_sqltableaddcolumn,
    "ALTERTABLEADDCONSTRAINTUNIQUE": SQLStatement.alter_sqltableaddconstraintunique,
    "ALTERTABLEDROPCONSTRAINTUNIQUE": SQLStatement.alter_sqltabledropconstraintunique,
    "DROPTABLE": SQLStatement.drop_sqltable,
    # "SELECT": func,
    # "INSERT": func,
    # "UPDATE": func,
    # "DELETE": func
}