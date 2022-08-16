from collections import namedtuple
from enum import Enum
from typing import List
from sqlparse import parse
from sqlparse.sql import Statement, Token, TokenList, Identifier, IdentifierList, Function, Parenthesis, Comparison, Where
import sqlparse.tokens as TType

class SQLDDLAction(str, Enum):
    """
    Enum listting SQL DDL actions.
    """

    CREATE = "CREATE"
    ALTER = "ALTER"
    DROP = "DROP"
    ADDCONSTRAINT = "ADDCONSTRAINT"
    DROPCONSTRAINT = "DROPCONSTRAINT"
    ADDCOLUMN = "ADDCOLUMN"
    MODIFYCOLUMN = "MODIFYCOLUMN"
    DROPCOLUMN = "DROPCOLUMN"

class SQLDMLAction(str, Enum):
    """
    Enum listting SQL DML actions.
    """

    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    WHERE  = "WHERE"

SQLEntity = namedtuple('SQLEntity', 'name action',)
SQLDatabase = namedtuple('SQLDatabase', SQLEntity._fields)
SQLTable = namedtuple('SQLTable', SQLEntity._fields + ('columns', 'where',), defaults=(None,))
SQLColumn = namedtuple('SQLColumn', SQLEntity._fields + ('type', 'size', 'constraints', 'value'), defaults=(None, None, None))

SQLConstraint = namedtuple('SQLConstraint', SQLEntity._fields)
SQLConstraintPrimaryKey = namedtuple('SQLConstraintPrimaryKey', SQLConstraint._fields)
SQLConstraintUnique = namedtuple('SQLConstraintUnique', SQLConstraint._fields)
SQLConstraintNotNull = namedtuple('SQLConstraintNotNull', SQLConstraint._fields)
SQLConstraintDefault = namedtuple('SQLConstraintDefault', SQLConstraint._fields + ('value',))

SQLAnd = namedtuple('SQLAnd', 'filter',)
SQLOr = namedtuple('SQLOr', 'filter',)

def is_db_or_tablename(token: Token):
    return (token.ttype == TType.Name
        and isinstance(token.parent, Identifier)
        and (isinstance(token.parent.parent, Statement) 
            or (isinstance(token.parent.parent, Function) and isinstance(token.parent.parent.parent, Statement))
        )
    )

def iscolumnname(token: Token):
    return (token.ttype == TType.Name
        and isinstance(token.parent, Identifier)
        and (
            isinstance(token.parent.parent, (Parenthesis, IdentifierList, Comparison)) 
            or (isinstance(token.parent.parent, Statement) 
                and (isdatatypefollowing(token.parent) or iskeywordpreceding(token.parent, "COLUMN"))
            )
        )
        and not isinwhere(token)
    )

def isdatatypefollowing(token: Token):
    statement: Statement = token.parent
    idx = statement.token_index(token)
    
    nextidx, tkn = statement.token_next(idx=idx)
    return isinteger(tkn)

def findstatement(token: Token, lasttoken: Token):
    if isinstance(token, (Statement)):
        return (token, lasttoken)
    else:
        return findstatement(token.parent, token)

def iskeyword(token: Token, value: str):
    return token.is_keyword and token.normalized == value

def iskeywordpreceding(token: Token, value: str):

    statement: Statement
    tkn: Token
    statement, tkn = findstatement(token, token)

    if tkn.tokens and len(tkn.tokens) > 0 and iskeyword(tkn.tokens[0], value):
        return True
    else:
        idx = statement.token_index(tkn)
        
        previdx, tkn = statement.token_prev(idx=idx)
        return iskeyword(tkn, value)


def isvarchar(token: Token):
    return (token.ttype == TType.Name
        and isinstance(token.parent, Identifier)
        and isinstance(token.parent.parent, Function)
    )

def isinteger(token: Token):
    return token.ttype == TType.Name.Builtin

def iscolumntype(token: Token):
    return isvarchar(token) or isinteger(token)
    
def getcolumntype(token: Token):
    if isinteger(token):
        result = (token.value, None)
    else:
        # extract type and size from f.i. varchar(255)
        value: str = token.parent.parent.value
        result = tuple(value.removesuffix(')').split('('))

    return result

def isvaluein(token: Token, keyword: str):
    return (token.ttype in (TType.String.Single, TType.Number.Integer)
        and (isinstance(token.parent, (IdentifierList))
            or (isinstance(token.parent, (Comparison)) 
                and iskeywordpreceding(token.parent, keyword) 
            )
        )
    )

def isdata(token: Token):
    return isvaluein(token, "SET")

def isinwhere(token: Token):
    if isinstance(token, (Where)):
        return True

    if isinstance(token, (Statement)):
        return False

    return isinwhere(token.parent)

def isandor(token: Token):
    return ((token.is_keyword and token.normalized in ('WHERE', 'AND', 'OR')) or token.normalized == "(")

def getnexttoken(token: Token):
    tokens = list(token.parent.tokens)
    nextidx = tokens.index(token) + 1

    while nextidx < len(tokens) and tokens[nextidx].is_whitespace:
        nextidx = nextidx + 1

    return tokens[nextidx]

actionmap = {
    "IdentifierList": SQLDMLAction.INSERT,
    "Comparison": SQLDMLAction.UPDATE
}    

class SQLEntityFactory:

    # def __init__(self, sql: str):
    #     self._sql: Statement = parse(sql)[0]
        
    #     # DB or table
    #     self._entity = self._create_entity()
    #     # SQLAction
    #     self._action = self._sql.get_type()

    @classmethod
    def normalize_funcname(cls, funcname: str):
        funcname = funcname.upper()
        if funcname.startswith("CREATETABLE"):
            funcname = "CREATETABLE"
        elif funcname.startswith("ALTERTABLEADD") and not funcname in ["ALTERTABLEADDCONSTRAINTUNIQUE", "ALTERTABLEADDCONSTRAINTPRIMARYKEY"]:
            funcname = "ALTERTABLEADD"

        return funcname

    @classmethod
    def create_entity(cls, sql: str):
        statement: Statement = parse(sql)[0]
        keywords = list(map(lambda token: token.value, filter(lambda token: token.is_keyword, statement.tokens)))
        funcname = cls.normalize_funcname("".join(keywords).replace(' ', '').upper())

        return SQLProcessor[funcname](statement)

    @classmethod
    def getnamesfrom(cls, func, sql: Statement):
        
        return list(
            map(lambda token: token.value,
                list(filter(func, sql.flatten()))
            )
        )

    @classmethod
    def gettypesfrom(cls, func, sql: Statement):
        
        return list(
            map(getcolumntype,
                list(filter(func, sql.flatten()))
            )
        )

    @classmethod
    def getvaluesfrom(cls, func, sql: Statement):

        return list(
            map(lambda token: token.value,
                list(filter(func, sql.flatten()))
            )
        )

    @classmethod
    def mapwherecondition(cls, andortoken: Token):
        token: Token = getnexttoken(andortoken)
        if isinstance(token, (Comparison)):
            filter=[SQLColumn(name=token.left.value, action=SQLDMLAction.WHERE, type=None, size=None, constraints=None, value=str(token.right.value).strip("'"))]
        elif isinstance(token, (Parenthesis)):
            filter=cls.getfilterconditions(token.tokens)

        match andortoken.normalized:
            case "WHERE":
                return SQLAnd(filter=filter)
            case "AND":
                return SQLAnd(filter=filter)
            case "(":
                return SQLAnd(filter=filter)
            case "OR":
                return SQLOr(filter=filter)
            case _:
                return (None, None)

    @classmethod
    def getfilterconditions(cls, tokens: List[Token]):
       
        return list(
            map(cls.mapwherecondition,
                list(filter(isandor, tokens))
            )
        )

    @classmethod
    def getactionfrom(cls, func, sql: Statement):        

        return list(
            map(lambda token: actionmap[type(token.parent).__name__],
                list(filter(func, sql.flatten()))
            )
        )

    @classmethod
    def getwhere(cls, sql: Statement):
        
        wheretokens: List[Token] = list(filter(lambda token: isinstance(token, (Where)), sql.tokens))
        if wheretokens:
            return cls.getfilterconditions(wheretokens[0])

        return None

    @classmethod
    def map_constraints(cls, colname: str, sql: Statement):

        tokens: List = list(sql.flatten())
        
        def isconstraint(token: Token):
            if token.normalized in ["PRIMARY", "NOT NULL", "UNIQUE"]:
                idx = tokens.index(token)
                subtokens = tokens[:idx-1]
                tkn: Token

                for tkn in reversed(subtokens):
                    if iscolumnname(tkn):
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

    """
    Create/drop DB
    """

    @classmethod
    def create_sqldatabase(cls, sql: Statement):        
        dbname, *_ = cls.getnamesfrom(is_db_or_tablename, sql)
        return SQLDatabase(name=dbname, action=SQLDDLAction.CREATE)

    @classmethod
    def drop_sqldatabase(cls, sql: Statement):
        dbname, *_ = cls.getnamesfrom(is_db_or_tablename, sql)
        return SQLDatabase(name=dbname, action=SQLDDLAction.DROP)

    """
    Extraction and mapping of the sql data to the SQLStatement data structure
    """

    @classmethod
    def map_sqltable(cls, sql: Statement, tableaction: SQLDDLAction, columnaction: SQLDDLAction):
        tablename, *_ = cls.getnamesfrom(is_db_or_tablename, sql)
        columnnames = cls.getnamesfrom(iscolumnname, sql)

        types = cls.gettypesfrom(iscolumntype, sql)
        if not types:
            types = [(None, None)] * len(columnnames)

        zipped = list(zip(columnnames, types))

        columns = list(map(
                lambda column: SQLColumn(name=column[0], action=columnaction, type=column[1][0], 
                    size=column[1][1], constraints=cls.map_constraints(column[0], sql)),
                zipped
            )
        )

        where = cls.getwhere(sql)
        
        return SQLTable(name=tablename, action=tableaction, columns=columns, where=where)

    """
    Create/drop table
    """

    @classmethod
    def create_sqltable(cls, sql: Statement):
        return cls.map_sqltable(sql, SQLDDLAction.CREATE, SQLDDLAction.CREATE)

    @classmethod
    def drop_sqltable(cls, sql: Statement):
        tablename, *_ = cls.getnamesfrom(is_db_or_tablename, sql)
        
        return SQLTable(name=tablename, action=SQLDDLAction.DROP, columns=[])

    """
    Add/modify column in table
    """    

    @classmethod
    def alter_sqltableaddcolumn(cls, sql: Statement):
        return cls.map_sqltable(sql, SQLDDLAction.ALTER, SQLDDLAction.ADDCOLUMN)
        
    @classmethod
    def alter_sqltabledropcolumn(cls, sql: Statement):
        return cls.map_sqltable(sql, SQLDDLAction.ALTER, SQLDDLAction.DROPCOLUMN)


    @classmethod
    def alter_sqltablemodifycolumn(cls, sql: Statement):
        return cls.map_sqltable(sql, SQLDDLAction.ALTER, SQLDDLAction.MODIFYCOLUMN)

    """
    Constraints
    """

    @classmethod
    def alter_sqltablemodifynotnull(cls, sql: Statement):
        tablename, *_ = cls.getnamesfrom(is_db_or_tablename, sql)
        columnnames = cls.getnamesfrom(iscolumnname, sql)

        types = cls.gettypesfrom(iscolumntype, sql)

        zipped = list(zip(columnnames, types))

        columns = list(map(
                lambda column: SQLColumn(name=column[0], action=SQLDDLAction.ADDCONSTRAINT, type=column[1][0], size=column[1][1], constraints=[SQLConstraintNotNull(name='notnull', action=SQLDDLAction.ADDCONSTRAINT)]),
                zipped
            )
        )        

        return SQLTable(name=tablename, action=SQLDDLAction.ADDCONSTRAINT, columns=columns)


    @classmethod
    def map_sqltableconstraint(cls, columnnames: List[str], action: SQLDDLAction, constraint):
                
        return list(map(
                lambda name: SQLColumn(name=name, action=action, type=None, size=None, constraints=[constraint]),
                columnnames
            )
        )        

    @classmethod
    def alter_sqltableaddconstraint(cls, sql: Statement, constrainttype: type):

        tablename, constraintname = cls.getnamesfrom(is_db_or_tablename, sql)
        columnnames = cls.getnamesfrom(iscolumnname, sql)

        columns = cls.map_sqltableconstraint(columnnames, SQLDDLAction.ADDCONSTRAINT, constrainttype(name=constraintname, action=SQLDDLAction.ADDCONSTRAINT))
        return SQLTable(name=tablename, action=SQLDDLAction.ADDCONSTRAINT, columns=columns)

    @classmethod
    def alter_sqltableaddconstraintunique(cls, sql: Statement):

        return cls.alter_sqltableaddconstraint(sql, SQLConstraintUnique)

    @classmethod
    def alter_sqltableaddconstraintprimarykey(cls, sql: Statement):        
        return cls.alter_sqltableaddconstraint(sql, SQLConstraintPrimaryKey)
    
    @classmethod
    def alter_sqltabledropconstraint(cls, sql: Statement):

        tablename, constraintname = cls.getnamesfrom(is_db_or_tablename, sql)
        columns = cls.map_sqltableconstraint(['*'], SQLDDLAction.DROPCONSTRAINT, SQLConstraint(name=constraintname, action=SQLDDLAction.DROPCONSTRAINT))

        return SQLTable(name=tablename, action=SQLDDLAction.DROPCONSTRAINT, columns=columns)

    @classmethod
    def map_sqldata(cls, sql: Statement, tableaction: SQLDMLAction, columnaction: SQLDMLAction):
        tablename, *_ = cls.getnamesfrom(is_db_or_tablename, sql)
        columnnames = cls.getnamesfrom(iscolumnname, sql)
        data = cls.getvaluesfrom(isdata, sql)
        dataactions = cls.getactionfrom(isdata, sql)

        if not columnnames:
            columnnames = [None] * len(data)        

        zipped = list(zip(columnnames, data, dataactions))

        columns = list(map(
                lambda column: SQLColumn(name=column[0], action=column[2], type=None, 
                    size=None, value=str(column[1]).strip("'"), constraints=[]),
                zipped
            )
        )

        where = cls.getwhere(sql)
        
        return SQLTable(name=tablename, action=tableaction, columns=columns, where=where)

    @classmethod
    def insert_into_sqltable(cls, sql: Statement):

        return cls.map_sqldata(sql, SQLDMLAction.INSERT, SQLDMLAction.INSERT)

    @classmethod
    def update_sqltable(cls, sql: Statement):
        
        return cls.map_sqldata(sql, SQLDMLAction.UPDATE, SQLDMLAction.UPDATE)

    @classmethod
    def selectfrom_sqltable(cls, sql: Statement):

        return cls.map_sqltable(sql, SQLDMLAction.SELECT, SQLDMLAction.SELECT)


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
    "SELECTFROM": SQLEntityFactory.selectfrom_sqltable
    # "DELETE": func
}