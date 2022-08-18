# Copyright (c) 2022 SQL Statement author, see LICENSE file. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

"""Utility functions for list filters analysing sqlparse.sql.Token."""

from sqlparse.sql import (Statement, Token, Identifier, IdentifierList,
    Function, Parenthesis, Comparison, Where)
import sqlparse.tokens as TType

def is_db_or_tablename(token: Token):
    """Returns true if sqlparse.sql.Token is a name of a database or a table."""
    return (token.ttype == TType.Name
        and isinstance(token.parent, Identifier)
        and (isinstance(token.parent.parent, Statement)
            or (isinstance(token.parent.parent, Function)
                and isinstance(token.parent.parent.parent, Statement)
            )
        )
    )

def iscolumnname(token: Token):
    """Returns true if sqlparse.sql.Token is a name of a column
    except for column names in a WHERE clause."""
    return (token.ttype == TType.Name
        and isinstance(token.parent, Identifier)
        and (
            isinstance(token.parent.parent, (Parenthesis, IdentifierList, Comparison))
            or (isinstance(token.parent.parent, Statement)
                and (isdatatypefollowing(token.parent)
                    or iskeywordpreceding(token.parent, "COLUMN")
                )
            )
        )
        and not isinwhere(token)
    )

def isdatatypefollowing(token: Token):
    """Returns true if sqlparse.sql.Token is followed by a column type definition."""
    statement: Statement = token.parent
    idx = statement.token_index(token)

    nextidx, tkn = statement.token_next(idx=idx)
    return isinteger(tkn)

def findstatement(token: Token, lasttoken: Token):
    """Returns sqlparse.sql.Statement by traversing the parent tree upwards.
    Needed internally by some token filtering functions"""
    if isinstance(token, (Statement)):
        return (token, lasttoken)

    return findstatement(token.parent, token)

def iskeyword(token: Token, value: str):
    """Returns true if sqlparse.sql.Token is a specific keyword indicated by value argument."""
    return token.is_keyword and token.normalized == value

def iskeywordpreceding(token: Token, value: str):
    """Returns true if sqlparse.sql.Token is preceeded by a specific keyword.
    Whitespaces skipped."""
    statement: Statement
    tkn: Token
    statement, tkn = findstatement(token, token)

    if tkn.tokens and len(tkn.tokens) > 0 and iskeyword(tkn.tokens[0], value):
        return True

    idx = statement.token_index(tkn)

    previdx, tkn = statement.token_prev(idx=idx)
    return iskeyword(tkn, value)


def isvarchar(token: Token):
    """Returns true if sqlparse.sql.Token is a VARCHAR type."""
    return (token.ttype == TType.Name
        and isinstance(token.parent, Identifier)
        and isinstance(token.parent.parent, Function)
    )

def isinteger(token: Token):
    """Returns true if sqlparse.sql.Token is an INTEGER type."""
    return token.ttype == TType.Name.Builtin

def iscolumntype(token: Token):
    """Returns true if sqlparse.sql.Token is a column type."""
    return isvarchar(token) or isinteger(token)

def getcolumntype(token: Token):
    """Returns column type as a typle specified by sqlparse.sql.Token.
    Possible values are for integer or varchar including its size."""
    if isinteger(token):
        result = (token.value, None)
    else:
        # extract type and size from f.i. varchar(255)
        value: str = token.parent.parent.value
        result = tuple(value.removesuffix(')').split('('))

    return result

def isvaluein(token: Token, keyword: str):
    """Returns true if sqlparse.sql.Token is a value preceeded by a specific keyword."""
    return (token.ttype in (TType.String.Single, TType.Number.Integer)
        and (isinstance(token.parent, (IdentifierList))
            or (isinstance(token.parent, (Comparison))
                and iskeywordpreceding(token.parent, keyword)
            )
        )
    )

def isdata(token: Token):
    """Returns true if sqlparse.sql.Token is a value in UPDATE SET."""
    return isvaluein(token, "SET")

def isinwhere(token: Token):
    """Returns true if sqlparse.sql.Token is a part of WHERE clause."""
    if isinstance(token, (Where)):
        return True

    if isinstance(token, (Statement)):
        return False

    return isinwhere(token.parent)

def isandor(token: Token):
    """Returns true if sqlparse.sql.Token is considered as AND/OR clause."""
    return (
        (token.is_keyword and token.normalized in ('WHERE', 'AND', 'OR'))
        or token.normalized == "("
    )

def getnexttoken(token: Token):
    """Returns next sqlparse.sql.token in a list. Whitespaces skipped."""
    tokens = list(token.parent.tokens)
    nextidx = tokens.index(token) + 1

    while nextidx < len(tokens) and tokens[nextidx].is_whitespace:
        nextidx = nextidx + 1

    return tokens[nextidx]
