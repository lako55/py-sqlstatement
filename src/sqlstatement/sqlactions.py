# Copyright (c) 2022 SQL Statement author, see LICENSE file. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

"""This module contains DDL and DML actions in form of enumerations."""

from enum import Enum

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

    SELECT      = "SELECT"
    INSERT      = "INSERT"
    UPDATE      = "UPDATE"
    DELETE      = "DELETE"
    WHERE       = "WHERE"
    WHEREEQUAL  = "WHEREEQUAL"
    WHERELIKE   = "WHERELIKE"
