import unittest
from typing import List, Tuple
from src.sqlstatement.sql import SQLEntityFactory
from src.sqlstatement.sqlentities import (SQLDatabase, SQLTable, SQLColumn, SQLConstraint,
    SQLConstraintNotNull, SQLConstraintPrimaryKey, SQLConstraintUnique, SQLAnd, SQLOr)
from src.sqlstatement.sqlactions import SQLDDLAction, SQLDMLAction

class SampleSQL:

    CREATEDB  = "CREATE DATABASE testDB;"
    DROPDB  = "DROP DATABASE testDB;"
    CREATETABLE = """CREATE TABLE Persons (
                    PersonID int,
                    LastName varchar(255),
                    FirstName varchar(255),
                    Address varchar(255),
                    City varchar(255)
                );"""

    CREATETABLEEXPECTED = [
        ("PersonID", "int", None, SQLDDLAction.CREATE, []),
        ("LastName", "varchar", "255", SQLDDLAction.CREATE, []),
        ("FirstName", "varchar", "255", SQLDDLAction.CREATE, []),
        ("Address", "varchar", "255", SQLDDLAction.CREATE, []),
        ("City", "varchar", "255", SQLDDLAction.CREATE, [])
    ]

    CREATETABLECONSTRAINTS = """CREATE TABLE Persons (
                    PersonID int PRIMARY KEY,
                    LastName varchar(255) NOT NULL,
                    FirstName varchar(255) UNIQUE NOT NULL,
                    Address varchar(255),
                    City varchar(255)
                );"""
    CREATETABLECONSTRAINTSEXPECTED = [
        ("PersonID", "int", None, SQLDDLAction.CREATE, [(SQLConstraintPrimaryKey, "primarykey", SQLDDLAction.ADDCONSTRAINT)]),
        ("LastName", "varchar", "255", SQLDDLAction.CREATE, [(SQLConstraintNotNull, "notnull", SQLDDLAction.ADDCONSTRAINT)]),
        ("FirstName", "varchar", "255", SQLDDLAction.CREATE, [(SQLConstraintUnique, "unique", SQLDDLAction.ADDCONSTRAINT), (SQLConstraintNotNull, "notnull", SQLDDLAction.ADDCONSTRAINT)]),
        ("Address", "varchar", "255", SQLDDLAction.CREATE, []),
        ("City", "varchar", "255", SQLDDLAction.CREATE, [])
    ]

    ALTERTABLEADD = "ALTER TABLE Persons ADD DateOfBirth date;"
    ALTERTABLEADDEXPECTED = [
        ("DateOfBirth", "date", None, SQLDDLAction.ADDCOLUMN, [])
    ]

    ALTERTABLEADDWCONSTRAINT = "ALTER TABLE Persons ADD DateOfBirth date NOT NULL UNIQUE, customer_name varchar(50) NOT NULL;"
    ALTERTABLEADDWCONSTRAINTEXPECTED = [
        ("DateOfBirth", "date", None, SQLDDLAction.ADDCOLUMN, [(SQLConstraintNotNull, "notnull", SQLDDLAction.ADDCONSTRAINT), (SQLConstraintUnique, "unique", SQLDDLAction.ADDCONSTRAINT)]),
        ("customer_name", "varchar", "50", SQLDDLAction.ADDCOLUMN, [(SQLConstraintNotNull, "notnull", SQLDDLAction.ADDCONSTRAINT)])
    ]

    ALTERTABLEMODIFY = "ALTER TABLE Persons MODIFY COLUMN DateOfBirth date;"
    ALTERTABLEMODIFYEXPECTED = [
        ("DateOfBirth", "date", None, SQLDDLAction.MODIFYCOLUMN, [])
    ]

    ALTERTABLEDROP = "ALTER TABLE Persons DROP COLUMN DateOfBirth;"
    ALTERTABLEDROPEXPECTED = [
        ("DateOfBirth", None, None, SQLDDLAction.DROPCOLUMN, [])
    ]

    ADDUNIQUE = "ALTER TABLE Persons ADD CONSTRAINT UC_Person UNIQUE (ID,LastName);"
    ADDUNIQUEEXPECTED = [
        ("ID", None, None, SQLDDLAction.ADDCONSTRAINT, [(SQLConstraintUnique, "UC_Person", SQLDDLAction.ADDCONSTRAINT)]),
        ("LastName", None, None, SQLDDLAction.ADDCONSTRAINT, [(SQLConstraintUnique, "UC_Person", SQLDDLAction.ADDCONSTRAINT)])
    ]

    ADDPRIMARYKEY = "ALTER TABLE Persons ADD CONSTRAINT PK_Person PRIMARY KEY (ID,LastName);"
    ADDPRIMARYKEYEXPECTED = [
        ("ID", None, None, SQLDDLAction.ADDCONSTRAINT, [(SQLConstraintPrimaryKey, "PK_Person", SQLDDLAction.ADDCONSTRAINT)]),
        ("LastName", None, None, SQLDDLAction.ADDCONSTRAINT, [(SQLConstraintPrimaryKey, "PK_Person", SQLDDLAction.ADDCONSTRAINT)])
    ]

    DROPCONSTRAINT = "ALTER TABLE Persons DROP CONSTRAINT UC_Person;"
    DROPCONSTRAINTEXPECTED = [
        ("*", None, None, SQLDDLAction.DROPCONSTRAINT, [(SQLConstraint, "UC_Person", SQLDDLAction.DROPCONSTRAINT)])        
    ]

    ADDNOTNULL = "ALTER TABLE Persons MODIFY Age int NOT NULL;"
    ADDNOTNULLEXPECTED = [
        ("Age", "int", None, SQLDDLAction.ADDCONSTRAINT, [(SQLConstraintNotNull, "notnull", SQLDDLAction.ADDCONSTRAINT)])
    ]

    DROPTABLE = "DROP TABLE Persons"

    INSERTINTOWCOLS = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', 4006, 'Norway');"
    INSERTINTOWCOLSEXPECTED = [
        ("CustomerName", "Cardinal", SQLDMLAction.INSERT),
        ("ContactName", "Tom B. Erichsen", SQLDMLAction.INSERT),
        ("Address", "Skagen 21", SQLDMLAction.INSERT),
        ("City", "Stavanger", SQLDMLAction.INSERT),
        ("PostalCode", "4006", SQLDMLAction.INSERT),
        ("Country", "Norway", SQLDMLAction.INSERT)
    ]

    INSERTINTONOCOLS = "INSERT INTO Customers VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', 4006, 'Norway');"
    INSERTINTONOCOLSEXPECTED = [
        (None, "Cardinal", SQLDMLAction.INSERT),
        (None, "Tom B. Erichsen", SQLDMLAction.INSERT),
        (None, "Skagen 21", SQLDMLAction.INSERT),
        (None, "Stavanger", SQLDMLAction.INSERT),
        (None, "4006", SQLDMLAction.INSERT),
        (None, "Norway", SQLDMLAction.INSERT)
    ]

    UPDATEONE = "UPDATE Customers SET CustomerName='Cardinal' WHERE Country='Mexico';"
    UPDATEONEEXPECTED = [
        ("CustomerName", "Cardinal", SQLDMLAction.UPDATE),        
    ]

    UPDATEMULTI = "UPDATE Customers SET ContactName='Juan', CustomerName='Cardinal' WHERE Country='Mexico' AND City LIKE '%Monterrey%' AND ( ContactName='Juan' OR ContactName='Isabela' );"
    UPDATEMULTIEXPECTED = [
        ("ContactName", "Juan", SQLDMLAction.UPDATE),
        ("CustomerName", "Cardinal", SQLDMLAction.UPDATE),        
    ]    

    SELECTFROM = "SELECT CustomerName, City FROM Customers WHERE Country='Mexico' AND City LIKE '%Monterrey%' AND ( ContactName='Juan' OR ContactName='Isabela' );"
    SELECTFROMEXPECTED = [
        ("CustomerName", None, None, SQLDMLAction.SELECT, []),
        ("City", None, None, SQLDMLAction.SELECT, [])
    ]

    WHEREEXPECTED = [
        (SQLAnd, [("Country", "Mexico", SQLDMLAction.WHEREEQUAL)]),
        (SQLAnd, [("City", "%Monterrey%", SQLDMLAction.WHERELIKE)]),
        (SQLAnd, [
            (SQLAnd, [("ContactName", "Juan", SQLDMLAction.WHEREEQUAL)]), 
            (SQLOr, [("ContactName", "Isabela", SQLDMLAction.WHEREEQUAL)]),
        ]),
    ]

    DELETEFROM = "DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';"
    DELETEWHEREEXPECTED = [
        (SQLAnd, [("CustomerName", "Alfreds Futterkiste", SQLDMLAction.WHEREEQUAL)]),
    ]

class TestSQLParse(unittest.TestCase):

    def test_parsecreatedb(self):
        sqlentity: SQLDatabase = SQLEntityFactory.create_entity(SampleSQL.CREATEDB)

        self.assert_entity(sqlentity, SQLDatabase, "testDB", SQLDDLAction.CREATE)

    def test_parsedropdb(self):
        sqlentity: SQLDatabase = SQLEntityFactory.create_entity(SampleSQL.DROPDB)

        self.assert_entity(sqlentity, SQLDatabase, "testDB", SQLDDLAction.DROP)          

    def test_parsecreatetable(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.CREATETABLE)

        self.assert_entity(sqlentity, SQLTable, "Persons", SQLDDLAction.CREATE)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.CREATETABLEEXPECTED)

    def test_parsecreatetableconstraints(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.CREATETABLECONSTRAINTS)

        self.assert_entity(sqlentity, SQLTable, "Persons", SQLDDLAction.CREATE)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.CREATETABLECONSTRAINTSEXPECTED)

    def test_parseaddconstraintunique(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ADDUNIQUE)

        self.assert_entity(sqlentity, SQLTable, "Persons", SQLDDLAction.ADDCONSTRAINT)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.ADDUNIQUEEXPECTED)

    def test_parseaddconstraintprimarykey(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ADDPRIMARYKEY)

        self.assert_entity(sqlentity, SQLTable, "Persons", SQLDDLAction.ADDCONSTRAINT)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.ADDPRIMARYKEYEXPECTED)
                
    def test_parsedropconstraint(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.DROPCONSTRAINT)

        self.assert_entity(sqlentity, SQLTable, "Persons", SQLDDLAction.DROPCONSTRAINT)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.DROPCONSTRAINTEXPECTED)

    def test_alteraddconstraintnotnull(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ADDNOTNULL)

        self.assert_entity(sqlentity, SQLTable, "Persons", SQLDDLAction.ADDCONSTRAINT)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.ADDNOTNULLEXPECTED)

    def test_altertableadd(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ALTERTABLEADD)

        self.assert_entity(sqlentity, SQLTable, "Persons", SQLDDLAction.ALTER)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.ALTERTABLEADDEXPECTED)

    def test_altertableaddwconstraint(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ALTERTABLEADDWCONSTRAINT)

        self.assert_entity(sqlentity, SQLTable, "Persons", SQLDDLAction.ALTER)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.ALTERTABLEADDWCONSTRAINTEXPECTED)
        
    def test_altertablemodify(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ALTERTABLEMODIFY)

        self.assert_entity(sqlentity, SQLTable, "Persons", SQLDDLAction.ALTER)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.ALTERTABLEMODIFYEXPECTED)

    def test_altertabledrop(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ALTERTABLEDROP)

        self.assert_entity(sqlentity, SQLTable, "Persons", SQLDDLAction.ALTER)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.ALTERTABLEDROPEXPECTED)

    def test_insertintowcols(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.INSERTINTOWCOLS)

        self.assert_entity(sqlentity, SQLTable, "Customers", SQLDMLAction.INSERT)
        self.assert_lists(self.assert_columndata, sqlentity.columns, SampleSQL.INSERTINTOWCOLSEXPECTED)

    def test_insertintonocols(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.INSERTINTONOCOLS)

        self.assert_entity(sqlentity, SQLTable, "Customers", SQLDMLAction.INSERT)
        self.assert_lists(self.assert_columndata, sqlentity.columns, SampleSQL.INSERTINTONOCOLSEXPECTED)

    def test_updateone(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.UPDATEONE)

        self.assert_entity(sqlentity, SQLTable, "Customers", SQLDMLAction.UPDATE)
        self.assert_lists(self.assert_columndata, sqlentity.columns, SampleSQL.UPDATEONEEXPECTED)

    def test_updatemulti(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.UPDATEMULTI)

        self.assert_entity(sqlentity, SQLTable, "Customers", SQLDMLAction.UPDATE)
        self.assert_lists(self.assert_columndata, sqlentity.columns, SampleSQL.UPDATEMULTIEXPECTED)

        self.assert_where(sqlentity.where, SampleSQL.WHEREEXPECTED)

    def test_selectfrom(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.SELECTFROM)

        self.assert_entity(sqlentity, SQLTable, "Customers", SQLDMLAction.SELECT)
        self.assert_lists(self.assert_column, sqlentity.columns, SampleSQL.SELECTFROMEXPECTED)

        self.assert_where(sqlentity.where, SampleSQL.WHEREEXPECTED)

    def test_deletefrom(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.DELETEFROM)

        self.assert_entity(sqlentity, SQLTable, "Customers", SQLDMLAction.DELETE)        

        self.assert_where(sqlentity.where, SampleSQL.DELETEWHEREEXPECTED)

    def assert_entity(self, sqlentity, enttype:type, name: str, action: SQLDDLAction):
        self.assertIsInstance(sqlentity, enttype)
        self.assertEqual(sqlentity.name, name, 'Name check failed.')
        self.assertEqual(sqlentity.action, action, 'Entity action check failed.')

    def assert_column(self, actual: SQLColumn, expected: Tuple):
        self.assertEqual(actual.name, expected[0], 'Column name check failed.')
        self.assertEqual(actual.type, expected[1], 'Column type check failed.')
        self.assertEqual(actual.size, expected[2], 'Column size check failed.')
        self.assertEqual(actual.action, expected[3], 'Column action check failed.')

        self.assert_lists(self.assert_constraint, actual.constraints, expected[4])

    def assert_columndata(self, actual: SQLColumn, expected: Tuple):
        self.assertEqual(actual.name, expected[0], 'Column name check failed.')
        self.assertEqual(actual.value, expected[1], 'Column value check failed.')        
        self.assertEqual(actual.action, expected[2], 'Column action check failed.')

    def assert_lists(self, assertfunc, actual_result: List, expected_result: List):
        self.assertEqual(len(actual_result), len(expected_result), 'List count check failed.')

        zipped = zip(actual_result, expected_result)
        for actual, expected in zipped:
            assertfunc(actual, expected)

    def assert_constraint(self, actual, expected: Tuple):
        self.assertIsInstance(actual, expected[0])
        self.assertEqual(actual.name, expected[1], 'Constraint name check failed.')
        self.assertEqual(actual.action, expected[2], 'Action check failed.')

    def assert_filter(self, actual, expected: Tuple):
        self.assertIsInstance(actual, expected[0])

        actualfilter = list(filter(lambda i: isinstance(i, (SQLColumn)), actual.filter))
        actualsubfilter = list(filter(lambda i: isinstance(i, (SQLAnd, SQLOr)), actual.filter))
        if actualfilter:
            self.assert_lists(self.assert_columndata, actualfilter, expected[1])
        elif actualsubfilter:
            self.assert_lists(self.assert_filter, actualsubfilter, expected[1])

    def assert_where(self, actual, expected: Tuple):
        self.assert_lists(self.assert_filter, actual, expected)


if __name__ == '__main__':
    unittest.main()
