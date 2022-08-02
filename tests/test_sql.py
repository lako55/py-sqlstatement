import unittest
from src.sql import SQLConstraintUnique, SQLEntity, SQLEntityFactory
from src.sql import SQLDatabase, SQLTable, SQLDDLAction

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

    ALTERTABLEADD = "ALTER TABLE Persons ADD DateOfBirth date;"
    ALTERTABLEMODIFY = "ALTER TABLE Persons MODIFY COLUMN DateOfBirth date;"
    ALTERTABLEDROP = "ALTER TABLE Persons DROP COLUMN DateOfBirth;"

    ADDUNIQUE = "ALTER TABLE Persons ADD CONSTRAINT UC_Person UNIQUE (ID,LastName);"
    DROPUNIQUE = "ALTER TABLE Persons DROP CONSTRAINT UC_Person;"

    ADDNOTNULL = "ALTER TABLE Persons MODIFY Age int NOT NULL;"

    DROPTABLE = "DROP TABLE Persons"

class TestSQLParse(unittest.TestCase):

    def test_parsecreatedb(self):
        sqlentity: SQLDatabase = SQLEntityFactory.create_entity(SampleSQL.CREATEDB)

        self.assertIsInstance(sqlentity, SQLDatabase)
        self.assertEqual(sqlentity.name, 'testDB', 'Database name check failed.')
        self.assertEqual(sqlentity.action, SQLDDLAction.CREATE, 'Action check failed.')

    def test_parsedropdb(self):
        sqlentity: SQLDatabase = SQLEntityFactory.create_entity(SampleSQL.DROPDB)

        self.assertIsInstance(sqlentity, SQLDatabase)
        self.assertEqual(sqlentity.name, 'testDB', 'Database name check failed.')
        self.assertEqual(sqlentity.action, SQLDDLAction.DROP, 'Action check failed.')

    def test_parsecreatetable(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.CREATETABLE)

        self.assertIsInstance(sqlentity, SQLTable)
        self.assertEqual(sqlentity.name, 'Persons', 'Table name check failed.')
        self.assertEqual(sqlentity.action, SQLDDLAction.CREATE, 'Action check failed.')

    def test_parseaddconstraint(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ADDUNIQUE)

        self.assertIsInstance(sqlentity, SQLTable)
        self.assertEqual(sqlentity.name, 'Persons', 'Table name check failed.')
        self.assertEqual(sqlentity.action, SQLDDLAction.ADDCONSTRAINT, 'Action check failed.')
        
        self.assertEqual(sqlentity.columns[0].name, 'ID', 'Column name check failed.')
        self.assertIsInstance(sqlentity.columns[0].constraints[0], SQLConstraintUnique)
        self.assertEqual(sqlentity.columns[0].constraints[0].name, 'UC_Person', 'Constraint name check failed.')
        self.assertEqual(sqlentity.columns[0].constraints[0].action, SQLDDLAction.ADDCONSTRAINT, 'Action check failed.')

        self.assertEqual(sqlentity.columns[1].name, 'LastName', 'Column name check failed.')
        self.assertIsInstance(sqlentity.columns[1].constraints[0], SQLConstraintUnique)
        self.assertEqual(sqlentity.columns[1].constraints[0].name, 'UC_Person', 'Constraint name check failed.')
        self.assertEqual(sqlentity.columns[1].constraints[0].action, SQLDDLAction.ADDCONSTRAINT, 'Action check failed.')

    def test_parsedropconstraint(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.DROPUNIQUE)

        self.assertIsInstance(sqlentity, SQLTable)
        self.assertEqual(sqlentity.name, 'Persons', 'Table name check failed.')
        self.assertEqual(sqlentity.action, SQLDDLAction.DROPCONSTRAINT, 'Action check failed.')

    def test_alteraddnotnull(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ADDNOTNULL)

        self.assertIsInstance(sqlentity, SQLTable)
        self.assertEqual(sqlentity.name, 'Persons', 'Table name check failed.')
        self.assertEqual(sqlentity.action, SQLDDLAction.ADDCONSTRAINT, 'Action check failed.')

    def test_altertableadd(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ALTERTABLEADD)

        self.assertIsInstance(sqlentity, SQLTable)
        self.assertEqual(sqlentity.name, 'Persons', 'Table name check failed.')
        self.assertEqual(sqlentity.action, SQLDDLAction.ALTER, 'Table action check failed.')

        self.assertEqual(sqlentity.columns[0].name, 'DateOfBirth', 'Column name check failed.')
        self.assertEqual(sqlentity.columns[0].type, 'date', 'Column type check failed.')
        self.assertEqual(sqlentity.columns[0].action, SQLDDLAction.ADDCOLUMN, 'Column action check failed.')
        # self.assertIsInstance(sqlentity.columns[0].constraints[0], SQLConstraintUnique)
        # self.assertEqual(sqlentity.columns[0].constraints[0].name, 'UC_Person', 'Constraint name check failed.')
        # self.assertEqual(sqlentity.columns[0].constraints[0].action, SQLDDLAction.ADDCONSTRAINT, 'Action check failed.')

    def test_altertablemodify(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ALTERTABLEMODIFY)

        self.assertIsInstance(sqlentity, SQLTable)
        self.assertEqual(sqlentity.name, 'Persons', 'Table name check failed.')
        self.assertEqual(sqlentity.action, SQLDDLAction.ALTER, 'Table action check failed.')

        self.assertEqual(sqlentity.columns[0].name, 'DateOfBirth', 'Column name check failed.')
        self.assertEqual(sqlentity.columns[0].type, 'date', 'Column type check failed.')
        self.assertEqual(sqlentity.columns[0].action, SQLDDLAction.MODIFYCOLUMN, 'Column action check failed.')

    def test_altertabledrop(self):
        sqlentity: SQLTable = SQLEntityFactory.create_entity(SampleSQL.ALTERTABLEDROP)

        self.assertIsInstance(sqlentity, SQLTable)
        self.assertEqual(sqlentity.name, 'Persons', 'Table name check failed.')
        self.assertEqual(sqlentity.action, SQLDDLAction.ALTER, 'Table action check failed.')

        self.assertEqual(sqlentity.columns[0].name, 'DateOfBirth', 'Column name check failed.')
        # self.assertEqual(sqlentity.columns[0].type, 'date', 'Column type check failed.')
        self.assertEqual(sqlentity.columns[0].action, SQLDDLAction.DROPCOLUMN, 'Column action check failed.')