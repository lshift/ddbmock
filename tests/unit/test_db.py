# -*- coding: utf-8 -*-

import unittest, mock

# tests
# - delete callback
# - create table persist schema

TABLE_NAME = "tabloid"
TABLE_NAME2 = "razoroid"

TABLE_RT = 45
TABLE_WT = 123

HASH_KEY = {"AttributeName":"hash_key","KeyType":"HASH"}

class TestDB(unittest.TestCase):
    def setUp(self):
        from ddbmock.database import dynamodb

        self.t1 = mock.Mock()
        self.t1.name = TABLE_NAME

        dynamodb.data[TABLE_NAME] = self.t1
        dynamodb.store[TABLE_NAME, False] = dynamodb.data[TABLE_NAME]

    def test_internal_delete_table(self):
        from ddbmock.database import dynamodb

        # make sure that deleting another table with the same name does nothing
        t2 = mock.Mock()
        t2.name = TABLE_NAME

        # delete a table
        dynamodb._internal_delete_table(t2)
        self.assertIn(TABLE_NAME, dynamodb.data)

        # make sure deleting already deleted table does not harm
        dynamodb._internal_delete_table(self.t1)

    @mock.patch('ddbmock.database.db.schedule_action')
    def test_delete_table(self, m_schedule):
        from ddbmock import config
        from ddbmock.database import dynamodb

        dynamodb.delete_table(TABLE_NAME)
        dynamodb.data[TABLE_NAME].delete.assert_called_with()
        m_schedule.assert_called_with(config.DELAY_DELETING, dynamodb._internal_delete_table, [dynamodb.data[TABLE_NAME]])

    @mock.patch('ddbmock.database.db.dynamodb.store')
    def test_create_table_saves_schema(self, m_store):
        from ddbmock.database.db import dynamodb

        data = {
            "TableName": TABLE_NAME2,
            "KeySchema": [HASH_KEY],
            "ProvisionedThroughput": {
                "ReadCapacityUnits": TABLE_RT,
                "WriteCapacityUnits": TABLE_WT,
            }
        }

        dynamodb.create_table(TABLE_NAME2, data)

        m_store.__setitem__.assert_called_with(
                                               (TABLE_NAME2, False),
                                               dynamodb.data[TABLE_NAME2],
                                              )

    @mock.patch('ddbmock.database.db.Store')
    def test_init_reloads_schema(self, m_store):
        from ddbmock.database.db import DynamoDB
        old_internal_state = DynamoDB._shared_data
        DynamoDB._shared_data = {'data': {}, 'store': None}

        table1 = mock.Mock()
        table2 = mock.Mock()
        table1.name = TABLE_NAME
        table2.name = TABLE_NAME2
        m_store.return_value.__iter__.return_value = [table1, table2]

        dynamodb = DynamoDB()

        m_store.assert_called_with('~*schema*~')
        self.assertIn(TABLE_NAME, dynamodb.data)
        self.assertIn(TABLE_NAME2, dynamodb.data)
        self.assertEqual(table1, dynamodb.data[TABLE_NAME])
        self.assertEqual(table2, dynamodb.data[TABLE_NAME2])


        DynamoDB._shared_data = old_internal_state
