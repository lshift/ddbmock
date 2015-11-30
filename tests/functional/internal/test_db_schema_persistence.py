# -*- coding: utf-8 -*-

import unittest, mock

# This module both validates schema persitence/reloading and that sqlite backend is
# actually working

TABLE_NAME = "tabloid"

TABLE_RT = 45
TABLE_WT = 123

HASH_KEY = {"AttributeName":"hash_key","KeyType":"HASH"}
RANGE_KEY = {"AttributeName":"range_key","KeyType":"RANGE"}

class TestDBSchemaPersist(unittest.TestCase):
    @mock.patch('ddbmock.database.db.Store')
    def test_db_schema_persistence(self, m_store):
        from ddbmock.database.db import DynamoDB
        from ddbmock.database import storage
        from ddbmock.database.storage.sqlite import Store

        data = {
            "TableName": TABLE_NAME,
            "KeySchema": [HASH_KEY, RANGE_KEY],
            "ProvisionedThroughput": {
                "ReadCapacityUnits": TABLE_RT,
                "WriteCapacityUnits": TABLE_WT,
            }
        }

        m_store.return_value = Store("whatever")

        # reset internal state + force SQLite
        old_internal_state = DynamoDB._shared_data
        DynamoDB._shared_data = {'data': {}, 'store': None}

        # data creation round
        dynamodb = DynamoDB()
        dynamodb.create_table(TABLE_NAME, data)

        # reset internal state (again) to simulate reload
        DynamoDB._shared_data = {'data': {}, 'store': None}
        dynamodb = DynamoDB()

        table = dynamodb.data[TABLE_NAME]
        self.assertEqual(TABLE_NAME, table.name)
        self.assertEqual(TABLE_RT, table.rt)
        self.assertEqual(TABLE_WT, table.wt)

        DynamoDB._shared_data = old_internal_state
