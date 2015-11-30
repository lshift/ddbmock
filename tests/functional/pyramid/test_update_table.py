
import json
import time
import unittest

import mock

NOW = time.time()
NOW2 = time.time() + 42 * 1000

TABLE_NAME = u'Table-1'
TABLE_RT = 45
TABLE_WT = 123
TABLE_RT2 = 10
TABLE_WT2 = 10
TABLE_HK_NAME = u'hash_key'
TABLE_HK_TYPE = u'N'
TABLE_RK_NAME = u'range_key'
TABLE_RK_TYPE = u'S'

HEADERS = {
    'x-amz-target': 'dynamodb_20111205.UpdateTable',
    'content-type': 'application/x-amz-json-1.0',
}


# Goal here is not to test the full API, this is done by the Boto tests
@unittest.skip("don't care right now")
class TestUpdateTable(unittest.TestCase):
    @mock.patch("ddbmock.database.table.time")  # Brrr
    def setUp(self, m_time):
        from ddbmock.database.db import dynamodb
        from ddbmock.database.table import Table
        from ddbmock.database.key import PrimaryKey

        import helpers
        self.app = helpers.makeTestApp()

        m_time.time.return_value = NOW

        dynamodb.hard_reset()

        hash_key = PrimaryKey(TABLE_HK_NAME, TABLE_HK_TYPE)
        range_key = PrimaryKey(TABLE_RK_NAME, TABLE_RK_TYPE)
        t1 = Table(TABLE_NAME, TABLE_RT, TABLE_WT, hash_key, range_key,
                   status="ACTIVE")
        dynamodb.data[TABLE_NAME] = t1

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        dynamodb.hard_reset()

    @mock.patch("ddbmock.database.table.time")
    def test_update(self, m_time):
        from ddbmock.database.db import dynamodb

        m_time.time.return_value = NOW2

        self.maxDiff = None

        request = {
            "TableName": TABLE_NAME,
            "ProvisionedThroughput": {
                "ReadCapacityUnits": TABLE_RT2,
                "WriteCapacityUnits": TABLE_WT2,
            },
        }

        expected = {
            u'TableDescription': {
                u'CreationDateTime': NOW,
                u'ItemCount': 0,
                u'KeySchema': [
                    {
                        u'AttributeName': u'hash_key',
                        u'KeyType': u'N',
                    },
                    {
                        u'AttributeName': u'range_key',
                        u'KeyType': u'S',
                    }
                ],
                u'ProvisionedThroughput': {
                    u'LastDecreaseDateTime': NOW2,
                    u'ReadCapacityUnits': TABLE_RT2,
                    u'WriteCapacityUnits': TABLE_WT2,
                },
                u'TableName': TABLE_NAME,
                u'TableSizeBytes': 0,
                u'TableStatus': u'UPDATING'
            }
        }

        # Protocol check
        res = self.app.post_json('/', request, headers=HEADERS, status=200)
        self.assertEqual(expected, json.loads(res.body))
        self.assertEqual('application/x-amz-json-1.0; charset=UTF-8',
                         res.headers['Content-Type'])

        # Live data check
        data = dynamodb.data
        assert TABLE_NAME in data
        table = data[TABLE_NAME]

        self.assertEqual(TABLE_RT2, table.rt)
        self.assertEqual(TABLE_WT2, table.wt)
