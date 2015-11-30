import json
import time
import unittest

import mock

NOW = time.time()

TABLE_NAME = u'Table-1'
TABLE_RT = 45
TABLE_WT = 123
TABLE_HK_NAME = u'hash_key'
TABLE_HK_TYPE = u'N'
TABLE_RK_NAME = u'range_key'
TABLE_RK_TYPE = u'S'

HEADERS = {
    'x-amz-target': 'dynamodb_20111205.DeleteTable',
    'content-type': 'application/x-amz-json-1.0',
}


# Goal here is not to test the full API, this is done by the Boto tests
@unittest.skip("don't care right now")
class TestDeleteTable(unittest.TestCase):
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
                   status='ACTIVE')
        dynamodb.data[TABLE_NAME] = t1

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        dynamodb.hard_reset()

    def test_delete_table(self):
        request = {"TableName": TABLE_NAME}

        expected = {
            u'TableDescription': {
                u'CreationDateTime': NOW,
                u'KeySchema': [
                    {
                        u'AttributeName': TABLE_HK_NAME,
                        u'KeyType': TABLE_HK_TYPE,
                    },
                    {
                        u'AttributeName': TABLE_RK_NAME,
                        u'KeyType': TABLE_RK_TYPE,
                    }
                ],
                u'ProvisionedThroughput': {
                    u'ReadCapacityUnits': TABLE_RT,
                    u'WriteCapacityUnits': TABLE_WT,
                },
                u'TableName': TABLE_NAME,
                u'TableStatus': u'DELETING',
            }
        }

        res = self.app.post_json('/', request, headers=HEADERS, status=200)
        self.assertEqual(expected, json.loads(res.body))
        self.assertEqual('application/x-amz-json-1.0; charset=UTF-8',
                         res.headers['Content-Type'])
