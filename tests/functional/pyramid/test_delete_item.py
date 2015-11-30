import json
import unittest

TABLE_NAME = 'Table-HR'
TABLE_NAME_404 = 'Waldo'
TABLE_RT = 45
TABLE_WT = 123
TABLE_HK_NAME = u'hash_key'
TABLE_HK_TYPE = u'N'
TABLE_RK_NAME = u'range_key'
TABLE_RK_TYPE = u'S'

HK_VALUE = u'123'
RK_VALUE = u'Decode this data if you are a coder'

HK = {TABLE_HK_TYPE: HK_VALUE}
RK = {TABLE_RK_TYPE: RK_VALUE}

ITEM = {
    TABLE_HK_NAME: HK,
    TABLE_RK_NAME: RK,
    u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
}

HEADERS = {
    'x-amz-target': 'dynamodb_20111205.DeleteItem',
    'content-type': 'application/x-amz-json-1.0',
}


# Goal here is not to test the full API, this is done by the Boto tests
@unittest.skip("don't care right now")
class TestDeleteItem(unittest.TestCase):
    def setUp(self):
        from ddbmock.database.db import dynamodb
        from ddbmock.database.table import Table
        from ddbmock.database.key import PrimaryKey

        import helpers
        self.app = helpers.makeTestApp()
        dynamodb.hard_reset()

        hash_key = PrimaryKey(TABLE_HK_NAME, TABLE_HK_TYPE)
        range_key = PrimaryKey(TABLE_RK_NAME, TABLE_RK_TYPE)

        self.t1 = Table(TABLE_NAME, TABLE_RT, TABLE_WT, hash_key, range_key)
        dynamodb.data[TABLE_NAME] = self.t1
        self.t1.put(ITEM, {})

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        dynamodb.hard_reset()

    def test_delete_item_hr(self):
        request = {
            "TableName": TABLE_NAME,
            "Key": {
                "HashKeyElement": HK,
                "RangeKeyElement": RK,
            },
        }
        expected = {
            u"ConsumedCapacityUnits": 1,
        }

        # Protocol check
        res = self.app.post_json('/', request, headers=HEADERS, status=200)
        self.assertEqual(expected, json.loads(res.body))
        self.assertEqual('application/x-amz-json-1.0; charset=UTF-8',
                         res.headers['Content-Type'])

        # Live data check
        self.assertNotIn((HK_VALUE, RK_VALUE), self.t1.store)
