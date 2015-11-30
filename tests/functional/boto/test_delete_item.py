# -*- coding: utf-8 -*-

import unittest
import boto

TABLE_NAME = 'Table-HR'
TABLE_NAME2 = 'Table-H'
TABLE_NAME_404 = 'Waldo'
TABLE_RT = 45
TABLE_WT = 123
TABLE_RT2 = 10
TABLE_WT2 = 10
TABLE_HK_NAME = u'hash_key'
TABLE_HK_TYPE = u'N'
TABLE_RK_NAME = u'range_key'
TABLE_RK_TYPE = u'S'

HK_VALUE = u'123'
HK_VALUE2 = u'456'
HK_VALUE_404 = u'404'
RK_VALUE = u'Decode this data if you are a coder'


ITEM = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE},
    u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
}
ITEM2 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
}
ITEM_BIG = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE2},
    u'relevant_data': {u'S': u'a'*1024},
}

@unittest.skip("don't care right now")
class TestDeleteItem(unittest.TestCase):
    def setUp(self):
        from ddbmock.database.db import dynamodb
        from ddbmock.database.table import Table
        from ddbmock.database.key import PrimaryKey

        dynamodb.hard_reset()

        hash_key = PrimaryKey(TABLE_HK_NAME, TABLE_HK_TYPE)
        range_key = PrimaryKey(TABLE_RK_NAME, TABLE_RK_TYPE)

        self.t1 = Table(TABLE_NAME, TABLE_RT, TABLE_WT, hash_key, range_key)
        self.t2 = Table(TABLE_NAME2, TABLE_RT, TABLE_WT, hash_key, None)

        dynamodb.data[TABLE_NAME]  = self.t1
        dynamodb.data[TABLE_NAME2] = self.t2

        self.t1.put(ITEM, {})
        self.t2.put(ITEM2, {})
        self.t2.put(ITEM_BIG, {})

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        from ddbmock import clean_boto_patch
        dynamodb.hard_reset()
        clean_boto_patch()

    def test_delete_item_hr(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertEqual({
                u'ConsumedCapacityUnits': 1,
            },
            db.delete_item(TABLE_NAME, key),
        )
        self.assertNotIn((HK_VALUE, RK_VALUE), self.t1.store)

    def test_delete_item_hr_404(self):
        # same behavior as found
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE_404},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertEqual({
                u'ConsumedCapacityUnits': 1,
            },
            db.delete_item(TABLE_NAME, key),
        )
        self.assertNotIn((HK_VALUE_404, RK_VALUE), self.t1.store)

    def test_delete_item_hr_old(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        expected = {
            u'ConsumedCapacityUnits': 1,
            u'Attributes': ITEM,
        }

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertEqual(
            expected,
            db.delete_item(TABLE_NAME, key, return_values=u'ALL_OLD'),
        )
        self.assertNotIn((HK_VALUE, RK_VALUE), self.t1.store)

    def test_delete_item_h(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
        }

        self.assertEqual({
                u'ConsumedCapacityUnits': 1,
            },
            db.delete_item(TABLE_NAME2, key),
        )
        self.assertNotIn((HK_VALUE, False), self.t2.store)

    def test_delete_item_h_big(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE2},
        }

        self.assertEqual({
                u'ConsumedCapacityUnits': 2,
            },
            db.delete_item(TABLE_NAME2, key),
        )
        self.assertNotIn((HK_VALUE2, False), self.t2.store)

    def test_delete_item_h_old(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        expected = {
            u'ConsumedCapacityUnits': 1,
            u'Attributes': ITEM2,
        }

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
        }

        self.assertEqual(
            expected,
            db.delete_item(TABLE_NAME2, key, return_values=u'ALL_OLD'),
        )
        self.assertNotIn((HK_VALUE, False), self.t1.store)

    def test_delete_item_hr_missing_r(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
        }

        self.assertRaises(DynamoDBValidationError,
                          db.delete_item,
                          TABLE_NAME, key)

    def test_delete_item_h_missing_h(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        key = {}

        self.assertRaises(DynamoDBValidationError,
                          db.delete_item,
                          TABLE_NAME2, key)

    def test_delete_item_h_expect_field_value_ok(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.exception import DynamoDBResponseError

        db = connect_boto_patch()

        ddb_expected = {
            u'relevant_data': {
                u'Exists': True,
                u'Value': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='}
            }
        }

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        db.delete_item(TABLE_NAME, key, expected=ddb_expected)
        self.assertNotIn((HK_VALUE, False), self.t1.store)

    def test_delete_item_h_expect_field_value_fail(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBConditionalCheckFailedError

        db = connect_boto_patch()

        ddb_expected = {
            u'relevant_data_et_bah_non': {
                u'Exists': True,
                u'Value': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='}
            }
        }

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertRaisesRegexp(DynamoDBConditionalCheckFailedError, 'ConditionalCheckFailedException',
            db.delete_item,
            TABLE_NAME, key, expected=ddb_expected
        )
        self.assertEqual(ITEM, self.t1.store[HK_VALUE, RK_VALUE])
