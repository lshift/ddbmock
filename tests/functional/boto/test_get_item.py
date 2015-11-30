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

class TestGetItem(unittest.TestCase):
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

    def test_get_hr(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        expected = {
            u'ConsumedCapacityUnits': 0.5,
            u'Item': ITEM,
        }

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertEquals(expected, db.get_item(TABLE_NAME, key))

    def test_get_hr_consistent(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        expected = {
            u'ConsumedCapacityUnits': 1,
            u'Item': ITEM,
        }

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertEquals(expected, db.get_item(TABLE_NAME, key, consistent_read=True))

    def test_get_h(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        expected = {
            u'ConsumedCapacityUnits': 0.5,
            u'Item': ITEM2,
        }

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
        }

        self.assertEquals(expected, db.get_item(TABLE_NAME2, key))

    @unittest.skip("don't care for now")
    def test_get_consistent_big_attributes_to_get(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        expected = {
            u'ConsumedCapacityUnits': 2,
            u'Item': {TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE2}},
        }

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE2},
        }

        self.assertEquals(expected, db.get_item(TABLE_NAME2, key, consistent_read=True, attributes_to_get=[TABLE_HK_NAME]))

    def test_get_h_404(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError

        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE_404},
        }

        expected = {
            u'ConsumedCapacityUnits': 0.5,
        }

        self.assertEquals(expected, db.get_item(TABLE_NAME2, key))

    @unittest.skip("don't care for now")
    def test_get_hr_attr_to_get(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        expected = {
            u'ConsumedCapacityUnits': 0.5,
            u'Item': {
                u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='}}
        }

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertEquals(expected, db.get_item(TABLE_NAME, key, attributes_to_get=[u'relevant_data']))
