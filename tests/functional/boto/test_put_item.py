# -*- coding: utf-8 -*-

import unittest
import boto

TABLE_NAME = 'Table-HR'
TABLE_NAME2 = 'Table-H'
TABLE_NAME3 = 'Table-HR-size'
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
RK_VALUE = u'Decode this data if you are a coder'


ITEM = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE},
    u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
}
ITEM2 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE},
    u'irelevant_data': {u'B': u'WW91IHdpc2ggeW91IGNvdWxkIGNoYW5nZSB5b3VyIGpvYi4uLg=='},
}
ITEM3 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
}
ITEM3_EMPTY_FIELD = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    u'relevant_data': {u'B': u''},
}
ITEM3_EMPTY_SET = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
    u'empty_set': {u'NS': []},
}
ITEM4 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    u'irelevant_data': {u'B': u'WW91IHdpc2ggeW91IGNvdWxkIGNoYW5nZSB5b3VyIGpvYi4uLg=='},
}
ITEM5 = {
    u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
}


ITEM_BIG = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    u'relevant_data': {u'S': u'a'*1024},
}

ITEM_HUGE = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    u'relevant_data': {u'S': u'a'*64*1024},  # thsi field itself is.... too big
}

ITEM_OVER_H = {
    TABLE_HK_NAME: {u'S': 'a'*2049},
    TABLE_RK_NAME: {u'S': 'a'},
}
ITEM_MAX_H = {
    TABLE_HK_NAME: {u'S': 'a'*2048},
    TABLE_RK_NAME: {u'S': 'a'},
}
ITEM_OVER_R = {
    TABLE_HK_NAME: {u'S': 'a'},
    TABLE_RK_NAME: {u'S': 'a'*1025},
}
ITEM_MAX_R = {
    TABLE_HK_NAME: {u'S': 'a'},
    TABLE_RK_NAME: {u'S': 'a'*1024},
}

ITEM_REGRESION = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    "Views": {"N": "0"},  # the one failing, sighs
    "forum_name": {"S": "Amazon dynamodb"},
    "Tags": {"SS": ["index", "primarykey", "table"]},
    "LastPostDateTime": {"S": "12/9/2011 11:36:03 PM"},
    "LastPostedBy": {"S": "User A"},
    "Answered": {"N": "0"},
    "Replies": {"N": "0"},
    "Message": {"S": "dynamodb thread 1 message text"},
    "Public": {"N": "1"},
    "subject": {"S": "dynamodb Thread 1"}
}

class TestPutItem(unittest.TestCase):
    def setUp(self):
        from ddbmock.database.db import dynamodb
        from ddbmock.database.table import Table
        from ddbmock.database.key import PrimaryKey

        dynamodb.hard_reset()

        hash_key = PrimaryKey(TABLE_HK_NAME, TABLE_HK_TYPE)
        hash_key2 = PrimaryKey(TABLE_HK_NAME, TABLE_RK_TYPE)
        range_key = PrimaryKey(TABLE_RK_NAME, TABLE_RK_TYPE)

        self.t1 = Table(TABLE_NAME, TABLE_RT, TABLE_WT, hash_key, range_key)
        self.t2 = Table(TABLE_NAME2, TABLE_RT, TABLE_WT, hash_key, None)
        self.t3 = Table(TABLE_NAME3, TABLE_RT, TABLE_WT, hash_key2, range_key)

        dynamodb.data[TABLE_NAME]  = self.t1
        dynamodb.data[TABLE_NAME2] = self.t2
        dynamodb.data[TABLE_NAME3] = self.t3

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        from ddbmock import clean_boto_patch
        dynamodb.hard_reset()
        clean_boto_patch()

    def test_put_hr(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        self.assertEqual({
                u'ConsumedCapacityUnits': 1,
            },
            db.put_item(TABLE_NAME, ITEM),
        )
        self.assertEqual(ITEM, self.t1.store[HK_VALUE, RK_VALUE])

        self.assertEqual({
                u'ConsumedCapacityUnits': 1,
                u'Attributes': ITEM,
            },
            db.put_item(TABLE_NAME, ITEM2, return_values=u'ALL_OLD'),
        )

    def test_put_h(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        self.assertEqual({
                u'ConsumedCapacityUnits': 1,
            },
            db.put_item(TABLE_NAME2, ITEM3),
        )
        self.assertEqual(ITEM3, self.t2.store[HK_VALUE, False])

        self.assertEqual({
                u'ConsumedCapacityUnits': 1,
                u'Attributes': ITEM3,
            },
            db.put_item(TABLE_NAME2, ITEM4, return_values=u'ALL_OLD'),
        )

    def test_put_check_throughput_max_old_new(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        self.assertEqual(
            {u'ConsumedCapacityUnits': 1},
            db.put_item(TABLE_NAME2, ITEM3),
        )
        self.assertEqual(
            {u'ConsumedCapacityUnits': 2},
            db.put_item(TABLE_NAME2, ITEM_BIG),
        )
        self.assertEqual(
            {u'ConsumedCapacityUnits': 2},
            db.put_item(TABLE_NAME2, ITEM3),
        )

    def test_put_h_empty_field_fail(self):
        # From http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_PutItem.html
        # Attribute values may not be null; string and binary type attributes must have lengths greater than zero; and set type attributes must not be empty. Requests with empty values will be rejected with a ValidationException.
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        self.assertRaises(DynamoDBValidationError,
                          db.put_item,
                          TABLE_NAME2, ITEM3_EMPTY_SET)

        self.assertNotIn((HK_VALUE, False), self.t2.store)

        self.assertRaises(DynamoDBValidationError,
                          db.put_item,
                          TABLE_NAME2, ITEM3_EMPTY_FIELD)

        self.assertNotIn((HK_VALUE, False), self.t2.store)

    def test_put_hr_404(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.exception import DynamoDBResponseError

        db = connect_boto_patch()

        self.assertRaisesRegexp(DynamoDBResponseError, 'ResourceNotFoundException',
                                db.put_item,
                                TABLE_NAME_404, ITEM)

    def test_put_h_404(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.exception import DynamoDBResponseError

        db = connect_boto_patch()

        self.assertRaisesRegexp(DynamoDBResponseError, 'ResourceNotFoundException',
                                db.put_item,
                                TABLE_NAME_404, ITEM3)

    def test_put_hr_missing_r(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        self.assertRaises(DynamoDBValidationError,
                          db.put_item,
                          TABLE_NAME, ITEM3)

    def test_put_h_missing_h(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        self.assertRaises(DynamoDBValidationError,
                          db.put_item,
                          TABLE_NAME2, ITEM5)

    def test_put_h_expect_no_exist(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.exception import DynamoDBResponseError

        db = connect_boto_patch()

        ddb_expected = {
            TABLE_HK_NAME: {u'Exists': False}
        }

        db.put_item(TABLE_NAME2, ITEM3, expected=ddb_expected)

        self.assertRaisesRegexp(DynamoDBResponseError, 'ConditionalCheckFailedException',
            db.put_item,
            TABLE_NAME2, ITEM4, expected=ddb_expected
        )
        self.assertEqual(ITEM3, self.t2.store[HK_VALUE, False])

    def test_put_h_expect_field_value(self):
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

        db.put_item(TABLE_NAME2, ITEM3)
        self.assertEqual(ITEM3, self.t2.store[HK_VALUE, False])
        db.put_item(TABLE_NAME2, ITEM4, expected=ddb_expected)
        self.assertEqual(ITEM4, self.t2.store[HK_VALUE, False])
        self.assertRaisesRegexp(DynamoDBResponseError, 'ConditionalCheckFailedException',
            db.put_item,
            TABLE_NAME2, ITEM4, expected=ddb_expected
        )

    def test_put_oversized_h(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        db.put_item(TABLE_NAME3, ITEM_MAX_H)
        self.assertRaisesRegexp(DynamoDBValidationError, 'bytes',
            db.put_item,
            TABLE_NAME3, ITEM_OVER_H)

    def test_put_oversized_r(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        db.put_item(TABLE_NAME3, ITEM_MAX_R)
        self.assertRaisesRegexp(DynamoDBValidationError, 'bytes',
            db.put_item,
            TABLE_NAME3, ITEM_OVER_R)

    def test_put_oversized_item(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        self.assertRaisesRegexp(DynamoDBValidationError, 'Item size.*exceeded',
            db.put_item,
            TABLE_NAME2, ITEM_HUGE)

        self.assertNotIn((HK_VALUE, False), self.t2.store)

    def test_put_boto_intergration(self):
        # This item comes directly from boto intergration test suite
        # we do no assertion. It should "just work"

        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()
        db.put_item(TABLE_NAME2, ITEM_REGRESION)
