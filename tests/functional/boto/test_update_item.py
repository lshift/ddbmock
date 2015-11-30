# -*- coding: utf-8 -*-

import unittest
import boto
from decimal import Decimal
from copy import deepcopy as cp

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
RK_VALUE = u'Decode this data if you are a coder'

RELEVANT_FIELD = {'S': 'Illyse'}
IRELEVANT_FIELD = {'B': 'WW91IHdpc2ggeW91IGNvdWxkIGNoYW5nZSB5b3VyIGpvYi4uLg=='}

FIELD_NAME = u"relevant_data"
FIELD_NAME_404 = u"ze dummy field name"
FIELD_SET_NAME = u"data list"
FIELD_NUM_NAME = u"counter"

RELEVANT_HUGE_FIELD = {'S': 'a'*64*1024}

ITEM = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE},
    FIELD_NAME: {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
    FIELD_SET_NAME: {u'SS': [u'item1', u'item2', u'item3', u'item4']}
}

ITEM2 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
    FIELD_NUM_NAME: {u'N': u'42'}

}

FIELD_SMALL = {u'S': u'a'}
FIELD_BIG   = {u'S': u'a'*1024}



@unittest.skip("don't care right now")
class TestUpdateItem(unittest.TestCase):
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

        self.t1.put(cp(ITEM), {})
        self.t2.put(cp(ITEM2), {})

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        from ddbmock import clean_boto_patch
        dynamodb.hard_reset()
        clean_boto_patch()

    def test_update_item_put_hr(self):
        from ddbmock import connect_boto_patch
        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        key2 = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE2},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        # use PUT as default action, champs existant
        db.update_item(TABLE_NAME, key, {
            'relevant_data': {'Value': RELEVANT_FIELD}  # Move type from 'B' to 'S'
        })
        self.assertEqual(RELEVANT_FIELD, self.t1.store[HK_VALUE, RK_VALUE]['relevant_data'])

        # PUT explicite, champ non existant
        db.update_item(TABLE_NAME, key, {
            'irelevant_data': {'Action': 'PUT', 'Value': IRELEVANT_FIELD}
        })
        self.assertEqual(RELEVANT_FIELD, self.t1.store[HK_VALUE, RK_VALUE]['relevant_data'])
        self.assertEqual(IRELEVANT_FIELD, self.t1.store[HK_VALUE, RK_VALUE]['irelevant_data'])

        # PUT explicite, item non existant(full item creation)
        db.update_item(TABLE_NAME, key2, {
            'relevant_data': {'Action': 'PUT', 'Value': RELEVANT_FIELD}
        })
        self.assertEqual({TABLE_HK_TYPE: HK_VALUE2}, self.t1.store[HK_VALUE2, RK_VALUE][TABLE_HK_NAME])
        self.assertEqual({TABLE_RK_TYPE: RK_VALUE}, self.t1.store[HK_VALUE2, RK_VALUE][TABLE_RK_NAME])
        self.assertEqual(RELEVANT_FIELD, self.t1.store[HK_VALUE2, RK_VALUE]['relevant_data'])

    def test_update_item_put_h(self):
        from ddbmock import connect_boto_patch
        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
        }

        key2 = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE2},
        }

        # use PUT as default action, champs existant
        db.update_item(TABLE_NAME2, key, {
            'relevant_data': {'Value': RELEVANT_FIELD}  # Move type from 'B' to 'S'
        })
        self.assertEqual(RELEVANT_FIELD, self.t2.store[HK_VALUE, False]['relevant_data'])

        # PUT explicite, champ non existant
        db.update_item(TABLE_NAME2, key, {
            'irelevant_data': {'Action': 'PUT', 'Value': IRELEVANT_FIELD}
        })
        self.assertEqual(RELEVANT_FIELD, self.t2.store[HK_VALUE, False]['relevant_data'])
        self.assertEqual(IRELEVANT_FIELD, self.t2.store[HK_VALUE, False]['irelevant_data'])

        # PUT explicite, item non existant(full item creation)
        db.update_item(TABLE_NAME2, key2, {
            'relevant_data': {'Action': 'PUT', 'Value': RELEVANT_FIELD}
        })
        self.assertEqual({TABLE_HK_TYPE: HK_VALUE2}, self.t2.store[HK_VALUE2, False][TABLE_HK_NAME])
        self.assertEqual(RELEVANT_FIELD, self.t2.store[HK_VALUE2, False]['relevant_data'])

    def test_put_check_throughput_max_old_new(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        db = connect_boto_patch()

        key = {u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE}}

        self.assertEqual(
            {u'ConsumedCapacityUnits': 1},
            db.update_item(TABLE_NAME2, key,
                                  {FIELD_NAME: {'Action': 'PUT', 'Value': FIELD_SMALL}}),
        )
        self.assertEqual(
            {u'ConsumedCapacityUnits': 2},
            db.update_item(TABLE_NAME2, key,
                                  {FIELD_NAME: {'Action': 'PUT', 'Value': FIELD_BIG}}),
        )
        self.assertEqual(
            {u'ConsumedCapacityUnits': 2},
            db.update_item(TABLE_NAME2, key,
                                  {FIELD_NAME: {'Action': 'PUT', 'Value': FIELD_SMALL}}),
        )

    def test_update_item_delete_primary_key_fails(self):
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertRaises(DynamoDBValidationError,
            db.update_item,
            TABLE_NAME, key, {TABLE_RK_NAME: {'Action': 'DELETE'}}
        )
        self.assertEqual({TABLE_RK_TYPE: RK_VALUE}, self.t1.store[HK_VALUE, RK_VALUE][TABLE_RK_NAME])

        self.assertRaises(DynamoDBValidationError,
            db.update_item,
            TABLE_NAME, key, {TABLE_HK_NAME: {'Action': 'DELETE'}}
        )
        self.assertEqual({TABLE_HK_TYPE: HK_VALUE}, self.t1.store[HK_VALUE, RK_VALUE][TABLE_HK_NAME])

    def test_update_item_delete_field_ok(self):
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        db.update_item(TABLE_NAME, key, {
            FIELD_NAME: {'Action': 'DELETE'},
        })
        self.assertNotIn(FIELD_NAME, self.t1.store[HK_VALUE, RK_VALUE])

        # Attempt to delete non-existing field, do nothing
        db.update_item(TABLE_NAME, key, {
            FIELD_NAME_404: {'Action': 'DELETE'},
        })

    def test_update_item_delete_field_set_ok(self):
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        expected1 = {u'SS': [u'item1', u'item2', u'item3', u'item4']}
        expected2 = {u'SS': [u'item3', u'item1']}

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        # Can not remove a scalar value, even from a set
        self.assertRaises(DynamoDBValidationError,
            db.update_item,
            TABLE_NAME, key, {
                FIELD_SET_NAME: {'Action': 'DELETE', u'Value': {u'S': u'item1'}},
            }
        )
        self.assertEqual(expected1, self.t1.store[HK_VALUE, RK_VALUE][FIELD_SET_NAME])

        # remove a couple of existing or not item from the field
        db.update_item(TABLE_NAME, key, {
            FIELD_SET_NAME: {'Action': 'DELETE', u'Value': {u'SS': [u'item2', u'item4', u'item6']}},
        })
        self.assertEqual(expected2, self.t1.store[HK_VALUE, RK_VALUE][FIELD_SET_NAME])

        # Field shoud disapear (Empty)
        db.update_item(TABLE_NAME, key, {
            FIELD_SET_NAME: {'Action': 'DELETE', u'Value': {u'SS': [u'item1', u'item3', u'item6']}},
        })
        self.assertNotIn(FIELD_SET_NAME, self.t1.store[HK_VALUE, RK_VALUE])

    def test_update_item_delete_field_set_bad_type(self):
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        expected = {u'SS': [u'item1', u'item2', u'item3', u'item4']}

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertRaises(DynamoDBValidationError,
            db.update_item,
            TABLE_NAME, key, {
                FIELD_SET_NAME: {'Action': 'DELETE', u'Value': {u'B': u'item1'}},
            }
        )
        self.assertEqual(expected, self.t1.store[HK_VALUE, RK_VALUE][FIELD_SET_NAME])

        self.assertRaises(DynamoDBValidationError,
            db.update_item,
            TABLE_NAME, key, {
                FIELD_SET_NAME: {'Action': 'DELETE', u'Value': {u'BS': [u'item2', u'item4', u'item6']}},
            }
        )
        self.assertEqual(expected, self.t1.store[HK_VALUE, RK_VALUE][FIELD_SET_NAME])

    def test_update_item_increment(self):
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
        }

        ADD_VALUE = 2

        expected = {u'N': unicode(42 + ADD_VALUE)}

        # regular increment
        db.update_item(TABLE_NAME2, key, {
            FIELD_NUM_NAME: {'Action': 'ADD', u'Value': {u'N': unicode(ADD_VALUE)}},
        })
        self.assertEqual(expected, self.t2.store[HK_VALUE, False][FIELD_NUM_NAME])

    def test_update_item_push_to_set_ok(self):
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        expected1 = {u'SS': [u'item1', u'item2', u'item3', u'item4']}
        expected2 = {u'SS': [u'item2', u'item3', u'item1', u'item4', u'item5']}

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertRaises(DynamoDBValidationError,
            db.update_item,
            TABLE_NAME, key, {
                FIELD_SET_NAME: {'Action': 'ADD', u'Value': {u'S': u'item5'}},
            }
        )
        self.assertEqual(expected1, self.t1.store[HK_VALUE, RK_VALUE][FIELD_SET_NAME])

        db.update_item(TABLE_NAME, key, {
            FIELD_SET_NAME: {'Action': 'ADD', u'Value': {u'SS': [u'item5']}},
        })
        self.assertEqual(expected2, self.t1.store[HK_VALUE, RK_VALUE][FIELD_SET_NAME])

    def test_update_item_push_to_non_set_fail(self):
        # sometimes weird black magic types occures in test. these are related
        # to internal "DB" logic. it does not affect real API output at all
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        db = connect_boto_patch()

        expected1 = {u'SS': [u'item1', u'item2', u'item3', u'item4']}

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
            u"RangeKeyElement": {TABLE_RK_TYPE: RK_VALUE},
        }

        self.assertRaises(DynamoDBValidationError,
            db.update_item,
            TABLE_NAME, key, {
                FIELD_NAME: {'Action': 'ADD', u'Value': {u'B': u'item5'}},
            }
        )
        self.assertEqual(expected1, self.t1.store[HK_VALUE, RK_VALUE][FIELD_SET_NAME])

    def test_update_return_all_old(self):
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        key = {u"HashKeyElement": {TABLE_HK_TYPE: HK_VALUE}}
        ADD_VALUE = 1

        db = connect_boto_patch()
        expected = cp(ITEM2)

        # regular increment
        ret = db.update_item(TABLE_NAME2, key, {
                FIELD_NUM_NAME: {'Action': 'ADD', u'Value': {u'N': unicode(ADD_VALUE)}},
            },
            return_values=u'ALL_OLD',
         )
        self.assertEqual(expected, ret[u'Attributes'])

    def test_update_return_all_new(self):
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        key = {u"HashKeyElement": {TABLE_HK_TYPE: HK_VALUE}}
        ADD_VALUE = 1

        db = connect_boto_patch()
        expected = cp(ITEM2)
        expected[FIELD_NUM_NAME][u'N'] = unicode(int(expected[FIELD_NUM_NAME][u'N']) + ADD_VALUE)

        # regular increment
        ret = db.update_item(TABLE_NAME2, key, {
                FIELD_NUM_NAME: {'Action': 'ADD', u'Value': {u'N': unicode(ADD_VALUE)}},
            },
            return_values=u'ALL_NEW',
        )
        self.assertEqual(expected, ret[u'Attributes'])

    def test_update_return_updated_old(self):
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        key = {u"HashKeyElement": {TABLE_HK_TYPE: HK_VALUE}}
        ADD_VALUE = 1

        db = connect_boto_patch()
        expected = {FIELD_NUM_NAME: cp(ITEM2[FIELD_NUM_NAME])}

        # regular increment
        ret = db.update_item(TABLE_NAME2, key, {
                FIELD_NUM_NAME: {'Action': 'ADD', u'Value': {u'N': unicode(ADD_VALUE)}},
            },
            return_values=u'UPDATED_OLD',
         )
        self.assertEqual(expected, ret[u'Attributes'])

    def test_update_return_updated_new(self):
        from ddbmock import connect_boto_patch
        from boto.dynamodb.exceptions import DynamoDBValidationError

        key = {u"HashKeyElement": {TABLE_HK_TYPE: HK_VALUE}}
        ADD_VALUE = 1

        db = connect_boto_patch()
        expected = {FIELD_NUM_NAME: cp(ITEM2[FIELD_NUM_NAME])}
        expected[FIELD_NUM_NAME][u'N'] = unicode(int(expected[FIELD_NUM_NAME][u'N']) + ADD_VALUE)

        # regular increment
        ret = db.update_item(TABLE_NAME2, key, {
                FIELD_NUM_NAME: {'Action': 'ADD', u'Value': {u'N': unicode(ADD_VALUE)}},
            },
            return_values=u'UPDATED_NEW',
         )
        self.assertEqual(expected, ret[u'Attributes'])

    def test_update_item_put_h_oversized(self):
        from boto.dynamodb.exceptions import DynamoDBValidationError
        from ddbmock import connect_boto_patch
        db = connect_boto_patch()

        key = {
            u"HashKeyElement":  {TABLE_HK_TYPE: HK_VALUE},
        }

        # PUT explicite, existing field
        self.assertRaisesRegexp(DynamoDBValidationError, 'Item size.*exceeded',
        db.update_item, TABLE_NAME2, key, {
            'relevant_data': {'Value': RELEVANT_HUGE_FIELD},
        })
        self.assertEqual(ITEM[FIELD_NAME], self.t2.store[HK_VALUE, False]['relevant_data'])
