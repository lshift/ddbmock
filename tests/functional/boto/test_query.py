# -*- coding: utf-8 -*-

import unittest
import boto

TABLE_NAME = 'Table-HR'
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
HK_VALUE_404 = u'404'
RK_VALUE1 = u'Waldo-1'
RK_VALUE2 = u'Waldo-2'
RK_VALUE3 = u'Waldo-3'
RK_VALUE4 = u'Waldo-4'
RK_VALUE5 = u'Waldo-5'


ITEM1 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE1},
    u'relevant_data': {u'S': u'tata'},
}
ITEM2 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE2},
    u'relevant_data': {u'S': u'tete'},
}
ITEM3 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE3},
    u'relevant_data': {u'S': u'titi'},
}
ITEM4 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE4},
    u'relevant_data': {u'S': u'toto'},
}
ITEM5 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE5},
    u'relevant_data': {u'S': u'tutu'},
}

# Please note that most query features are not yet implemented hence not tested
@unittest.skip("don't care right now")
class TestQuery(unittest.TestCase):
    def setUp(self):
        from ddbmock.database.db import dynamodb
        from ddbmock.database.table import Table
        from ddbmock.database.key import PrimaryKey

        dynamodb.hard_reset()

        hash_key = PrimaryKey(TABLE_HK_NAME, TABLE_HK_TYPE)
        range_key = PrimaryKey(TABLE_RK_NAME, TABLE_RK_TYPE)

        self.t1 = Table(TABLE_NAME, TABLE_RT, TABLE_WT, hash_key, range_key)

        dynamodb.data[TABLE_NAME]  = self.t1

        self.t1.put(ITEM1, {})
        self.t1.put(ITEM2, {})
        self.t1.put(ITEM3, {})
        self.t1.put(ITEM4, {})
        self.t1.put(ITEM5, {})

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        from ddbmock import clean_boto_patch
        dynamodb.hard_reset()
        clean_boto_patch()

    def test_query_all(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 5,
            u"Items": [ITEM1, ITEM2, ITEM3, ITEM4, ITEM5],
            u"ConsumedCapacityUnits": 0.5,
        }

        db = connect_boto_patch()

        ret = db.query(TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE})
        self.assertEqual(expected, ret)

    # Regression test for #9
    def test_query_all_404(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 0,
            u'Items': [],
            u"ConsumedCapacityUnits": 0.5,
        }

        db = connect_boto_patch()

        ret = db.query(TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE_404})
        self.assertEqual(expected, ret)

    def test_query_2_first(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 2,
            u"Items": [ITEM1, ITEM2],
            u"ConsumedCapacityUnits": 0.5,
            u'LastEvaluatedKey': {
                u'HashKeyElement': {u'N': u'123'},
                u'RangeKeyElement': {u'S': u'Waldo-2'},
            },
        }

        db = connect_boto_patch()

        ret = db.query(TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE}, limit=2)
        self.assertEqual(expected, ret)

    def test_query_paged(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        esk = {
            u'HashKeyElement': {u'N': u'123'},
            u'RangeKeyElement': {u'S': u'Waldo-3'},
        }

        bad_esk = {
            u'HashKeyElement': {u'N': u'123.43'},
            u'RangeKeyElement': {u'S': u'Waldo-3'},
        }

        expected1 = {
            u"Count": 3,
            u"Items": [ITEM1, ITEM2, ITEM3],
            u"ConsumedCapacityUnits": 0.5,
            u'LastEvaluatedKey': esk,
        }
        expected2 = {
            u"Count": 2,
            u"Items": [ITEM4, ITEM5],
            u"ConsumedCapacityUnits": 0.5,
        }

        db = connect_boto_patch()

        ret = db.query(TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE}, limit=3)
        self.assertEqual(expected1, ret)
        ret = db.query(TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE}, limit=3, exclusive_start_key=esk)
        self.assertEqual(expected2, ret)
        self.assertRaises(DynamoDBValidationError,
                          db.query,
                          TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE}, limit=3, exclusive_start_key=bad_esk)

    def test_query_2_last(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 2,
            u"Items": [ITEM5, ITEM4],
            u"ConsumedCapacityUnits": 0.5,
            u'LastEvaluatedKey': {
                u'HashKeyElement': {u'N': u'123'},
                u'RangeKeyElement': {u'S': u'Waldo-4'},
            }
        }

        db = connect_boto_patch()

        ret = db.query(TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE}, limit=2, scan_index_forward=False)
        self.assertEqual(expected, ret)

    def test_query_all_filter_fields(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 5,
            u"Items": [
                {u"relevant_data": {u"S": "tata"}},
                {u"relevant_data": {u"S": "tete"}},
                {u"relevant_data": {u"S": "titi"}},
                {u"relevant_data": {u"S": "toto"}},
                {u"relevant_data": {u"S": "tutu"}},
            ],
            u"ConsumedCapacityUnits": 0.5,
        }
        fields = [u'relevant_data']

        db = connect_boto_patch()

        ret = db.query(TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE}, None, fields)
        self.assertEqual(expected, ret)

    # No need to test all conditions/type mismatch as they are unit tested
    def test_query_condition_filter_fields(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 3,
            u"Items": [
                {u"relevant_data": {u"S": u"titi"}},
                {u"relevant_data": {u"S": u"toto"}},
                {u"relevant_data": {u"S": u"tutu"}},
            ],
            u"ConsumedCapacityUnits": 0.5,
        }

        condition = {"AttributeValueList":[{"S":"Waldo-2"}],"ComparisonOperator":"GT"}
        fields = [u'relevant_data']

        db = connect_boto_patch()

        ret = db.query(TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE}, condition, fields)
        self.assertEqual(expected, ret)

    def test_query_all_consistent(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 5,
            u"Items": [ITEM1, ITEM2, ITEM3, ITEM4, ITEM5],
            u"ConsumedCapacityUnits": 1,
        }

        db = connect_boto_patch()

        ret = db.query(TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE}, consistent_read=True)
        self.assertEqual(expected, ret)

    def test_query_invalid_condition_multiple_data_in_field(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        condition = {
            "AttributeValueList":[
                {"S":"Waldo-2"},
                {"S":"Waldo-3"},
            ],
            "ComparisonOperator":"GT"
        }
        fields = [u'relevant_data']

        db = connect_boto_patch()

        self.assertRaises(DynamoDBValidationError,
                          db.query,
                          TABLE_NAME, {TABLE_HK_TYPE: HK_VALUE}, condition, fields)

