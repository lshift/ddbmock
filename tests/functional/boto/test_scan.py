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

HK_VALUE1 = u'123'
HK_VALUE2 = u'456'
HK_VALUE3 = u'789'
RK_VALUE1 = u'Waldo-1'
RK_VALUE2 = u'Waldo-2'
RK_VALUE3 = u'Waldo-3'
RK_VALUE4 = u'Waldo-4'
RK_VALUE5 = u'Waldo-5'
RK_VALUE6 = u'Waldo-6'


ITEM1 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE1},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE1},
    u'relevant_data': {u'S': u'tata'},
}
ITEM2 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE1},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE2},
    u'relevant_data': {u'S': u'tete'},
}
ITEM3 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE2},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE3},
    u'relevant_data': {u'S': u'titi'},
}
ITEM4 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE3},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE4},
    u'relevant_data': {u'S': u'toto'},
}
ITEM5 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE3},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE5},
    u'relevant_data': {u'S': u'tutu'},
}
ITEM6 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE3},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE6},
    u'relevant_data': {u'S': u'tyty'},
    u'irelevant_data': {u'S': u'a'*(2048)},
}

# Please note that most query features are not yet implemented hence not tested
@unittest.skip("don't care right now")
class TestScan(unittest.TestCase):
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
        self.t1.put(ITEM6, {})

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        from ddbmock import clean_boto_patch
        dynamodb.hard_reset()
        clean_boto_patch()

    def test_scan_all(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 6,
            u"ScannedCount": 6,
            u"Items": [ITEM2, ITEM1, ITEM6, ITEM5, ITEM4, ITEM3],
            u"ConsumedCapacityUnits": 1.5,
        }

        db = connect_boto_patch()

        ret = db.scan(TABLE_NAME, None)
        self.assertEqual(expected, ret)

    def test_scan_paged(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        esk = {
            u'HashKeyElement': {u'N': u'789'},
            u'RangeKeyElement': {u'S': u'Waldo-5'},
        }

        expected1 = {
            u"Count": 4,
            u"ScannedCount": 4,
            u"Items": [ITEM2, ITEM1, ITEM6, ITEM5],
            u"ConsumedCapacityUnits": 1.5,
            u'LastEvaluatedKey': esk,
        }
        expected2 = {
            u"Count": 2,
            u"ScannedCount": 2,
            u"Items": [ITEM4, ITEM3],
            u"ConsumedCapacityUnits": 0.5,
        }

        db = connect_boto_patch()

        ret = db.scan(TABLE_NAME, limit=4)
        self.assertEqual(expected1, ret)
        ret = db.scan(TABLE_NAME, exclusive_start_key=esk)
        self.assertEqual(expected2, ret)

    def test_scan_all_filter_fields(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        self.maxDiff = None

        expected = {
            u"Count": 6,
            u"ScannedCount": 6,
            u"Items": [
                {u"relevant_data": {u"S": u"tete"}},
                {u"relevant_data": {u"S": u"tata"}},
                {u"relevant_data": {u"S": u"tyty"}},
                {u"relevant_data": {u"S": u"tutu"}},
                {u"relevant_data": {u"S": u"toto"}},
                {u"relevant_data": {u"S": u"titi"}},
            ],
            u"ConsumedCapacityUnits": 1.5,
        }
        fields = [u'relevant_data']

        db = connect_boto_patch()

        ret = db.scan(TABLE_NAME, None, fields)
        self.assertEqual(expected, ret)

    # No need to test all conditions/type mismatch as they are unit tested
    def test_scan_condition_filter_fields_in(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 3,
            u"ScannedCount": 6,
            u"Items": [
                {u"relevant_data": {u"S": u"tata"}},
                {u"relevant_data": {u"S": u"toto"}},
                {u"relevant_data": {u"S": u"titi"}},
            ],
            u"ConsumedCapacityUnits": 1.5,
        }

        conditions = {
            "relevant_data": {
                "AttributeValueList": [{"S":"toto"},{"S":"titi"},{"S":"tata"}],
                "ComparisonOperator": "IN",
            }
        }
        fields = [u'relevant_data']

        db = connect_boto_patch()

        ret = db.scan(TABLE_NAME, conditions, fields)
        self.assertEqual(expected, ret)

    def test_scan_condition_filter_fields_contains(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 1,
            u"ScannedCount": 6,
            u"Items": [
                {u"relevant_data": {u"S": u"toto"}},
            ],
            u"ConsumedCapacityUnits": 1.5,
        }

        conditions = {
            "relevant_data": {
                "AttributeValueList": [{"S":"to"}],
                "ComparisonOperator": "CONTAINS",
            },
        }
        fields = [u'relevant_data']

        db = connect_boto_patch()

        ret = db.scan(TABLE_NAME, conditions, fields)
        self.assertEqual(expected, ret)

    def test_scan_filter_ghost_fields(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb

        expected = {
            u"Count": 0,
            u"ScannedCount": 6,
            u"Items": [],
            u"ConsumedCapacityUnits": 1.5,
        }

        conditions = {
            "ghost field": {
                "AttributeValueList": [{"N":"123"}],
                "ComparisonOperator": "LT",
            },
        }
        fields = [u'relevant_data']

        db = connect_boto_patch()

        ret = db.scan(TABLE_NAME, conditions, fields)
        self.assertEqual(expected, ret)

    def test_scan_validation_error(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from boto.dynamodb.exceptions import DynamoDBValidationError

        expected = {
            u"Count": 1,
            u"ScannedCount": 6,
            u"Items": [
                {u"relevant_data": {u"S": u"toto"}},
            ],
            u"ConsumedCapacityUnits": 1.5,
        }

        conditions = {
            "relevant_data": {
                "AttributeValueList": [{"S":"to"},{"S":"ta"}],
                "ComparisonOperator": "CONTAINS",
            }
        }
        fields = [u'relevant_data']

        db = connect_boto_patch()

        self.assertRaises(DynamoDBValidationError, db.scan,
            TABLE_NAME, conditions, fields
        )
