import json
import unittest

TABLE_NAME1 = 'Table-1'

TABLE_RT = 45
TABLE_WT = 123

TABLE_NAME = 'Table-HR'
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

HEADERS = {
    'x-amz-target': 'dynamodb_20111205.Query',
    'content-type': 'application/x-amz-json-1.0',
}


# Goal here is not to test the full API, this is done by the Boto tests
@unittest.skip("don't care right now")
class TestQuery(unittest.TestCase):
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

        self.t1.put(ITEM1, {})
        self.t1.put(ITEM2, {})
        self.t1.put(ITEM3, {})
        self.t1.put(ITEM4, {})
        self.t1.put(ITEM5, {})

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        dynamodb.hard_reset()

    def test_query_condition_filter_fields(self):
        request = {
            "TableName": TABLE_NAME,
            "HashKeyValue": {TABLE_HK_TYPE: HK_VALUE},
            "RangeKeyCondition": {
                "AttributeValueList": [{"S":"Waldo-2"}],
                "ComparisonOperator": "GT",
                },
            "AttributesToGet": [u'relevant_data'],
        }

        expected = {
            u"Count": 3,
            u"Items": [
                {u"relevant_data": {u"S": u"titi"}},
                {u"relevant_data": {u"S": u"toto"}},
                {u"relevant_data": {u"S": u"tutu"}},
            ],
            u"ConsumedCapacityUnits": 0.5,
        }

        # Protocol check
        res = self.app.post_json('/', request, headers=HEADERS, status=200)
        self.assertEqual(expected, json.loads(res.body))
        self.assertEqual('application/x-amz-json-1.0; charset=UTF-8',
                         res.headers['Content-Type'])

    def test_query_count_and_attrs_to_get_fails(self):
        request = {
            "TableName": TABLE_NAME,
            "HashKeyValue": {TABLE_HK_TYPE: HK_VALUE},
            "RangeKeyCondition": {
                "AttributeValueList": [{"S":"Waldo-2"}],
                "ComparisonOperator": "GT",
                },
            "AttributesToGet": [u'relevant_data'],
            "Count": True,
        }

        expected = {
            u'__type': u'com.amazonaws.dynamodb.v20120810#ValidationException',
            u'message': u'Can filter fields when only count is requested'
        }

        # Protocol check
        res = self.app.post_json('/', request, headers=HEADERS, status=400)
        self.assertEqual(expected, json.loads(res.body))
        self.assertEqual('application/x-amz-json-1.0; charset=UTF-8',
                         res.headers['Content-Type'])

    # Regression test for #9
    def test_query_all_404(self):
        request = {
            "TableName": TABLE_NAME,
            "HashKeyValue": {TABLE_HK_TYPE: HK_VALUE_404},
        }

        expected = {
            u"Count": 0,
            u'Items': [],
            u"ConsumedCapacityUnits": 0.5,
        }

        # Protocol check
        res = self.app.post_json('/', request, headers=HEADERS, status=200)
        self.assertEqual(expected, json.loads(res.body))
        self.assertEqual('application/x-amz-json-1.0; charset=UTF-8',
                         res.headers['Content-Type'])
