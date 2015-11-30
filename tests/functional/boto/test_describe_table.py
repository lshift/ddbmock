# -*- coding: utf-8 -*-

import unittest
import boto
unittest.TestCase.maxDiff = None

TABLE_NAME = 'Table-1'
TABLE_NAME_404= 'Waldo'
TABLE_RT = 45
TABLE_WT = 123
TABLE_HK_NAME = u'hash_key'
TABLE_HK_TYPE = u'N'
TABLE_RK_NAME = u'range_key'
TABLE_RK_TYPE = u'S'

HK_VALUE = u'123'
RK_VALUE = u'Decode this data if you are a coder'
RK_VALUE2 = u'Decode this data if you are a coder (or not)'


ITEM1 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE},
    u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
}
ITEM2 = {
    TABLE_HK_NAME: {TABLE_HK_TYPE: HK_VALUE},
    TABLE_RK_NAME: {TABLE_RK_TYPE: RK_VALUE2},
    u'relevant_data': {u'B': u'THVkaWEgaXMgdGhlIGJlc3QgY29tcGFueSBldmVyIQ=='},
}

@unittest.skip("don't care right now")
class TestDescribeTables(unittest.TestCase):
    def setUp(self):
        from ddbmock.database.db import dynamodb
        from ddbmock.database.table import Table
        from ddbmock.database.key import PrimaryKey

        dynamodb.hard_reset()

        hash_key = PrimaryKey(TABLE_HK_NAME, TABLE_HK_TYPE)
        range_key = PrimaryKey(TABLE_RK_NAME, TABLE_RK_TYPE)

        t1 = Table(TABLE_NAME, TABLE_RT, TABLE_WT, hash_key, range_key, status='ACTIVE')

        t1.put(ITEM1, {})
        t1.put(ITEM2, {})

        dynamodb.data[TABLE_NAME] = t1

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        from ddbmock import clean_boto_patch
        dynamodb.hard_reset()
        clean_boto_patch()

    def test_describe_table(self):
        from ddbmock import connect_boto_patch
        db = connect_boto_patch()

        table = db.get_table(TABLE_NAME)

        self.assertEqual(TABLE_NAME, table.name)
        self.assertEqual(421, table.size_bytes)
        self.assertEqual(TABLE_RT, table.read_units)
        self.assertEqual(TABLE_WT, table.write_units)
        self.assertEqual(u'ACTIVE', table.status)
        self.assertEqual(TABLE_HK_NAME, table.schema.dict[0]["AttributeName"])
        self.assertEqual(TABLE_HK_TYPE, table.schema.dict[0]["KeyType"])
        self.assertEqual(TABLE_RK_NAME, table.schema.dict[1]["AttributeName"])
        self.assertEqual(TABLE_RK_TYPE, table.schema.dict[1]["KeyType"])

    def describe_table_404(self):
        from ddbmock import connect_boto_patch
        db = connect_boto_patch()

        self.assertRaises(DDBValidationErr, db.get_table,
            TABLE_NAME_404,
        )
