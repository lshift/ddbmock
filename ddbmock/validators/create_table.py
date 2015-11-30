# -*- coding: utf-8 -*-

from .types import table_name, table_key_schema, provisioned_throughtput, WRITE_PERMISSION, anything

post = {
    u'TableName': table_name,
    u'KeySchema': table_key_schema,
    u'ProvisionedThroughput': provisioned_throughtput,
    u'AttributeDefinitions': anything, #TODO
}

permissions = WRITE_PERMISSION
