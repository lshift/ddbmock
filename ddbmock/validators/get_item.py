# -*- coding: utf-8 -*-

from .types import (table_name, Required, item_schema, get_key_schema,
    consistent_read, attributes_to_get_schema, anything, Optional, Any)

post = {
    u'TableName': table_name,
    u'Key': get_key_schema,
    Required(u'ConsistentRead', False): consistent_read,
    #Optional(u'ExpressionAttributeNames'): anything,  # TODO
    #Optional(u'ProjectionExpression'): anything, # TODO
    Optional(u'ReturnConsumedCapacity'): Any(u'INDEXES', u'TOTAL', u'NONE'),
}
