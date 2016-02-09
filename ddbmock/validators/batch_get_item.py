# -*- coding: utf-8 -*-

from .types import table_name, Required, item_schema, attributes_to_get_schema, consistent_read, consumed_capacity

post = {
    u"RequestItems": {
        table_name: {
            u"Keys": [item_schema],
            Required(u'AttributesToGet', []): attributes_to_get_schema,
            Required(u'ConsistentRead', False): consistent_read,
        },
    },
    Required(u'ReturnConsumedCapacity', u'NONE'): consumed_capacity,
}
