# -*- coding: utf-8 -*-

from . import load_table
from ddbmock.utils import push_read_throughput

@load_table
def get_item(post, table):
    base_capacity = 1 if post[u'ConsistentRead'] else 0.5
    attributes_to_get = post.get(u'AttributesToGet', [])
    assert len(attributes_to_get) == 0
    item = table.get(post[u'Key'], attributes_to_get)

    if item is not None:
        capacity = base_capacity*item.get_size().as_units()
        push_read_throughput(table.name, capacity)
        return {
            "ConsumedCapacityUnits": capacity,
            "Item": item,
        }
    else:
        push_read_throughput(table.name, base_capacity)
        return {
            "ConsumedCapacityUnits": base_capacity,
        }
