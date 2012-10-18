# -*- coding: utf-8 -*-

from .key import Key, PrimaryKey
from .item import Item, ItemSize
from ddbmock import config
from collections import defaultdict, namedtuple
from threading import Timer
from ddbmock.errors import ValidationException, LimitExceededException, ResourceInUseException
import time, copy, datetime


def change_is_less_than_x_percent(current, candidate, threshold):
    """Return True iff 0% < change < 10%"""
    return current != candidate and (abs(current-candidate)/float(current))*100 < threshold

# items: array
# size: ItemSize
Results = namedtuple('Results', ['items', 'size', 'last_key', 'scanned'])

# All validations are performed on *incomming* data => already done :)

class Table(object):
    def __init__(self, name, rt, wt, hash_key, range_key, status='CREATING'):
        self.name = name
        self.rt = rt
        self.wt = wt
        self.hash_key = hash_key
        self.range_key = range_key
        self.status = status
        self.data = defaultdict(lambda: defaultdict(Item))
        self.creation_time = time.time()
        self.last_increase_time = 0
        self.last_decrease_time = 0
        self.count = 0

        Timer(config.DELAY_CREATING, self.activate).start()

    def delete(self, callback):
        """
        delete is really done when the timeout is exhausted, so we need a callback
        for this

        :ivar callback: real delete function
        """
        if self.status != "ACTIVE":
            raise ResourceInUseException("Table {} is in {} state. Can not UPDATE.".format(self.name, self.status))

        self.status = "DELETING"

        Timer(config.DELAY_DELETING, callback, [self.name]).start()

    def activate(self):
        self.status = "ACTIVE"

    def update_throughput(self, rt, wt):
        if self.status != "ACTIVE":
            raise ResourceInUseException("Table {} is in {} state. Can not UPDATE.".format(self.name, self.status))

        if change_is_less_than_x_percent(self.rt, rt, config.MIN_TP_CHANGE):
            raise LimitExceededException('Requested provisioned throughput change is not allowed. The ReadCapacityUnits change must be at least {} percent of current value. Current ReadCapacityUnits provisioned for the table: {}. Requested ReadCapacityUnits: {}.'.format(config.MIN_TP_CHANGE, self.rt, rt))
        if change_is_less_than_x_percent(self.wt, wt, config.MIN_TP_CHANGE):
            raise LimitExceededException('Requested provisioned throughput change is not allowed. The WriteCapacityUnits change must be at least {} percent of current value. Current WriteCapacityUnits provisioned for the table: {}. Requested WriteCapacityUnits: {}.'.format(config.MIN_TP_CHANGE, self.wt, wt))

        # is decrease ?
        if self.rt > rt or self.wt > wt:
            current_time = time.time()
            current_date = datetime.date.fromtimestamp(current_time)
            last_decrease = datetime.date.fromtimestamp(self.last_decrease_time)
            if (current_date - last_decrease).days < config.MIN_TP_DEC_INTERVAL:
                last = datetime.datetime.fromtimestamp(self.last_decrease_time)
                current = datetime.datetime.fromtimestamp(current_time)
                raise LimitExceededException("Subscriber limit exceeded: Provisioned throughput can be decreased only once within the {} day. Last decrease time: Tuesday, {}. Request time: {}".format(config.MIN_TP_DEC_INTERVAL, last, current))
            self.last_decrease_time = current_time

        # is increase ?
        if self.rt < rt or self.wt < wt:
            if (rt - self.rt)/float(self.rt)*100 > config.MAX_TP_INC_CHANGE:
                raise LimitExceededException('Requested provisioned throughput change is not allowed. The ReadCapacityUnits change must be at most {} percent of current value. Current ReadCapacityUnits provisioned for the table: {}. Requested ReadCapacityUnits: {}.'.format(config.MAX_TP_INC_CHANGE, self.rt, rt))
            if (wt - self.wt)/float(self.wt)*100 > config.MAX_TP_INC_CHANGE:
                raise LimitExceededException('Requested provisioned throughput change is not allowed. The WriteCapacityUnits change must be at most {} percent of current value. Current WriteCapacityUnits provisioned for the table: {}. Requested WriteCapacityUnits: {}.'.format(config.MAX_TP_INC_CHANGE, self.wt, wt))
            self.last_increase_time = time.time()

        # real work
        self.status = "UPDATING"

        self.rt = rt
        self.wt = wt

        Timer(config.DELAY_UPDATING, self.activate).start()

    def delete_item(self, key, expected):
        key = Item(key)
        hash_key = key.read_key(self.hash_key, u'HashKeyElement')
        range_key = key.read_key(self.range_key, u'RangeKeyElement')

        old = self.data[hash_key][range_key]
        old.assert_match_expected(expected)

        if self.range_key is None:
            del self.data[hash_key]
        else:
            del self.data[hash_key][range_key]

        if not old.is_new():
            # If this NOT new item, decrement counter
            self.count -= 1

        return old

    def update_item(self, key, actions, expected):
        key = Item(key)
        hash_key = key.read_key(self.hash_key, u'HashKeyElement', max_size=config.MAX_HK_SIZE)
        range_key = key.read_key(self.range_key, u'RangeKeyElement', max_size=config.MAX_RK_SIZE)

        # Need a deep copy as we will *modify* it
        old = copy.deepcopy(self.data[hash_key][range_key])
        old.assert_match_expected(expected)

        # Make sure we are not altering a key
        if self.hash_key.name in actions:
            raise ValidationException("UpdateItem can not alter the hash_key.")
        if self.range_key is not None and self.range_key.name in actions:
            raise ValidationException("UpdateItem can not alter the range_key.")

        self.data[hash_key][range_key].apply_actions(actions)
        new = copy.deepcopy(self.data[hash_key][range_key])

        size = self.data[hash_key][range_key].get_size()
        if size > config.MAX_ITEM_SIZE:
            self.data[hash_key][range_key] = old  # roll back
            raise ValueError("Items must be smaller than {} bytes. Got {} after applying update".format(config.MAX_ITEM_SIZE, size))


        # If new item:
        if old.is_new():
            # increment counter
            self.count += 1
            # append the keys, this is a new item
            self.data[hash_key][range_key][self.hash_key.name] = hash_key
            if self.range_key is not None:
                self.data[hash_key][range_key][self.range_key.name] = range_key

        return old, new

    def put(self, item, expected):
        item = Item(item)

        if item.get_size() > config.MAX_ITEM_SIZE:
            raise ValueError("Items must be smaller than {} bytes. Got {}".format(config.MAX_ITEM_SIZE, item.get_size()))

        hash_key = item.read_key(self.hash_key, max_size=config.MAX_HK_SIZE)
        range_key = item.read_key(self.range_key, max_size=config.MAX_RK_SIZE)

        old = self.data[hash_key][range_key]
        old.assert_match_expected(expected)

        self.data[hash_key][range_key] = item
        new = copy.deepcopy(self.data[hash_key][range_key])

        # If this a new item, increment counter
        if not old:
            self.count += 1

        return old, new

    def get(self, key, fields):
        key = Item(key)
        hash_key = key.read_key(self.hash_key, u'HashKeyElement')
        range_key = key.read_key(self.range_key, u'RangeKeyElement')

        if self.range_key is None and u'RangeKeyElement' in key:
            raise ValidationException("Table {} has no range_key".format(self.name))

        item = self.data[hash_key][range_key]

        if item.is_new():  # not found
            del self.data[hash_key][range_key]  # workaround defaultdict "feature" :)
            return None

        return item.filter(fields)

    def query(self, hash_key, rk_condition, fields, start, reverse, limit):
        """Scans all items at hash_key and return matches as well as last
        evaluated key if more than 1MB was scanned.

        :ivar hash_key: Element describing the hash_key, no type checkeing performed
        :ivar rk_condition: Condition which must be matched by the range_key. If None, all is returned.
        :ivar fields: return only these fields is applicable
        :ivar start: key structure. where to start iteration
        :ivar reverse: wether to scan the collection backward
        :ivar limit: max number of items to parse in this batch
        :return: Results(results, cumulated_size, last_key)
        """
        #FIXME: naive implementation
        #FIXME: what is an item disappears during the operation ?
        #TODO:
        # - esk
        # - size limit
        # - last evaluated key

        hk_name = self.hash_key.read(hash_key)
        rk_name = self.range_key.name
        size = ItemSize(0)
        good_item_count = 0
        results = []

        if reverse:
            keys = sorted(self.data[hk_name].keys()).reverse()
        else:
            keys = sorted(self.data[hk_name].keys())

        for key in keys:
            item = self.data[hk_name][key]

            if item.field_match(rk_name, rk_condition):
                good_item_count += 1
                size += item.get_size()
                results.append(item.filter(fields))

            if good_item_count == limit:
                break

        return Results(results, size, None, -1)

    def scan(self, scan_conditions, fields, start, limit):
        """Scans a whole table, no matter the structure, and return matches as
        well as the the last_evaluated key if applicable and the actually scanned
        item count.

        :ivar scan_conditions: Dict of key:conditions to match items against. If None, all is returned.
        :ivar fields: return only these fields is applicable
        :ivar start: key structure. where to start iteration
        :ivar limit: max number of items to parse in this batch
        :return: results, cumulated_scanned_size, last_key
        """
        #FIXME: naive implementation (too)
        #TODO:
        # - reverse
        # - esk
        # - limit
        # - size limit
        # - last evaluated key

        size = ItemSize(0)
        scanned = 0
        results = []

        for outer in self.data.values():
            for item in outer.values():
                size += item.get_size()
                scanned += 1
                if item.match(scan_conditions):
                    results.append(item.filter(fields))

        return Results(results, size, None, scanned)

    @classmethod
    def from_dict(cls, data):
        hash_key = PrimaryKey.from_dict(data[u'KeySchema'][u'HashKeyElement'])
        range_key = None
        if u'RangeKeyElement' in data[u'KeySchema']:
            range_key = PrimaryKey.from_dict(data[u'KeySchema'][u'RangeKeyElement'])

        return cls( data[u'TableName'],
                    data[u'ProvisionedThroughput'][u'ReadCapacityUnits'],
                    data[u'ProvisionedThroughput'][u'WriteCapacityUnits'],
                    hash_key,
                    range_key,
                  )

    def get_size(self):
        # TODO: update size only every 6 hours
        size = 0

        for outer in self.data.values():
            for item in outer.values():
                size += item.get_size().with_indexing_overhead()

        return size

    def to_dict(self, verbose=True):
        """Serialize table metadata for the describe table method. ItemCount and
        TableSizeBytes are accurate but highly depends on CPython > 2.6. Do not
        rely on it to project the actual size on a real DynamoDB implementation.
        """

        ret = {
            "CreationDateTime": self.creation_time,
            "KeySchema": {
                "HashKeyElement": self.hash_key.to_dict(),
            },
            "ProvisionedThroughput": {
                "ReadCapacityUnits": self.rt,
                "WriteCapacityUnits": self.wt,
            },
            "TableName": self.name,
            "TableStatus": self.status
        }

        if verbose:
            ret[u'ItemCount'] = self.count
            ret[u'TableSizeBytes'] = self.get_size()

        if self.last_increase_time:
            ret[u'ProvisionedThroughput'][u'LastIncreaseDateTime'] = self.last_increase_time
        if self.last_decrease_time:
            ret[u'ProvisionedThroughput'][u'LastDecreaseDateTime'] = self.last_decrease_time

        if self.range_key is not None:
            ret[u'KeySchema'][u'RangeKeyElement'] = self.range_key.to_dict()

        return ret
