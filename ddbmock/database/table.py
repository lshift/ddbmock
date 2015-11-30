# -*- coding: utf-8 -*-

from .key import Key, PrimaryKey
from .item import Item, ItemSize
from .storage import Store
from collections import defaultdict, namedtuple
from threading import Lock
from ddbmock import config
from ddbmock.errors import ValidationException, LimitExceededException, ResourceInUseException
from ddbmock.utils import schedule_action
import time, copy, datetime

# All validations are performed on *incomming* data => already done :)

# items: array
# size: ItemSize
Results = namedtuple('Results', ['items', 'size', 'last_key', 'scanned'])

class Table(object):
    """
    Table abstraction. Actual :py:class:`ddbmock.database.item.Item` are stored
    in :py:attr:`store`.
    """
    def __init__(self, name, rt, wt, hash_key, range_key, status='CREATING'):
        """
        Create a new ``Table``. When manually creating a table, make sure you
        registered it in :py:class:`ddbmock.database.db.DynamoDB` with a something
        like ``dynamodb.data[name] = Table(name, "...")``.

        Even though there are :py:const:`DELAY_CREATING` seconds before the status
        is updated to ``ACTIVE``, the table is immediately available. This is a
        slight difference with real DynamooDB to ease unit and functionnal tests.

        :param name: Valid table name. No further checks are performed.
        :param rt: Provisioned read throughput.
        :param wt: Provisioned write throughput.
        :param hash_key: :py:class:`ddbmock.database.key.Key` instance describe the ``hash_key``
        :param hash_key: :py:class:`ddbmock.database.key.Key` instance describe the ``range_key`` or ``None`` if table has no ``range_key``
        :param status: (optional) Valid initial table status. If Table needd to be avaible immediately, use ``ACTIVE``, otherwise, leave default value.

        .. note:: ``rt`` and ``wt`` are only used by ``DescribeTable`` and ``UpdateTable``. No throttling is nor will ever be done.
        """
        self.name = name
        self.rt = rt
        self.wt = wt
        self.hash_key = hash_key
        self.range_key = range_key
        self.status = status

        self.store = Store(name)
        self.write_lock = Lock()

        self.creation_time = time.time()
        self.last_increase_time = 0
        self.last_decrease_time = 0
        self.count = 0

        schedule_action(config.DELAY_CREATING, self.activate)

    def truncate(self):
        """
        Remove all Items from this table. This is like a reset. Might be very
        usefull in unit and functional tests.
        """
        self.store.truncate()

    def delete(self):
        """
        If the table was ``ACTIVE``, update its state to ``DELETING``. This is
        not a destructor, only a sate updater and the Table instance will still
        be valid afterward. In all othercases, raise :py:exc:`ddbmock.errors.ResourceInUseException.`

        If you want to perform the full table delete cycle, please use
        :py:meth:`ddbmock.database.db.DynamoDB.delete_table()` instead

        :raises: :py:exc:`ddbmock.errors.ResourceInUseException` is the table was not in ``Active`` state
        """
        if self.status != "ACTIVE":
            raise ResourceInUseException("Table {} is in {} state. Can not DELETE.".format(self.name, self.status))

        self.status = "DELETING"

    def activate(self):
        """
        Unconditionnaly set Table status to ``ACTIVE``. This method is automatically
        called by the constructor once :py:const:`DELAY_CREATING` is over.
        """
        self.status = "ACTIVE"

    def update_throughput(self, rt, wt):
        """
        Update table throughput. Same conditions and limitations as real DynamoDB
        applies:

        - No more that 1 decrease operation per UTC day.
        - No more than doubling throughput at once.
        - Table must be in ``ACTIVE`` state.

        Table status is then set to ``UPDATING`` until :py:const:`DELAY_UPDATING`
        delay is over. Like real DynamoDB, the Table can still be used during
        this period

        :param rt: New read throughput
        :param wt: New write throughput

        :raises: :py:exc:`ddbmock.errors.ResourceInUseException` if table was not in ``ACTIVE`` state
        :raises: :py:exc:`ddbmock.errors.LimitExceededException` if the other above conditions are not met.

        """
        if self.status != "ACTIVE":
            raise ResourceInUseException("Table {} is in {} state. Can not UPDATE.".format(self.name, self.status))

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

        schedule_action(config.DELAY_UPDATING, self.activate)

    def delete_item(self, key, expected):
        """
        Delete item at ``key`` from the databse provided that it matches ``expected``
        values.

        This operation is atomic and blocks all other pending write operations.

        :param key: Raw DynamoDB request hash and range key dict.
        :param expected: Raw DynamoDB request conditions.

        :return: deepcopy of :py:class:`ddbmock.database.item.Item` as it was before deletion.

        :raises: :py:exc:`ddbmock.errors.ConditionalCheckFailedException` if conditions are not met.
        """
        key = Item(key)
        hash_key = key.read_key(self.hash_key, u'HashKeyElement')
        range_key = key.read_key(self.range_key, u'RangeKeyElement')

        with self.write_lock:
            try:
                old = copy.deepcopy(self.store[hash_key, range_key])
            except KeyError:
                return Item()

            old.assert_match_expected(expected)
            del self.store[hash_key, range_key]

        self.count -= 1
        return old

    def update_item(self, key, actions, expected):
        """
        Apply ``actions`` to item at ``key`` provided that it matches ``expected``.

        This operation is atomic and blocks all other pending write operations.

        :param key: Raw DynamoDB request hash and range key dict.
        :param actions: Raw DynamoDB request actions.
        :param expected: Raw DynamoDB request conditions.

        :return: both deepcopies of :py:class:`ddbmock.database.item.Item` as it was (before, after) the update.

        :raises: :py:exc:`ddbmock.errors.ConditionalCheckFailedException` if conditions are not met.
        :raises: :py:exc:`ddbmock.errors.ValidationException` if ``actions`` attempted to modify the key or the resulting Item is biggere than :py:const:`config.MAX_ITEM_SIZE`
        """
        key = Item(key)
        hash_key = key.read_key(self.hash_key, u'HashKeyElement', max_size=config.MAX_HK_SIZE)
        range_key = key.read_key(self.range_key, u'RangeKeyElement', max_size=config.MAX_RK_SIZE)

        with self.write_lock:
            # Need a deep copy as we will *modify* it
            try:
                old = self.store[hash_key, range_key]
                new = copy.deepcopy(old)
                old.assert_match_expected(expected)
            except KeyError:
                # Item was not in the DB yet
                old = Item()
                new = Item()
                self.count += 1
                # append the keys
                new[self.hash_key.name] = key['HashKeyElement']
                if self.range_key is not None:
                    new[self.range_key.name] = key['RangeKeyElement']


            # Make sure we are not altering a key
            if self.hash_key.name in actions:
                raise ValidationException("UpdateItem can not alter the hash_key.")
            if self.range_key is not None and self.range_key.name in actions:
                raise ValidationException("UpdateItem can not alter the range_key.")

            new.apply_actions(actions)
            self.store[hash_key, range_key] = new

            size = new.get_size()
            if size > config.MAX_ITEM_SIZE:
                self.store[hash_key, range_key] = old  # roll back
                raise ValidationException(
                    "Item size has exceeded the maximum allowed size of {}".format(config.MAX_ITEM_SIZE))

        return old, new

    def put(self, item, expected):
        """
        Save ``item`` in the database provided that ``expected`` matches. Even
        though DynamoDB ``UpdateItem`` operation only supports returning ``ALL_OLD``
        or ``NONE``, this method returns both ``old`` and ``new`` values as the
        throughput, computed in the view, takes the maximum of both size into
        account.

        This operation is atomic and blocks all other pending write operations.

        :param item: Raw DynamoDB request item.
        :param expected: Raw DynamoDB request conditions.

        :return: both deepcopies of :py:class:`ddbmock.database.item.Item` as it was (before, after) the update or empty item if not found.

        :raises: :py:exc:`ddbmock.errors.ConditionalCheckFailedException` if conditions are not met.
        """
        item = Item(item)

        if item.get_size() > config.MAX_ITEM_SIZE:
            raise ValidationException(
                "Item size has exceeded the maximum allowed size of {}".format(config.MAX_ITEM_SIZE))

        hash_key = item.read_key(self.hash_key, max_size=config.MAX_HK_SIZE)
        range_key = item.read_key(self.range_key, max_size=config.MAX_RK_SIZE)

        with self.write_lock:
            try:
                old = self.store[hash_key, range_key]
                old.assert_match_expected(expected)
            except KeyError:
                # Item was not in the DB yet
                self.count += 1
                old = Item()

            self.store[hash_key, range_key] = item
            new = copy.deepcopy(item)

        return old, new

    def get(self, key, fields):
        """
        Get ``fields`` from :py:class:`ddbmock.database.item.Item` at ``key``.

        :param key: Raw DynamoDB request key.
        :param fields: Raw DynamoDB request array of field names to return. Empty to return all.

        :return: reference to :py:class:`ddbmock.database.item.Item` at ``key`` or ``None`` when not found

        :raises: :py:exc:`ddbmock.errors.ValidationException` if a ``range_key`` was provided while table has none.
        """
        key = Item(key)
        hash_key = key.read_key(self.hash_key, u'HashKeyElement')
        range_key = key.read_key(self.range_key, u'RangeKeyElement')

        if self.range_key is None and u'RangeKeyElement' in key:
            raise ValidationException("Table {} has no range_key".format(self.name))

        try:
            item = self.store[hash_key, range_key]
            return item.filter(fields)
        except KeyError:
            # Item was not in the DB yet
            return None

    def query(self, hash_key, rk_condition, fields, start, reverse, limit):
        """
        Return ``fields`` of all items with provided ``hash_key`` whose ``range_key``
        matches ``rk_condition``.

        :param hash_key: Raw DynamoDB request hash_key.
        :param rk_condition: Raw DynamoDB request ``range_key`` condition.
        :param fields: Raw DynamoDB request array of field names to return. Empty to return all.
        :param start: Raw DynamoDB request key of the first item to scan. Empty array to indicate first item.
        :param reverse: Set to ``True`` to parse the range keys backward.
        :param limit: Maximum number of items to return in this batch. Set to 0 or less for no maximum.

        :return: Results(results, cumulated_size, last_key)

        :raises: :py:exc:`ddbmock.errors.ValidationException` if ``start['HashKeyElement']`` is not ``hash_key``
        """
        #FIXME: naive implementation
        #FIXME: what if an item disappears during the operation ?
        #TODO:
        # - size limit

        hash_value = self.hash_key.read(hash_key)
        rk_name = self.range_key.name
        size = ItemSize(0)
        good_item_count = 0
        results = []
        lek = None

        if start and start['HashKeyElement'] != hash_key:
            raise ValidationException("'HashKeyElement' element of 'ExclusiveStartKey' must be the same as the hash_key. Expected {}, got {}".format(hash_key, start['HashKeyElement']))

        try:
            data = self.store[hash_value, None]
        except KeyError:
            # fix #9: return empty result set if first key does not exist
            return Results(results, size, lek, -1)

        keys = sorted(data.keys())

        if reverse:
            keys.reverse()

        if start:
            first_key = self.range_key.read(start['RangeKeyElement'])
            index = keys.index(first_key) + 1  # May raise ValueError but that's OK
            keys = keys[index:]

        for key in keys:
            item = data[key]

            if item.field_match(rk_name, rk_condition):
                good_item_count += 1
                size += item.get_size()
                results.append(item.filter(fields))

            if good_item_count == limit:
                lek = {
                    u'HashKeyElement': hash_key,
                    u'RangeKeyElement': item[rk_name],
                }
                break

        return Results(results, size, lek, -1)

    def scan(self, scan_conditions, fields, start, limit):
        """
        Return ``fields`` of all items matching ``scan_conditions``. No matter the
        ``start`` key, ``scan`` allways starts from teh beginning so that it might
        be quite slow.

        :param scan_conditions: Raw DynamoDB request conditions.
        :param fields: Raw DynamoDB request array of field names to return. Empty to return all.
        :param start: Raw DynamoDB request key of the first item to scan. Empty array to indicate first item.
        :param limit: Maximum number of items to return in this batch. Set to 0 or less for no maximum.

        :return: Results(results, cumulated_size, last_key, scanned_count)
        """
        #FIXME: naive implementation (too)
        #TODO:
        # - esk
        # - size limit

        size = ItemSize(0)
        scanned = 0
        lek = {}
        results = []
        skip = start is not None
        hk_name = self.hash_key.name
        rk_name = self.range_key.name if self.range_key else None

        for item in self.store:
            # first, skip all items before start
            if skip:
                if start['HashKeyElement'] != item[hk_name]:
                    continue
                if rk_name and start['RangeKeyElement'] != item[rk_name]:
                    continue
                skip = False
                continue

            # match filters ?
            if item.match(scan_conditions):
                results.append(item.filter(fields))

            # update stats
            size += item.get_size()
            scanned += 1

            # quit ?
            if scanned == limit:
                lek[u'HashKeyElement'] = item[hk_name]
                if rk_name:
                    lek[u'RangeKeyElement'] = item[rk_name]
                break

        return Results(results, size, lek, scanned)

    @classmethod
    def from_dict(cls, data):
        """
        Alternate constructor which deciphers raw DynamoDB request data before
        ultimately calling regular ``__init__`` method.

        See :py:meth:`__init__` for more insight.

        :param data: raw DynamoDB request data.

        :return: fully initialized :py:class:`Table` instance
        """

        key_schema = data[u'KeySchema']
        assert len(key_schema) <= 2
        hash_key = PrimaryKey.from_dict(key_schema[0])
        assert hash_key.typename == "HASH"
        if len(key_schema) == 2:
            range_key = Key.from_dict(key_schema[1])
            assert range_key.typename == "RANGE"
        else:
            range_key = None

        return cls( data[u'TableName'],
                    data[u'ProvisionedThroughput'][u'ReadCapacityUnits'],
                    data[u'ProvisionedThroughput'][u'WriteCapacityUnits'],
                    hash_key,
                    range_key,
                  )

    def get_size(self):
        """
        Compute the whole table size using the same rules as the real DynamoDB.
        Actual memory usage in ddbmock will be much higher due to dict and Python
        overheadd.

        .. note:: Real DynamoDB updates this result every 6 hours or so while this is an "on demand" call.

        :return: cumulated size of all items following DynamoDB size computation.
        """
        # TODO: update size only every 6 hours
        size = 0

        for item in self.store:
            size += item.get_size().with_indexing_overhead()

        return size

    def to_dict(self, verbose=True):
        """
        Serialize this table to DynamoDB compatible format. Every fields are
        realistic, including the ``TableSizeBytes`` which relies on
        :py:meth:`get_size.`

        Some DynamoDB requests only send a minimal version of Table metadata. to
        reproduce this behavior, just set ``verbose`` to ``False``.

        :param verbose: Set to ``False`` to skip table size computation.

        :return: Serialized version of table metadata compatible with DynamoDB API syntax.
        """

        ret = {
            "CreationDateTime": self.creation_time,
            "KeySchema": [self.hash_key.to_dict()],
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
            ret[u'KeySchema'].append(self.range_key.to_dict())

        return ret

    # these 2 functions helps to pickle table schema (If store supports it)
    # please note that only the schema is persisted, not the state
    def __getstate__(self):
        return (
            self.name,
            self.rt,
            self.wt,
            self.hash_key,
            self.range_key,
            'ACTIVE',
        )

    def __setstate__(self, state):
        self.__init__(*state)  # quick and dirty (tm), but it does the job
