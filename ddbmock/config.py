# -*- coding: utf-8 -*-

# Index overhead. This is added to each Item size when computing table size
INDEX_OVERHEAD = 100

# count: maximum number of tables
MAX_TABLES = 256

# bytes: max hash_key_size
MAX_HK_SIZE = 2048
# bytes: max range key size
MAX_RK_SIZE = 1024
# bytes: max item size, not including the index overhead
MAX_ITEM_SIZE = 64*1024

# value: minimum throughput value
MIN_TP = 1
# value: maximum throughput value
MAX_TP = 10000
# percent: min throughput change when increasing/decreasing
MIN_TP_CHANGE = 10
# days: min time between 2 throughtput decrease
MIN_TP_DEC_INTERVAL = 1
# percent: max throughput increase per single operation
MAX_TP_INC_CHANGE = 100
