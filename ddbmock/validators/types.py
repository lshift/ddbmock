# -*- coding: utf-8 -*-

# Very bad import statement
from onctuous import *
from decimal import Decimal
from ddbmock import config
import re

# custom validator for integers
def Precision(min_exp=None, max_exp=None, max_digits=None, msg=None):
    def f(v):
        d = Decimal(v).as_tuple()
        if max_digits is not None and len(d.digits) > max_digits:
            raise Invalid(msg or ('{} has {} digits but maximum is {}'.format(v, len(d.digits), max_digits)))
        if min_exp is not None and min_exp > d.exponent:
            raise Invalid(msg or ('{} exponent is smaller than minimum {}'.format(v, min_exp)))
        if max_exp is not None and max_exp < d.exponent:
            raise Invalid(msg or ('{} exponent is bigger than maximum {}'.format(v, max_exp)))
        return v
    return f

# Elementary types

base_64 = re.compile(r'^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$')

table_name = All(
    unicode,
    Length(min=3, max=255, msg="Table name Length must be between 3 and 255"),
    Match(re.compile(r'^[a-zA-Z0-9\-_\.]*$'), msg="Table name may only contain alphanumeric chars and '-' '_' '.'"),
)

table_page = All(
    int,
    InRange(1, 100, msg="Table pages can only contain from 1 to 100 table"),
)

key_name = All(
    unicode,
    Length(min=1, max=255, msg="Key length must be between 1 and 255"),
)

primary_key_type = All(
    unicode,
    Any(u'S', u'N', u'B', msg="Primary key type may only be one of 'S', 'N' or 'B'"),
)

throughtput = All(
    int,
    InRange(config.MIN_TP, config.MAX_TP, msg="throughtput value must be between {} and {}".format(config.MIN_TP, config.MAX_TP)),
)

return_values = All(
    Any(u'NONE', u'ALL_OLD', msg="If specified, return value may only be one of 'NONE' or 'ALL_OLD'"),
)

return_values_all = All(
    Any(u'NONE', u'ALL_OLD', u'UPDATED_OLD', u'ALL_NEW', u'UPDATED_NEW', msg="If specified, return value may only be one of 'ALL_OLD', 'UPDATED_OLD', 'ALL_NEW' or 'UPDATED_NEW'"),
)

consistent_read = All(
    Boolean(msg="Consistent_read parameter must be a Boolean"),
)

limit = All(
    int,
    InRange(min=1, msg="Limit parameter must be a positive integer"),
)

scan_index_forward = All(
    Boolean(msg="ScanIndexForward must be either True either False"),
)

# DynamoDB data types

field_name = unicode

field_number_value = All(
    Precision(min_exp=-128, max_exp=126, max_digits=38),
)

field_string_value = All(unicode, Length(min=1, msg="String fields can not be empty"))

field_binary_value = All(
    unicode,
    Length(min=1, msg="Binary data fields can not be empty"),
    Match(base_64, msg="Binary data must be base64 encoded"),

)

field_number_set_value = All(Length(min=1, msg="A set can not be empty"), [field_number_value])
field_string_set_value = All(Length(min=1, msg="A set can not be empty"), [field_string_value])
field_binary_set_value = All(Length(min=1, msg="A set can not be empty"), [field_binary_value])

# complex types

provisioned_throughtput = {
    u'ReadCapacityUnits':  throughtput,
    u'WriteCapacityUnits': throughtput,
}

primary_key = {
    u'AttributeName': key_name,
    u'AttributeType': primary_key_type,
}

key_schema = {
    u'AttributeName': key_name,
    u'KeyType': Any(u'HASH', u'RANGE', msg="value may only be one of 'HASH' or 'RANGE'")
}

table_key_schema = All(Length(min=1, max=2), [key_schema])

# Fixme: max 1 item
simple_field_value = {
    Optional(u'N'): field_number_value,
    Optional(u'S'): field_string_value,
    Optional(u'B'): field_binary_value,
}

set_field_value = {
    Optional(u'NS'): field_number_set_value,
    Optional(u'SS'): field_string_set_value,
    Optional(u'BS'): field_binary_set_value,
}

key_field_value = simple_field_value
field_value = simple_field_value.copy()
field_value.update(set_field_value)

single_str_num_bin_list = All(Length(min=1, max=1), [simple_field_value])
double_str_num_bin_list = All(Length(min=2, max=2), [simple_field_value])
single_str_bin_list = All(Length(min=1, max=1), [{
    Optional(u'S'): field_string_value,
    Optional(u'B'): field_binary_value,
}])

item_schema = {
    Required(field_name): field_value,
}

get_key_schema = {
    u'HashKeyElement': key_field_value,
    Optional(u'RangeKeyElement'): key_field_value,
}

attributes_to_get_schema = All(
    [field_name],
)

expected_schema = {
    field_name: Any(
        {"Exists": IsFalse()},
        {"Exists": IsTrue(), "Value": field_value}, # pas sur de la vraie syntaxe :/
        {"Value": field_value},
    )
}

# Tss, y'a pas idee de faire des API aussi tordu...
# catches only half of the validity conditions :/
update_action_schema = Any(
    {"Value": field_value, Required("Action", "PUT"): Any("PUT", "ADD")},
    {"Value": set_field_value, "Action": "DELETE"},
    {"Action": "DELETE"},
)

attribute_update_schema = {
    field_name: update_action_schema
}

# Conditions supported by Query
range_key_condition = Any(
    {
        u"ComparisonOperator": Any(u"EQ", u"GT", u"GE", u"LT", u"LE"),
        u"AttributeValueList": single_str_num_bin_list,
    },{
        u"ComparisonOperator": u"BETWEEN",
        u"AttributeValueList": double_str_num_bin_list,
    },{
        u"ComparisonOperator": u"BEGINS_WITH",
        u"AttributeValueList": single_str_bin_list,
    },
)

# Conditions supported by Scan
scan_condition = Any(
    {
        u"ComparisonOperator": Any(u"EQ", u"NE", u"GT", u"GE", u"LT", u"LE"),
        u"AttributeValueList": single_str_num_bin_list,
    },{
        u"ComparisonOperator": u"BETWEEN",
        u"AttributeValueList": double_str_num_bin_list,
    },{
        u"ComparisonOperator": u"BEGINS_WITH",
        u"AttributeValueList": single_str_bin_list,
    },{
        u"ComparisonOperator": Any(u"NULL", u"NOT_NULL"),
    },{
        u"ComparisonOperator": Any(u"CONTAINS", u"NOT_CONTAINS"),
        u"AttributeValueList": single_str_num_bin_list,
    },{
        u"ComparisonOperator": u"IN",
        u"AttributeValueList": [simple_field_value],
    },
)

# Scan filter
scan_filter = {
    field_name: scan_condition,
}

# Permissions

READ_PERMISSION = "read"
WRITE_PERMISSION = "write"

def anything(x):
    return x
