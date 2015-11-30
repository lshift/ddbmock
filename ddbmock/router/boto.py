# -*- coding: utf-8 -*-

# This module is not imported unless connect_boto is called thus making 'boto'
# an optional dependancy

# boto entry point: semantic name
from __future__ import absolute_import

import json, time, boto

from ddbmock.router import router
from ddbmock.errors import ValidationException, ConditionalCheckFailedException, DDBError
from boto.exception import DynamoDBResponseError
from boto.dynamodb.exceptions import (DynamoDBValidationError as DDBValidationErr,
                                      DynamoDBConditionalCheckFailedError)
from ddbmock.config import config_for_user

# DDB to Boto exception
def _ddbmock_exception_to_boto_exception(err):
    if isinstance(err, ValidationException):
        return DDBValidationErr(err.status, err.status_str, err.to_dict())
    elif isinstance(err, ConditionalCheckFailedException):
        return DynamoDBConditionalCheckFailedError(err.status, err.status_str, err.to_dict())
    else:
        return DynamoDBResponseError(err.status, err.status_str, err.to_dict())


# Boto lib version entry point
def boto_router(self, action, body='', object_hook=None):
    """Replace boto.layer1.make_request to avoid the network layer. It also
    skips the authentication part. The external behavior is strictly compatible"""
    target = '%s_%s.%s' % (self.ServiceName, self.APIVersion, action)
    start = time.time()

    post = json.loads(body)

    try:
        ret = router(action, post, config_for_user(self.aws_access_key_id))
    except DDBError as e:
        raise _ddbmock_exception_to_boto_exception(e)
    finally:
        if post.has_key("request_id"): # doesn't if there's an error
            boto.log.debug('RequestId: %s', post['request_id'])
            elapsed = (time.time() - start) * 1000
            boto.perflog.info('dynamodb %s: id=%s time=%sms', target, post['request_id'], int(elapsed))

    # FIXME: dump followed by load... can be better...
    ret = json.dumps(ret)
    return json.loads(ret, object_hook=object_hook)
