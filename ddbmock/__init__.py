# -*- coding: utf-8 -*-

from pyramid.config import Configurator
from .router.pyramid import pyramid_router

# Pyramid entry point
def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    config = Configurator(settings=settings)

    # Insert router as '/' route. This is because all DDB URL are '/' (!)
    config.add_route('pyramid_router', '')
    config.add_view(pyramid_router, route_name='pyramid_router')

    return config.make_wsgi_app()

# Regular "over the network" connection wrapper.
def connect_boto_network(host='localhost', port=6543):
    """Connect to ddbmock launched in *server* mode via boto"""
    import boto
    from boto.regioninfo import RegionInfo
    endpoint = '{}:{}'.format(host,port)
    region = RegionInfo(name='ddbmock', endpoint=endpoint)
    return boto.connect_dynamodb(region=region, port=port, is_secure=False)

# Monkey patch magic, required for the Boto entry point
# Request hijacking Yeah !
real_boto = {}

def layer1_mock_init(self, access_key, *args, **kwargs):
    class provider(object):
        pass
    self.provider = provider()
    self.provider.access_key = access_key

def connect_boto_patch(aws_access_key_id=None):
    """Connect to ddbmock as a library via boto"""
    import boto

    from boto.dynamodb2.layer1 import DynamoDBConnection

    if real_boto:
        return DynamoDBConnection(aws_access_key_id)

    from router.boto import boto_router

    # Backup real functions for potential cleanup
    real_boto['DynamoDBConnection.make_request'] = DynamoDBConnection.make_request
    real_boto['DynamoDBConnection.__init__'] = DynamoDBConnection.__init__

    # Bypass network *and* authentication
    DynamoDBConnection.make_request = boto_router
    DynamoDBConnection.__init__ = layer1_mock_init

    # Just one more shortcut
    return DynamoDBConnection(aws_access_key_id)

def clean_boto_patch():
    """Restore real boto code"""
    if real_boto:
        from boto.dynamodb2.layer1 import DynamoDBConnection

        DynamoDBConnection.make_request = real_boto['DynamoDBConnection.make_request']
        DynamoDBConnection.__init__ = real_boto['DynamoDBConnection.__init__']

        real_boto.clear()
