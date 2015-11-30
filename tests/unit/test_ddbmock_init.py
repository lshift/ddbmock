# -*- coding: utf-8 -*-

import unittest
import socket

HOST = 'samaritain'
PORT = -12321
ENDPOINT = '{}:{}'.format(HOST, PORT)

@unittest.skip("don't care right now")
class TestDdbmockInit(unittest.TestCase):
    def tearDown(self):
        from ddbmock import clean_boto_patch
        clean_boto_patch()

    def test_connect_boto_patch_patch(self):
        from ddbmock import connect_boto_patch, real_boto, layer1_mock_init
        from ddbmock.router.boto import boto_router

        db = connect_boto_patch()

        self.assertEqual(boto_router, db.layer1.make_request.im_func)
        self.assertEqual(layer1_mock_init, db.layer1.__init__.im_func)
        self.assertNotEqual(real_boto['Layer1.make_request'], boto_router)
        self.assertNotEqual(real_boto['Layer1.__init__'], layer1_mock_init)

        # do it twice to make sure it is safe
        db = connect_boto_patch()

        self.assertEqual(boto_router, db.layer1.make_request.im_func)
        self.assertEqual(layer1_mock_init, db.layer1.__init__.im_func)
        self.assertNotEqual(real_boto['Layer1.make_request'], boto_router)
        self.assertNotEqual(real_boto['Layer1.__init__'], layer1_mock_init)

    def test_connect_boto_patch_network(self):
        try:
            socket.gethostbyname(HOST)
        except Exception:
            self.skipTest("Can't connect to test host: %s" % HOST)

        from ddbmock import connect_boto_network, clean_boto_patch

        clean_boto_patch()
        db = connect_boto_network(HOST, PORT)

        self.assertEqual('ddbmock', db.layer1.region.name)
        self.assertEqual(ENDPOINT, db.layer1.region.endpoint)
        self.assertEqual(ENDPOINT, db.layer1.host)
        self.assertEqual(PORT, db.layer1.port)
        self.assertFalse(db.layer1.is_secure)
