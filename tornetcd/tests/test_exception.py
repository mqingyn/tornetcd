#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by qingyun.meng on 16/1/19.
import time
from .. import Client, EtcdResult
from .. import exceptions as etcd_exc
from . import BaseTestCase


class TestTimeout(BaseTestCase):
    def get_etcdclient(self):
        return Client(host=self.hosts,
                      httpclient=self.http,
                      read_timeout=0.0001,
                      ioloop=self.ioloop)

    def testAllTimeout(self):
        key = "/test5"
        fun = self.get_coroutine(self.client.get, key)
        try:
            result = self.ioloop.run_sync(fun)
        except Exception, ex:
            assert isinstance(ex, etcd_exc.EtcdConnectionFailed)


class TestAnyHostFaild(BaseTestCase):
    def get_hosts(self):
        # two valid,one fake
        return ['127.0.0.1:2370', '127.0.0.1:237', '127.0.0.1:212']

    def testTimeout(self):
        key = "/test5"
        fun = self.get_coroutine(self.client.get, key)
        for x in range(5):
            result = self.ioloop.run_sync(fun)
            assert isinstance(result, EtcdResult)
