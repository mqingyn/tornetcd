#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by qingyun.meng on 16/1/19.

import time
from .. import Client, EtcdResult
from tornado import ioloop, httpclient, gen, options
from . import BaseTestCase
from .. import exceptions as etcdexc


class TestDelete(BaseTestCase):
    def testPopDefault(self):
        key = "/test0123_%s" % time.time()
        value = "tornado-etcd"
        fun = self.get_coroutine(self.client.write, key, value)
        result = self.ioloop.run_sync(fun)
        fun = self.get_coroutine(self.client.pop, key)
        result = self.ioloop.run_sync(fun)

        assert isinstance(result, EtcdResult)
        assert result.action == 'delete'
        assert result.value == None

    def testNonKeyDefault(self):
        key = "/abc%s" % time.time()
        fun = self.get_coroutine(self.client.pop, key)
        try:
            result = self.ioloop.run_sync(fun)
        except Exception, ex:

            assert isinstance(ex, etcdexc.EtcdKeyNotFound)

