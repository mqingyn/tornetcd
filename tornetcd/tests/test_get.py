#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by qingyun.meng on 16/1/19.
import time
from .. import Client, EtcdResult
from tornado import ioloop, httpclient, gen, options
from . import BaseTestCase


class TestMember(BaseTestCase):
    def testGetDefault(self):
        key = "/test0123_%s" % time.time()
        value = "tornado-etcd"
        fun = self.get_coroutine(self.client.write, key, value)
        self.ioloop.run_sync(fun)
        fun = self.get_coroutine(self.client.get, key)
        result = self.ioloop.run_sync(fun)
        assert isinstance(result, EtcdResult)
        assert result.key == key
        assert result.value == value
