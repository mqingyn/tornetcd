#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by qingyun.meng on 16/1/19.
import time
from .. import Client, EtcdResult
from . import BaseTestCase


class TestWriteRead(BaseTestCase):
    def testWriteDefault(self):
        key = "/test0123_%s" % time.time()
        value = "tornado-etcd"
        fun = self.get_coroutine(self.client.write, key, value)
        result = self.ioloop.run_sync(fun)
        assert isinstance(result, EtcdResult)
        assert result.key == key
        assert result.value == value
        assert True

    def testReadDefault(self):
        key = "/test0123_%s" % time.time()
        value = "tornado-etcd"
        fun = self.get_coroutine(self.client.write, key, value)
        self.ioloop.run_sync(fun)
        fun = self.get_coroutine(self.client.read, key)
        result = self.ioloop.run_sync(fun)
        assert isinstance(result, EtcdResult)
        assert result.key == key
        assert result.value == value

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

    def get_etcdclient(self):
        return Client(host=self.hosts,
                      httpclient=self.http,
                      ioloop=self.ioloop)

    def testUpdate(self):
        key = "/test0123_%s" % time.time()
        value = "tornado-etcd"
        fun = self.get_coroutine(self.client.write, key, value)
        result = self.ioloop.run_sync(fun)
        assert isinstance(result, EtcdResult)
        value = "tornado-etcd1"
        result.value = value
        update = self.get_coroutine(self.client.update, result)
        update_result = self.ioloop.run_sync(update)
        assert isinstance(update_result, EtcdResult)
        assert update_result.value == value
