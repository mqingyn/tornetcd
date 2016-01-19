#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by qingyun.meng on 16/1/19.
import time
from .. import Client, EtcdResult, EtcdException
from tornado import ioloop, httpclient, gen, options
from . import BaseTestCase


class TestMember(BaseTestCase):
    def testGetMember(self):
        result = self.ioloop.run_sync(self.get_coroutine(self.client.get_members))
        assert isinstance(result, dict)
        assert len(result.items())

    def testSearchMachine(self):
        fun = self.get_coroutine(self.client.search_machine)
        result = self.ioloop.run_sync(fun)
        assert True


class TestMachineException(BaseTestCase):
    def testSearchMachineException(self):
        fun = self.get_coroutine(self.client.search_machine)
        try:
            result = self.ioloop.run_sync(fun)
        except Exception, ex:
            assert isinstance(ex, EtcdException)

    def get_hosts(self):
        return ['127.0.0.1:2333', '127.0.0.1:2379', '127.0.0.1:2379']

class TestMachineExceptionDown1(BaseTestCase):
    def get_hosts(self):
        return ['127.0.0.1:2333', '127.0.0.1:2370', '127.0.0.1:2379']


    def testSuccess(self):
        fun = self.get_coroutine(self.client.search_machine)
        try:
            result = self.ioloop.run_sync(fun)
        except Exception, ex:
            assert isinstance(ex, EtcdException)
        else:
            assert True