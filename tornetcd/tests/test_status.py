#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by qingyun.meng on 16/1/22.
import time
from .. import Client, EtcdResult, EtcdException
from tornado import ioloop, httpclient, gen, options
from . import BaseTestCase


class TestStatus(BaseTestCase):
    def testLeader(self):
        result = self.ioloop.run_sync(self.get_coroutine(self.client.leader))

        assert isinstance(result, dict)
        assert 'name' in result
        assert 'peerURIs' in result

    def testStats(self):
        result = self.ioloop.run_sync(self.get_coroutine(self.client.leader_stats))
        assert isinstance(result, dict)

    def testStoreStats(self):
        result = self.ioloop.run_sync(self.get_coroutine(self.client.store_stats))
        assert isinstance(result, dict)

