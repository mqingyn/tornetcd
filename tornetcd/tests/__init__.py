#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by qingyun.meng on 16/1/19.
from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from tornado import gen
from .. import Client


class BaseTestCase(object):
    def setUp(self):
        self.ioloop = self.get_ioloop()
        self.http = self.get_httpclient()
        self.hosts = self.get_hosts()
        self.client = self.get_etcdclient()



    def get_ioloop(self):
        return IOLoop.instance()

    def get_httpclient(self):
        return AsyncHTTPClient()

    def get_etcdclient(self):
        return Client(host=self.hosts,
                      httpclient=self.http,
                      ioloop=self.ioloop)

    def get_hosts(self):
        return ['127.0.0.1:2370', '127.0.0.1:2371', '127.0.0.1:2372']

    def get_coroutine(self, func, *args, **kwargs):
        @gen.coroutine
        def run():
            res = yield func(*args, **kwargs)
            raise gen.Return(res)

        return run
