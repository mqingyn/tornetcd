#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by qingyun.meng on 16/1/19.

import time
from tornetcd import Client, EtcdResult
from tornado import ioloop, httpclient, gen, options
from functools import partial


def setUp(self):
    self.ioloop = ioloop.IOLoop.instance()
    self.client = Client(host=['127.0.0.1:2370', '127.0.0.1:2371', '127.0.0.1:2372'],
                         httpclient=httpclient.AsyncHTTPClient(),
                         ioloop=self.ioloop)


def get_coroutine(func, *args, **kwargs):
    @gen.coroutine
    def run():
        res = yield func(*args, **kwargs)
        raise gen.Return(res)

    return run


def callback(response):
    print response.value
    print 'key change'


if __name__ == "__main__":
    ioloop = ioloop.IOLoop.instance()
    client = Client(host=['127.0.0.1:2370', '127.0.0.1:2371', '127.0.0.1:2372'], ioloop=ioloop,
                    httpclient=httpclient.AsyncHTTPClient())

    ioloop.add_callback(partial(client.eternal_watch, "/watch", callback=callback))
    ioloop.start()
