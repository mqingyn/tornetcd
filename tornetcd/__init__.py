#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by qingyun.meng on 16/1/19.
__author__ = 'mqingyn'
__version__ = '0.1.4'

version = tuple(map(int, __version__.split('.')))
from etcd_result import EtcdResult
from exceptions import *
