#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by qingyun.meng on 16/1/19.

"""
tornetcd
A python async-etcd client.
"""
import json
import random
from copy import copy
import socket

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
from functools import wraps
from tornado.log import gen_log as _log
from tornado.httpclient import HTTPRequest, HTTPError
from tornado.httputil import urlencode, url_concat
from tornado.concurrent import return_future, Future
from tornado import gen
from tornado.ioloop import PeriodicCallback
from etcd_result import EtcdResult
from . import exceptions as etcdexcept


class Client(object):
    """
    Client for etcd, the distributed log service using raft.
    """

    _MGET = 'GET'
    _MPUT = 'PUT'
    _MPOST = 'POST'
    _MDELETE = 'DELETE'
    _comparison_conditions = set(('prevValue', 'prevIndex', 'prevExist'))
    _read_options = set(('recursive', 'wait', 'waitIndex', 'sorted', 'quorum'))
    _del_conditions = set(('prevValue', 'prevIndex'))

    http = None

    def __init__(self, host='127.0.0.1:2730', version_prefix='/v2', read_timeout=60,
                 allow_reconnect=True, allow_redirect=True, protocol='http', cert_options=None,
                 username=None, password=None, use_proxies=False, expected_cluster_id=None, ioloop=None,
                 httpclient=None):
        if not ioloop:
            raise etcdexcept.EtcdException("ioloop is None.")

        if not httpclient:
            raise etcdexcept.EtcdException("httpclient is None.")
        self._protocol = protocol
        if not ioloop:
            from tornado.ioloop import IOLoop
            self.ioloop = IOLoop.current()
        else:
            self.ioloop = ioloop

        if not isinstance(host, (tuple, list)):
            hosts = [host]
        else:
            hosts = host

        self._machines_cache = [self._uri(self._protocol, conn) for conn in hosts]

        self.version_prefix = version_prefix
        self._allow_reconnect = allow_reconnect
        self._read_timeout = read_timeout
        self._allow_redirect = allow_redirect
        self._use_proxies = use_proxies

        self.cert_options = cert_options or {}

        self.username = None
        self.password = None

        if username and password:
            self.username = username
            self.password = password
        elif username:
            _log.warning('Username provided without password, both are required for authentication')
        elif password:
            _log.warning('Password provided without username, both are required for authentication')
        self.expected_cluster_id = expected_cluster_id
        self.http = httpclient

        self._machines_cache = list(set(self._machines_cache))

        _log.debug("Machines cache initialised to %s",
                   self._machines_cache)
        self._base_url = self._choice_machine()
        _log.debug("New etcd client created for %s", self.base_uri)
        if use_proxies:
            _log.debug("Use proxies at %s" % host)

        if not use_proxies:
            PeriodicCallback(self.search_machine, 1000 * 60).start()

    def _uri(self, protocol, host):
        return '%s://%s' % (protocol, host)

    @property
    def base_uri(self):
        """URI used by the client to connect to etcd."""
        return self._base_url

    @property
    def hosts(self):
        """Node to connect  etcd."""
        return urlparse(self.base_uri).netloc.split(':')[0]

    @property
    def port(self):
        """Port to connect etcd."""
        return int(urlparse(self.base_uri).netloc.split(':')[1])

    @property
    def protocol(self):
        """Protocol used to connect etcd."""
        return self._protocol

    @property
    def read_timeout(self):
        """Max seconds to wait for a read."""
        return self._read_timeout

    @property
    def allow_redirect(self):
        """Allow the client to connect to other nodes."""
        return self._allow_redirect

    @return_future
    def search_machine(self, callback=None):
        uri = self._base_url + self.version_prefix + '/machines'
        req = HTTPRequest(uri, self._MGET, request_timeout=self.read_timeout,
                          follow_redirects=self.allow_redirect, )
        response_future = self.http.fetch(req, callback=lambda result: result)

        def _callback(fut):
            exc = fut.exc_info()
            if exc:
                if not isinstance(exc[1], etcdexcept.EtcdException):
                    # We can't get the list of machines, if one server is in the
                    # machines cache, try on it
                    _log.error("Failed to get list of machines from %s%s: %r and retry it.",
                               uri, self.version_prefix, exc)
                    if self._machines_cache:
                        self._base_url = self._machines_cache.pop(0)
                        _log.debug("Retrying on %s", self._base_url)
                        # Call myself
                        self.ioloop.add_future(self.search_machine(), _callback)
                        return
                    else:
                        raise etcdexcept.EtcdException("Could not get the list of servers, "
                                                       "maybe you provided the wrong "
                                                       "host(s) to connect to?")
            else:
                response = fut.result()
                machines = [
                    node.strip() for node in
                    self._handle_server_response(response).body.decode('utf-8').split(',')
                    ]
                _log.debug("Retrieved list of machines: %s", machines)
                self._machines_cache = machines
                if self._base_url not in self._machines_cache:
                    self._base_url = self._choice_machine()
            callback(fut.result())

        self.ioloop.add_future(response_future, _callback)

    def _choice_machine(self):
        return random.choice(self._machines_cache)

    @return_future
    def get_members(self, callback=None):
        """
        A more structured view of peers in the cluster.

        Note that while we have an internal DS called _members, accessing the public property will call etcd.
        """

        # Empty the members list


        def cb(fut):
            response = fut.result()
            _members = {}
            data = response.body.decode('utf-8')
            res = json.loads(data)
            for member in res['members']:
                _members[member['id']] = member
            callback(_members)

        future = self.api_execute(self.version_prefix + '/members', self._MGET)
        self.ioloop.add_future(future, cb)

    @gen.coroutine
    def leader(self):
        """
        Returns:
            dict. the leader of the cluster.

        >>> print client.leader
        {"id":"ce2a822cea30bfca","name":"default","peerURLs":["http://localhost:2380","http://localhost:7001"],"clientURLs":["http://127.0.0.1:4001"]}
        """
        stats = yield self._stats()
        leader_info = yield self.get_members()
        raise gen.Return(leader_info[stats['leaderInfo']['leader']])

    def stats(self, callback=None):
        """
        Returns:
            dict. the stats of the local server
        """
        return self._stats(callback=callback)

    def leader_stats(self, callback=None):
        """
        Returns:
            dict. the stats of the leader
        """
        return self._stats('leader', callback)

    def store_stats(self, callback=None):
        """
        Returns:
           dict. the stats of the kv store
        """
        return self._stats('store', callback)

    @return_future
    def _stats(self, what='self', callback=None):
        """ Internal method to access the stats endpoints"""
        fut = self.api_execute(self.version_prefix
                               + '/stats/' + what, self._MGET)

        def _cb(fut):
            data = fut.result()
            try:
                data = data.body.decode('utf-8')
                callback(json.loads(data))
            except (TypeError, ValueError):
                raise etcdexcept.EtcdException("Cannot parse json data in the response")

        self.ioloop.add_future(fut, _cb)

    @property
    def key_endpoint(self):
        """
        REST key endpoint.
        """
        return self.version_prefix + '/keys'

    def _sanitize_key(self, key):
        if not key.startswith('/'):
            key = "/{}".format(key)
        return key

    @gen.coroutine
    def contains(self, key):
        """
        Check if a key is available in the cluster.
        >>> print 'key' in client
        True
        """
        try:
            yield self.get(key)
            raise gen.Return(True)
        except etcdexcept.EtcdKeyNotFound:
            raise gen.Return(False)

    @return_future
    def refresh_ttl(self, key, ttl=None, callback=None, **kwdargs):
        """
        refresh ttl for a key
        """
        _log.debug("Refresh key %s ttl=%s", key, ttl)
        key = self._sanitize_key(key)
        params = {"refresh": "true"}

        if ttl is not None:
            params['ttl'] = ttl

        for (k, v) in kwdargs.items():
            if k in self._comparison_conditions:
                if type(v) == bool:
                    params[k] = v and "true" or "false"
                else:
                    params[k] = v

        if '_endpoint' in kwdargs:
            path = kwdargs['_endpoint'] + key
        else:
            path = self.key_endpoint + key

        def cb(fut):
            callback(self._result_from_response(fut.result()))

        fut = self.api_execute(path, self._MPUT, params=params)
        self.ioloop.add_future(fut, cb)

    @return_future
    def write(self, key, value, ttl=None, dir=False, append=False, callback=None, **kwdargs):
        """
        Writes the value for a key, possibly doing atomit Compare-and-Swap

        Args:
            key (str):  Key.

            value (object):  value to set

            ttl (int):  Time in seconds of expiration (optional).

            dir (bool): Set to true if we are writing a directory; default is false.

            append (bool): If true, it will post to append the new value to the dir, creating a sequential key. Defaults to false.

            Other parameters modifying the write method are accepted:


            prevValue (str): compare key to this value, and swap only if corresponding (optional).

            prevIndex (int): modify key only if actual modifiedIndex matches the provided one (optional).

            prevExist (bool): If false, only create key; if true, only update key.

        Returns:
            client.EtcdResult

        >>> print client.write('/key', 'newValue', ttl=60, prevExist=False).value
        'newValue'

        """
        _log.debug("Writing %s to key %s ttl=%s dir=%s append=%s",
                   value, key, ttl, dir, append)
        key = self._sanitize_key(key)
        params = {}
        if value is not None:
            params['value'] = value

        if ttl is not None:
            params['ttl'] = ttl

        if dir:
            if value:
                raise etcdexcept.EtcdException(
                        'Cannot create a directory with a value')
            params['dir'] = "true"

        for (k, v) in kwdargs.items():
            if k in self._comparison_conditions:
                if type(v) == bool:
                    params[k] = v and "true" or "false"
                else:
                    params[k] = v

        method = append and self._MPOST or self._MPUT
        if '_endpoint' in kwdargs:
            path = kwdargs['_endpoint'] + key
        else:
            path = self.key_endpoint + key

        def cb(fut):
            callback(self._result_from_response(fut.result()))

        fut = self.api_execute(path, method, params=params)
        self.ioloop.add_future(fut, cb)

    def update(self, obj, callback=None):
        """
        Updates the value for a key atomically. Typical usage would be:

        c = etcd.Client()
        o = c.read("/somekey")
        o.value += 1
        c.update(o)

        Args:
            obj (etcd.EtcdResult):  The object that needs updating.

        """

        assert isinstance(obj, EtcdResult), "obj not a EtcdResult."

        _log.debug("Updating %s to %s.", obj.key, obj.value)
        kwdargs = {
            'dir': obj.dir,
            'ttl': obj.ttl,
            'prevExist': True
        }

        if not obj.dir:
            # prevIndex on a dir causes a 'not a file' error. d'oh!
            kwdargs['prevIndex'] = obj.modifiedIndex
        return self.write(obj.key, obj.value, callback=callback, **kwdargs)

    @return_future
    def read(self, key, callback=None, **kwargs):
        """
        Returns the value of the key 'key'.

        Args:
            key (str):  Key.

            Recognized kwd args

            recursive (bool): If you should fetch recursively a dir

            wait (bool): If we should wait and return next time the key is changed

            waitIndex (int): The index to fetch results from.

            sorted (bool): Sort the output keys (alphanumerically)

            timeout (int):  max seconds to wait for a read.

        Returns:
            client.EtcdResult (or an array of client.EtcdResult if a
            subtree is queried)

        Raises:
            KeyValue:  If the key doesn't exists.

            urllib3.exceptions.TimeoutError: If timeout is reached.

        >>> print client.get('/key').value
        'value'

        """
        _log.debug("Issuing read for key %s with args %s", key, kwargs)
        key = self._sanitize_key(key)

        params = {}
        for (k, v) in kwargs.items():
            if k in self._read_options:
                if type(v) == bool:
                    params[k] = v and "true" or "false"
                elif v is not None:
                    params[k] = v

        timeout = kwargs.get('timeout', None)

        def cb(fut):
            callback(self._result_from_response(fut.result()))

        fut = self.api_execute(self.key_endpoint + key, self._MGET, params=params, timeout=timeout)
        self.ioloop.add_future(fut, cb)

    @return_future
    def delete(self, key, recursive=None, dir=None, callback=None, **kwdargs):
        """
        Removed a key from etcd.

        Args:

            key (str):  Key.

            recursive (bool): if we want to recursively delete a directory, set
                              it to true

            dir (bool): if we want to delete a directory, set it to true

            prevValue (str): compare key to this value, and swap only if
                             corresponding (optional).

            prevIndex (int): modify key only if actual modifiedIndex matches the
                             provided one (optional).

        Returns:
            client.EtcdResult

        Raises:
            KeyValue:  If the key doesn't exists.

        >>> print client.delete('/key').key
        '/key'

        """
        _log.debug("Deleting %s recursive=%s dir=%s extra args=%s",
                   key, recursive, dir, kwdargs)
        key = self._sanitize_key(key)

        kwds = {}
        if recursive is not None:
            kwds['recursive'] = recursive and "true" or "false"
        if dir is not None:
            kwds['dir'] = dir and "true" or "false"

        for k in self._del_conditions:
            if k in kwdargs:
                kwds[k] = kwdargs[k]
        _log.debug("Calculated params = %s", kwds)

        def cb(fut):
            callback(self._result_from_response(fut.result()))

        fut = self.api_execute(self.key_endpoint + key, self._MDELETE, params=kwds)
        self.ioloop.add_future(fut, cb)

    def pop(self, key, recursive=None, dir=None, callback=None, **kwdargs):
        """
        Remove specified key from etcd and return the corresponding value.

        Args:

            key (str):  Key.

            recursive (bool): if we want to recursively delete a directory, set
                              it to true

            dir (bool): if we want to delete a directory, set it to true

            prevValue (str): compare key to this value, and swap only if
                             corresponding (optional).

            prevIndex (int): modify key only if actual modifiedIndex matches the
                             provided one (optional).

        Returns:
            client.EtcdResult

        Raises:
            KeyValue:  If the key doesn't exists.

        >>> print client.pop('/key').value
        'value'

        """

        return self.delete(key=key, recursive=recursive, dir=dir,
                           callback=lambda result: callback(result._prev_node),
                           **kwdargs)

    # Higher-level methods on top of the basic primitives
    def test_and_set(self, key, value, prev_value, ttl=None, callback=None):
        """
        Atomic test & set operation.
        It will check if the value of 'key' is 'prev_value',
        if the the check is correct will change the value for 'key' to 'value'
        if the the check is false an exception will be raised.

        Args:
            key (str):  Key.
            value (object):  value to set
            prev_value (object):  previous value.
            ttl (int):  Time in seconds of expiration (optional).

        Returns:
            client.EtcdResult

        Raises:
            ValueError: When the 'prev_value' is not the current value.

        >>> print client.test_and_set('/key', 'new', 'old', ttl=60).value
        'new'

        """
        return self.write(key, value, prevValue=prev_value, ttl=ttl, callback=callback)

    def set(self, key, value, ttl=None, callback=None):
        """
        Compatibility: sets the value of the key 'key' to the value 'value'

        Args:
            key (str):  Key.
            value (object):  value to set
            ttl (int):  Time in seconds of expiration (optional).

        Returns:
            client.EtcdResult

        Raises:
           etcd.EtcdException: when something weird goes wrong.

        """
        return self.write(key, value, ttl=ttl, callback=callback)

    def get(self, key, callback=None):
        """
        Returns the value of the key 'key'.

        Args:
            key (str):  Key.

        Returns:
            client.EtcdResult

        Raises:
            KeyError:  If the key doesn't exists.

        >>> print client.get('/key').value
        'value'

        """
        return self.read(key, callback)

    def watch(self, key, index=None, timeout=None, recursive=None, callback=None):
        # todo
        """
        Blocks until a new event has been received, starting at index 'index'

        Args:
            key (str):  Key.

            index (int): Index to start from.

            timeout (int):  max seconds to wait for a read.

        Returns:
            client.EtcdResult

        Raises:
            KeyValue:  If the key doesn't exists.

            urllib3.exceptions.TimeoutError: If timeout is reached.

        >>> print client.watch('/key').value
        'value'

        """
        _log.debug("About to wait on key %s, index %s", key, index)
        if index:
            return self.read(key, wait=True, waitIndex=index, timeout=timeout,
                             recursive=recursive, callback=callback)
        else:
            return self.read(key, wait=True, timeout=timeout,
                             recursive=recursive, callback=callback)

    def eternal_watch(self, key, index=None, recursive=None, callback=None, timeout=0):
        # todo
        """
        持续的watch一个key


        Args:
            key (str):  Key to subcribe to.
            index (int):  Index from where the changes will be received.

        Yields:
            client.EtcdResult

        >>> for event in client.eternal_watch('/subcription_key'):
        ...     print event.value
        ...
        value1
        value2


        """
        local_index = [index]

        def watch_again(_cb):
            fut = self.watch(key, index=local_index[0], timeout=timeout, recursive=recursive)
            self.ioloop.add_future(fut, _cb)
            return fut

        def _cb(future):
            exc = future.exception()

            if not exc:
                response = future.result()
                local_index[0] = response.modifiedIndex + 1
                callback(response)
                watch_again(_cb)
            else:
                _log.warning(exc)
                watch_again(_cb)

        return watch_again(_cb)

    def _result_from_response(self, response):
        """ 创建一个EtcdResult """
        raw_response = response.body
        try:
            res = json.loads(raw_response.decode('utf-8'))
        except (TypeError, ValueError, UnicodeError) as e:
            raise etcdexcept.EtcdException(
                    'Server response was not valid JSON: %r' % e)
        try:
            r = EtcdResult(**res)
            if response.code == 201:
                r.newKey = True
            r.parse_headers(response)
            return r
        except Exception as e:
            raise etcdexcept.EtcdException(
                    'Unable to decode server response: %r' % e)

    def _request_wrapper(func):
        @wraps(func)
        def wrapper(self, path, method, params=None, timeout=None):

            if timeout is None:
                timeout = self.read_timeout

            if not path.startswith('/'):
                raise ValueError('Path does not start with /')
            fut = func(self, path, method, params=params, timeout=timeout)
            if self._allow_reconnect:
                fut._retry = copy(self._machines_cache)
                fut._retry.remove(self._base_url)
            new_future = Future()

            def _callback(fut):
                exc_obj = fut.exception()
                if exc_obj:
                    if isinstance(exc_obj, HTTPError) and exc_obj.code < 500:
                        try:
                            self._handle_server_response(exc_obj.response)
                        except Exception, ex:
                            new_future.set_exception(ex)
                        return
                    elif (isinstance(exc_obj, HTTPError) and exc_obj.code > 500) or isinstance(exc_obj, socket.error):
                        _log.error("Request to server %s failed: %r",
                                   self._base_url, exc_obj)
                        if isinstance(params, dict) and params.get("wait") == "true":
                            _log.debug("Watch timed out.")
                            exc = etcdexcept.EtcdWatchTimedOut(
                                    "Watch timed out: %r" % exc_obj,
                                    cause=exc_obj
                            )
                            new_future.set_exception(exc)
                            return
                        elif self._allow_reconnect and fut._retry:
                            _log.info("Reconnection allowed, looking for another "
                                      "server.")
                            self._base_url = fut._retry.pop(0)
                            fut_reconnect = func(self, path, method, params=params, timeout=timeout)
                            fut_reconnect._retry = fut._retry
                            self.ioloop.add_future(fut_reconnect, _callback)
                            return

                        else:
                            _log.debug("Reconnection disabled, giving up.")
                            exc = etcdexcept.EtcdConnectionFailed(
                                    "Connection to etcd failed due to %r" % exc_obj,
                                    cause=exc_obj
                            )
                            new_future.set_exception(exc)

                    else:
                        _log.exception("Unexpected request failure.")
                        exc = etcdexcept.EtcdException(exc_obj.message)
                        new_future.set_exception(exc)

                result = fut.result()
                self._check_cluster_id(result)
                result = self._handle_server_response(result)
                new_future.set_result(result)

            self.ioloop.add_future(fut, _callback)
            return new_future

        return wrapper

    @_request_wrapper
    def api_execute(self, path, method, params=None, timeout=None):
        """ Executes the query. """
        url = self._base_url + path

        validate_cert = True if self.cert_options else False
        if (method == self._MGET) or (method == self._MDELETE):
            if params:
                url = url_concat(url, params)
            body = None

        elif (method == self._MPUT) or (method == self._MPOST):
            body = urlencode(params)

        else:
            raise etcdexcept.EtcdException(
                    'HTTP method {} not supported'.format(method))
        request = HTTPRequest(url, method=method,
                              request_timeout=timeout,
                              headers=self._get_default_headers(method),
                              follow_redirects=self.allow_redirect,
                              body=body,
                              validate_cert=validate_cert,
                              ca_certs=self.cert_options.get('ca_certs', None),
                              client_key=self.cert_options.get('client_key', None),
                              client_cert=self.cert_options.get('client_cert', None),
                              auth_username=self.username,
                              auth_password=self.password)
        _log.debug("Request %s %s %s" % (path, method, request.body))
        return self.http.fetch(request)

    @_request_wrapper
    def api_execute_json(self, path, method, params=None, timeout=None):
        url = self._base_url + path
        json_payload = json.dumps(params)
        headers = self._get_default_headers(method)
        headers['Content-Type'] = 'application/json'
        validate_cert = True if self.cert_options else False
        request = HTTPRequest(url, method=method,
                              request_timeout=timeout,
                              headers=headers,
                              body=json_payload,
                              follow_redirects=self.allow_redirect,
                              validate_cert=validate_cert,
                              ca_certs=self.cert_options.get('ca_certs', None),
                              client_key=self.cert_options.get('client_key', None),
                              client_cert=self.cert_options.get('client_cert', None),
                              auth_username=self.username,
                              auth_password=self.password)

        return self.http.fetch(request)

    def _handle_server_response(self, response):
        """ Handles the server response """
        if response.code in [200, 201]:
            return response

        else:
            _log.debug("Response %s", response.body)
            resp = response.body.decode('utf-8')

            # throw the appropriate exception
            try:
                r = json.loads(resp)
                r['status'] = response.code
            except (TypeError, ValueError):
                # Bad JSON, make a response locally.
                r = {"message": "Bad response",
                     "cause": str(resp)}
            etcdexcept.EtcdError.handle(r)

    def _get_default_headers(self, method):
        headers = {}
        if method == self._MPOST or method == self._MPUT:
            headers.update({"Content-Type": "application/x-www-form-urlencoded"})
        return headers

    def _check_cluster_id(self, response):
        cluster_id = response.headers.get("x-etcd-cluster-id")
        if not cluster_id:
            _log.warning("etcd response did not contain a cluster ID")
            return
        id_changed = (self.expected_cluster_id and
                      cluster_id != self.expected_cluster_id)
        # Update the ID so we only raise the exception once.
        old_expected_cluster_id = self.expected_cluster_id
        self.expected_cluster_id = cluster_id
        if id_changed:
            # Defensive: clear the pool so that we connect afresh next
            # time.
            self._base_url = self._choice_machine()
            _log.error(
                    'The UUID of the cluster changed from {} to '
                    '{}.'.format(old_expected_cluster_id, cluster_id))
