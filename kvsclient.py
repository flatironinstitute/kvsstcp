import os
import socket
import sys
import time
from cPickle import dumps as PDS, loads as PLS

from kvscommon import *

class KVSClient(object):
    '''KVS convenience wrapper that includes pickling by default.'''
    def __init__(self, host=None, port=None, retry=0):
        '''Establish connection to a key value storage server at an address
        given by host, port or "host:port"

        If retry > 0, retry any failed operation this many times.
        '''
        if not host:
            host = os.environ.get('KVSSTCP_HOST', None)
            # TODO: Silently overrides user supplied value, if there is one.
            port = os.environ.get('KVSSTCP_PORT', None)

        if not host: raise Exception('Missing host')

        if not port:
            if host.count(':') != 1: raise Exception('Missing port')
            host, port = host.split(':')

        self.addr = (host, int(port))
        self.retry = retry
        self.socket = None
        self.connect()

    def _connect(self):
        if self.socket: return
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            self.socket.connect(self.addr)
        except socket.error:
            self.socket = None
            raise

    def _retry(self, act, *args):
        retry = 0
        while 1:
            try:
                self._connect()
                return act(*args)
            except socket.error, msg:
                if retry >= self.retry: raise
                print >>sys.stderr, 'kvs socket error: %s, retrying' % msg
            try:
                self.socket.close()
            except socket.error:
                pass
            self.socket = None
            # exponential backoff
            time.sleep(2 ** retry)
            retry += 1

    def connect(self):
        # _retry calls _connect
        self._retry(lambda: None)

    def close(self):
        '''Close the connection to the KVS storage server. Does a socket shutdown as well.'''
        if not self.socket: return
        try:
            self.socket.sendall('clos')
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        except socket.error, e:
            # this is the client --- cannot assume logging is available.
            print >>sys.stderr, 'Ignoring exception during client close: "%s"'%e
        self.socket = None

    def _dump(self):
        self.socket.sendall('dump')
        return self._recvValue(True)

    def dump(self):
        return self._retry(self._dump)

    def _get(self, k, usePickle=True):
        return self._gv(k, 'get_', usePickle)

    def get(self, k, usePickle=True):
        return self._retry(self._get, k, usePickle)

    def _gv(self, k, op, usePickle):
        self.socket.sendall(op)
        self._sendLenAndBytes(k)
        coding = recvall(self.socket, 4)
        return self._recvValue(usePickle and coding == 'PYPK')

    def _monkey(self, k, v):
        self.socket.sendall('mkey')
        self._sendLenAndBytes(k)
        self._sendLenAndBytes(v)

    def monkey(self, k, v):
        return self._retry(self._monkey, k, v)

    def _put(self, k, v, usePickle=True):
        if usePickle:
            coding = 'PYPK'
        else:
            # TODO: Is this silent stringification two clever by half?
            # Maybe, since unicode strings will end up as "u'\\u...'". perhaps utf8-encode strings, and fail on other types?
            if type(v) != str: v = repr(v)
            coding = 'ASTR'

        self.socket.sendall('put_')
        self._sendLenAndBytes(k)
        self.socket.sendall(coding)
        self._sendLenAndBytes(v, usePickle)

    def put(self, k, v, usePickle=True):
        return self._retry(self._put, k, v, usePickle)

    def _recvValue(self, doPickle=False):
        l = int(recvall(self.socket, AsciiLenChars))
        payload = recvall(self.socket, l)
        if doPickle: payload = PLS(payload)
        return payload

    def _sendLenAndBytes(self, payload, doPickle=False):
        if doPickle: payload = PDS(payload)
        # if not doPickle, this seems very likely to be wrong for anything but bytearrays (encoding, etc.)
        self.socket.sendall(AsciiLenFormat%len(payload))
        self.socket.sendall(payload)

    def _shutdown(self):
        self.socket.sendall('down')

    def shutdown(self):
        '''Tell the KVS server to shutdown (and run the close() method for this client).'''
        try:
            self._retry(self.shutdown)
        finally:
            self.close()

    def _view(self, k, usePickle=True):
        return self._gv(k, 'view', usePickle)

    def view(self, k, usePickle=True):
        return self._retry(self._view, k, usePickle)
