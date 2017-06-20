#!/usr/bin/env python
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
        self.waiting = None
        self.connect()

    def _check_wait(self, wait=None):
        if self.waiting and self.waiting != wait:
            # We could implement an explicit "cancel wait" call with a response to let this be canceled explicitly
            raise Exception("Previous %s timed out: you must retreive the previously requested %s value first." % self.waiting)

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
            self._close()
            # exponential backoff
            time.sleep(2 ** retry)
            retry += 1

    def connect(self):
        '''Reconnect, if necessary.  Can be used after an explicit close.'''
        # _retry calls _connect
        self._retry(lambda: None)

    def _close(self):
        if not self.socket: return
        try:
            self.socket.close()
        except socket.error:
            pass
        self.socket = None
        self.waiting = None

    def close(self):
        '''Close the connection to the KVS storage server. Does a socket shutdown as well.'''
        self._check_wait()
        if not self.socket: return
        try:
            self.socket.sendall('clos')
            self.socket.shutdown(socket.SHUT_RDWR)
        except socket.error, e:
            # this is the client --- cannot assume logging is available.
            print >>sys.stderr, 'Ignoring exception during client close: "%s"'%e
        self._close()

    def _dump(self):
        self.socket.sendall('dump')
        return self._recvValue(True)

    def dump(self):
        self._check_wait()
        return self._retry(self._dump)

    def get(self, k, usePickle=True, timeout=None):
        '''Retrieve and remove a value from the store.
        
        If usePickle is True, and the value was pickled, then the value will be
        unpickled before being returned.  If timeout is not None, this will
        only wait for timeout seconds before throwing a socket.timeout error.
        In this case, you MUST call this function again in the future until it
        returns a value before doing any other operation, otherwise the value
        may be lost.
        '''
        return self._rgv('get_', k, usePickle, timeout)

    def _rgv(self, op, k, usePickle, timeout):
        self._check_wait((op, k))
        return self._retry(self._gv, op, k, usePickle, timeout)

    def _gv(self, op, k, usePickle, timeout):
        if not self.waiting:
            self.socket.sendall(op)
            self._sendLenAndBytes(k)
            self.waiting = (op, k)
        self.socket.settimeout(timeout)
        try:
            c = self.socket.recv(1)
        finally:
            self.socket.settimeout(None)
        if not c:
            raise socket.error("Connection closed")
        coding = c + recvall(self.socket, 3)
        return self._recvValue(usePickle and coding == 'PYPK')

    def _monkey(self, k, v):
        self.socket.sendall('mkey')
        self._sendLenAndBytes(k)
        self._sendLenAndBytes(v)

    def monkey(self, k, v):
        self._check_wait()
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
        self._check_wait()
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
            self._retry(self._shutdown)
        finally:
            self.close()

    def view(self, k, usePickle=True, timeout=None):
        '''Retrieve, but do not remove, a value from the store.  See 'get'.'''
        return self._rgv('view', k, usePickle, timeout)

def addKVSServerArgument(argp, name = 'kvsserver'):
    '''Add an argument to the given ArgumentParser that accepts the address of a running KVSServer, defaulting to $KVSSTCP_HOST:$KVSSTCP_PORT.'''
    host = os.environ.get('KVSSTCP_HOST')
    port = os.environ.get('KVSSTCP_PORT') if host else None
    argp.add_argument(name, metavar='host:port', nargs='?' if port else None, default=host+':'+port if port else None, help='KVS server address.')

if '__main__' == __name__:
    import argparse

    class OpAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            items = getattr(namespace, 'ops', [])
            op = self.option_strings[1][2:]
            if op in ['get', 'view', 'put']:
                pickle = getattr(namespace, 'pickle', False)
                values.append(pickle)
                if pickle and op == 'put':
                    values[1] = eval(values[1], {})
                if op in ['get', 'view']:
                    values.append(getattr(namespace, 'timeout', None))
            values.insert(0, op)
            items.append(values)
            namespace.ops = items

    argp = argparse.ArgumentParser(description='Command-line client to key-value storage server.')
    argp.add_argument('-r', '--retry', default=0, type=int, help='Number of times to retry on failure')
    argp.add_argument('-P', '--pickle', action='store_true', help='(Un-)Pickle values')
    argp.add_argument('-R', '--no-pickle', dest='pickle', action='store_false', help="Don't (un-)pickle values")
    argp.add_argument('-t', '--timeout', type=float, metavar='SECS', help='Timeout waiting for get/view')
    argp.add_argument('-d', '--dump', action=OpAction, nargs=0, help='Dump the current state')
    argp.add_argument('-g', '--get', action=OpAction, nargs=1, metavar='KEY', help='Retrieve and remove a value')
    argp.add_argument('-v', '--view', action=OpAction, nargs=1, metavar='KEY', help='Retrieve a value')
    argp.add_argument('-p', '--put', action=OpAction, nargs=2, metavar=('KEY','VALUE'), help='Put a value (if pickling, evaluate as a python expression)')
    argp.add_argument('-m', '--monkey', action=OpAction, nargs=2, metavar=('MKEY','KEY:EVENTS'), help='Create a monitor key in the KVS')
    argp.add_argument('-S', '--shutdown', action=OpAction, nargs=0, help='Tell the KVS to shutdown')
    argp.add_argument('-s', '--sleep', action=OpAction, nargs=1, type=float, metavar='SECS', help='Pause for a time')
    addKVSServerArgument(argp, 'server')
    args = argp.parse_args()

    kvs = KVSClient(args.server, retry = args.retry)

    for cmd in args.ops:
        op = cmd.pop(0)
        if op == 'sleep':
            time.sleep(*cmd)
        else:
            try:
                r = getattr(kvs, op)(*cmd)
            except Exception, e:
                r = e
            if r is not None:
                print(r)
