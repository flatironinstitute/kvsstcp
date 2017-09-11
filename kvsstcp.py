#!/usr/bin/env python2
from collections import defaultdict as DD
try:
    from cPickle import dumps as PDS
except ImportError:
    from pickle import dumps as PDS
from functools import partial
import errno
import gc
import logging
import os
import resource
import select
import socket
import sys
from threading import Thread

from kvscommon import *

logger = logging.getLogger('kvs')

# There are some cyclic references in in asyncio, handlers, waiters, etc., so I'm re-enabling this:
#gc.disable()

_DISCONNECTED = frozenset((errno.ECONNRESET, errno.ENOTCONN, errno.ESHUTDOWN, errno.ECONNABORTED, errno.EPIPE, errno.EBADF))
_BUFSIZ = 8192

class HandlerThread(Thread):
    '''Based on asyncore, but with a simpler, stricter per-thread interface that allows better performance.'''
    def __init__(self):
        super(HandlerThread, self).__init__(name='HandlerThread')
        self.epoll = select.epoll()
        self.disps = dict()
        self.daemon = True
        self.current = None
        self.start()

    def register(self, dispatcher):
        self.disps[dispatcher.fd] = dispatcher
        self.epoll.register(dispatcher.fd, dispatcher.mask)

    def unregister(self, dispatcher):
        self.epoll.unregister(dispatcher.fd)
        del self.disps[dispatcher.fd]

    def modify(self, dispatcher):
        self.epoll.modify(dispatcher.fd, dispatcher.mask)

    def poll(self):
        ev = self.epoll.poll()
        for (f, e) in ev:
            d = self.current = self.disps[f]
            oldm = d.mask
            if e & select.EPOLLHUP:
                d.handle_close()
                continue
            if e & select.EPOLLIN:
                d.handle_read()
                if d.mask & select.EPOLLHUP: continue
            if d.mask & select.EPOLLOUT:
                d.handle_write()
                if d.mask & select.EPOLLHUP: continue
            self.current = None
            m = d.mask
            if m != oldm:
                self.epoll.modify(f, m)

    def run(self):
        while True:
            try:
                self.poll()
            except IOError as e:
                if e.errno == errno.EINTR:
                    continue
                raise

    def close(self):
        self.epoll.close()

class Dispatcher(object):
    '''Based on asyncore.dispatcher_with_send, works with EventHandler.
    Also allows input of known-size blocks.'''
    def __init__(self, sock, handler):
        self.out_buf = []
        self.in_buf = b''
        self.read_size = 0
        self.read_handler = None
        self.sock = sock
        self.fd = sock.fileno()
        self.mask = 0
        sock.setblocking(0)
        self.handler = handler

    def open(self):
        self.handler.register(self)

    def _send(self, data):
        try:
            return self.sock.send(data)
        except socket.error as e:
            if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                return 0
            if e.errno in _DISCONNECTED:
                self.handle_close()
                return 0
            raise

    def _recv(self, siz):
        try:
            data = self.sock.recv(siz)
            if not data:
                self.handle_close()
            return data
        except socket.error as e:
            if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                return b''
            if e.errno in _DISCONNECTED:
                self.handle_close()
                return b''
            raise

    def close(self):
        self.mask |= select.EPOLLHUP
        self.handler.unregister(self)

    def handle_close(self):
        self.close()

    def write(self, *data):
        self.out_buf.extend(data)
        if not self.mask & select.EPOLLOUT:
            self.mask |= select.EPOLLOUT
            # write can be called from other threads
            if self.handler.current is not self:
                self.handler.modify(self)

    def handle_write(self):
        while self.out_buf:
            buf = self.out_buf[0]
            r = self._send(buf)
            if r < len(buf):
                if r: self.out_buf[0] = buf[r:]
                return
            self.out_buf.pop(0)
        self.mask &= ~select.EPOLLOUT

    def got_read(self, z):
        i = self.in_buf[:z]
        handler = self.read_handler
        self.in_buf = self.in_buf[z:]
        self.read_handler = None
        self.mask &= ~select.EPOLLIN
        handler(i)

    def next_read(self, size, f):
        self.read_size = size
        self.read_handler = f
        self.mask |= select.EPOLLIN
        if size <= len(self.in_buf):
            self.got_read(size)

    def handle_read(self):
        z = self.read_size
        n = len(self.in_buf)
        if n < z:
            self.in_buf += self._recv(max(_BUFSIZ, z-n))
            n = len(self.in_buf)
        if n >= z:
            self.got_read(z)

class KVSRequestHandler(Dispatcher):
    def __init__(self, pair, server, handler):
        sock, self.addr = pair
        self.server = server
        # Keep track of any currently waiting get:
        self.waiter = None
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        super(KVSRequestHandler, self).__init__(sock, handler)
        logger.info('Accepted connect from %s', repr(self.addr))
        self.next_op()
        self.open()

    def handle_close(self):
        self.cancel_waiter()
        logger.info('Closing connection from %s', repr(self.addr))
        self.close()

    def cancel_waiter(self):
        if self.waiter:
            self.server.kvs.cancel_wait(self.waiter)
            self.waiter = None

    def next_op(self):
        self.next_read(4, self.handle_op)

    def next_lendata(self, handler):
        # wait for variable-length data prefixed by AsciiLenFormat
        def handle_len(l):
            n = int(l)
            if n < 0: raise Exception("invalid data len: '%s'" % l)
            self.next_read(n, handler)
        self.next_read(AsciiLenChars, handle_len)

    def handle_op(self, op):
        if b'clos' == op:
            self.sock.shutdown(socket.SHUT_RDWR)
        elif b'down' == op:
            logger.info('Calling server shutdown')
            self.server.shutdown()
        elif b'dump' == op:
            d = self.server.kvs.dump()
            self.write(AsciiLenFormat(len(d)), d)
            self.next_op()
        elif op in [b'get_', b'mkey', b'put_', b'view']:
            self.next_lendata(partial(self.handle_opkey, op))
        else:
            raise Exception("Unknown op from %s: '%s'", repr(self.addr), op)

    def handle_opkey(self, op, key):
        #DEBUGOFF            logger.debug('(%s) %s key "%s"', whoAmI, reqtxt, key)
        if b'mkey' == op:
            self.next_lendata(partial(self.handle_mkey, key))
        elif b'put_' == op:
            self.next_read(4, lambda encoding:
                self.next_lendata(partial(self.handle_put, key, encoding)))
        else: # 'get_' or 'view'
            # Cancel waiting for any previous get/view operation (since client wouldn't be able to distinguish the async response)
            self.cancel_waiter()
            self.waiter = KVSWaiter(op, key, self.handle_got)
            self.server.kvs.wait(self.waiter)
            # But keep listening for another op (like 'clos') to cancel this one
            self.next_op()

    def handle_mkey(self, key, val):
        #DEBUGOFF                logger.debug('(%s) val: %s', whoAmI, repr(val))
        self.server.kvs.monkey(key, val)
        self.next_op()

    def handle_put(self, key, encoding, val):
        # TODO: bytearray val?
        #DEBUGOFF                logger.debug('(%s) val: %s', whoAmI, repr(val))
        self.server.kvs.put(key, (encoding, val))
        self.next_op()

    def handle_got(self, encval):
        (encoding, val) = encval
        self.write(encoding, AsciiLenFormat(len(val)), val)
        self.waiter = None

class KVSWaiter:
    def __init__(self, op, key, handler):
        if op == b'get_': op = b'get'
        self.op = op
        self.delete = op == b'get'
        self.key = key
        self.handler = handler

class KVS(object):
    '''Get/Put/View implements a client-server key value store. If no
    value is associated with a given key, clients will block on get or
    view until a value is available. Multiple values may be associated
    with any given key.

    This is, by design, a very simple, lightweight service that only
    depends on standard Python modules.

    '''
 
    def __init__(self, getIndex=0, viewIndex=-1):
        self.getIndex, self.viewIndex = getIndex, viewIndex #TODO: Add sanity checks?
        self.key2mon = DD(lambda:DD(set)) # Maps a normal key to keys that monitor it.
        self.monkeys = set()              # List of monitor keys.
        # store and waiters are mutually exclusive, and could be kept in the same place
        self.store = DD(list)
        self.waiters = DD(list)
        self.opCounts = {b'get': 0, b'put': 0, b'view': 0, b'wait': 0}
        self.ac, self.rc = 0, 0

    def _doMonkeys(self, op, k):
        # Don't monitor operations on monitor keys.
        if k in self.monkeys: return
        #DEBUGOFF        logger.debug('doMonkeys: %s %s %s', op, k, repr(self.key2mon[True][op] | self.key2mon[k][op]))
        for p in (True, k):
            for mk in self.key2mon[p][op]:
                self.put(mk, (b'ASTR', repr((op, k))))
        
    def dump(self):
        '''Utility function that returns a snapshot of the KV store.'''
        def vrep(v):
            # Omit or truncate some values, in which cases add the original length as a third value
            if v[0] == b'JSON' or v[0] == b'HTML': return v
            if v[0] != b'ASTR': return (v[0], None, len(v[1]))
            if v[1][:6].lower() == '<html>': return v # for backwards compatibility only
            if len(v[1]) > 50: return (v[0], v[1][:24] + '...' + v[1][-23:], len(v[1]))
            return v

        return PDS(([self.opCounts[b'get'], self.opCounts[b'put'], self.opCounts[b'view'], self.opCounts[b'wait'], self.ac, self.rc], [(k, len(v)) for k, v in self.waiters.iteritems() if v], [[k, len(vv), vrep(vv[-1])] for k, vv in self.store.iteritems() if vv]))

    def wait(self, waiter):
        '''Atomically (remove and) return a value associated with key k. If
        none, block.'''
        #DEBUGOFF        logger.debug('wait: %s, %s', repr(waiter.key), repr(waiter.op))
        self._doMonkeys(waiter.op, waiter.key)
        vv = self.store.get(waiter.key)
        if vv:
            if waiter.delete:
                v = vv.pop(self.getIndex)
                if not vv: self.store.pop(waiter.key)
            else:
                v = vv[self.viewIndex]
            self.opCounts[waiter.op] += 1
            #DEBUGOFF                logger.debug('_gv (%s): %s => %s (%d)', waiter.op, waiter.key, repr(v[0]), len(v[1]))
            waiter.handler(v)
        else:
            self.waiters[waiter.key].append(waiter)
            self.opCounts[b'wait'] += 1
            self._doMonkeys(b'wait', waiter.key)
            #DEBUGOFF                logger.debug('(%s) %s acquiring', repr(waiter), repr(s))
            self.ac += 1

    def cancel_wait(self, waiter):
        ww = self.waiters.get(waiter.key)
        if ww:
            try:
                ww.remove(waiter)
            except ValueError:
                pass
            if not ww: self.waiters.pop(waiter.key)

    def monkey(self, mkey, v):
        '''Make Mkey a monitor key. Value encodes what events to monitor and
        for which key:

                Key:Events

        Whenever a listed event occurs for "Key", a put will be done
        to "Mkey" with the value "<event> <key>".  If 'Key' is empty,
        the events listed will be monitored for all keys.  'Events' is
        some subset of 'g', 'p', 'v' and 'w' (get, put, view and
        wait). Monitoring of any event *not* listed is turned off for
        the specified key.

        '''
        #DEBUGOFF        logger.debug('monkey: %s %s', mkey, v)
        if b':' not in v: return #TODO: Add some sort of error handling?
        self.monkeys.add(mkey)
        k, events = v.rsplit(b':', 1)
        if not k: k = True
        for e, op  in [(b'g', b'get'), (b'p', b'put'), (b'v', b'view'), (b'w', b'wait')]:
            if e in events:
                self.key2mon[k][op].add(mkey)
            else:
                try: self.key2mon[k][op].remove(mkey)
                except KeyError: pass
        #DEBUGOFF        logger.debug('monkey: %s', repr(self.key2mon))

    def put(self, k, v):
        '''Add value v to those associated with the key k.'''
        #DEBUGOFF        logger.debug('put: %s, %s', repr(k), repr(v))
        self.opCounts[b'put'] += 1
        ww = self.waiters.get(k) # No waiters is probably most common, so optimize for
                                 # that. ww will be None if no waiters have been
                                 # registered for key k.
        consumed = False
        if ww:
            while ww:
                waiter = ww.pop(0)
                #DEBUGOFF                    logger.debug('%s releasing', repr(waiter))
                self.rc += 1
                self.opCounts[waiter.op] += 1
                waiter.handler(v)
                if waiter.delete:
                    consumed = True
                    break
            if not ww: self.waiters.pop(k)

        if not consumed: self.store[k].append(v)
        self._doMonkeys(b'put', k)

class KVSServer(Thread):
    def __init__(self, host=None, port=0):
        super(KVSServer, self).__init__(name='KVSServerThread')
        if not host: host = socket.gethostname()

        self.kvs = KVS()

        snof, hnof = resource.getrlimit(resource.RLIMIT_NOFILE)
        if snof < hnof:
            logger.info('Raising max open files from %d to %d', snof, hnof)
            resource.setrlimit(resource.RLIMIT_NOFILE, (hnof, hnof))

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setblocking(1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        logger.info('Setting queue size to 4000')
        self.sock.listen(4000)
        self.cinfo = self.sock.getsockname()

        self.handler = HandlerThread()

        self.start()

    def shutdown(self):
        if self.sock:
            self.sock.shutdown(socket.SHUT_RDWR)

    def run(self):
        while True:
            try:
                pair = self.sock.accept()
            except socket.error as e:
                if e.errno in _DISCONNECTED or e.errno == errno.EINVAL:
                    break
                raise
            KVSRequestHandler(pair, self, self.handler)
        logger.info('Server shutting down')
        self.sock.close()
        self.sock = None
        self.handler.close()

    def env(self, env = os.environ.copy()):
        '''Add the KVSSTCP environment variables to the given environment.'''
        env['KVSSTCP_HOST'] = self.cinfo[0]
        env['KVSSTCP_PORT'] = str(self.cinfo[1])
        return env

if '__main__' == __name__:
    import argparse
    argp = argparse.ArgumentParser(description='Start key-value storage server.')
    argp.add_argument('-H', '--host', default='', help='Host interface (default is hostname).')
    argp.add_argument('-p', '--port', type=int, default=0, help='Port (default is 0 --- let the OS choose).')
    argp.add_argument('-a', '--addrfile', default=None,  metavar='AddressFile', type=argparse.FileType('w'), help='Write address to this file.')
    argp.add_argument('-e', '--execcmd', default=None,  metavar='COMMAND SEQUENCE', help='Execute command with augmented environment.')
    argp.add_argument('-l', '--logfile', default=None,  metavar='KVSSLogfile', type=argparse.FileType('w'), help='Log file for key-value storage server.')
    args = argp.parse_args()

    # TODO: figure out where this should really go.
    lconf = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': logging.DEBUG}
    if args.logfile:
        args.logfile.close()
        lconf['filename'] = args.logfile.name
    logging.basicConfig(**lconf)

    t = KVSServer(args.host, args.port)
    addr = '%s:%d'%t.cinfo
    logger.info('Server running at %s.', addr)
    if args.addrfile:
        args.addrfile.write(addr)
        args.addrfile.close()

    try:
        if args.execcmd:
            import subprocess
            subprocess.check_call(args.execcmd, shell=True, env=t.env())
        else:
            while t.isAlive():
                t.join(60)
    finally:
        t.shutdown()
    t.join()
