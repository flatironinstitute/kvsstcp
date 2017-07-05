#!/usr/bin/env python
import asyncore
from collections import defaultdict as DD
from cPickle import dumps as PDS
import gc
import logging
import os
import resource
import socket
import sys
from threading import Thread

from kvscommon import *

logger = logging.getLogger('Key value store')
#logger.config.dictConfig({'format': '%(asctime)s - %(levelname)s: %(message)s', 'filename': 'toodle', 'level': logging.DEBUG3})

# There are some cyclic references in in asyncio, handlers, waiters, etc., so I'm re-enabling this:
#gc.disable()

# Adds buffered output and input of known-size blocks (like dispatcher_with_send)
class KVSDispatcher(asyncore.dispatcher):
    def __init__(self, sock=None, map=None):
        asyncore.dispatcher.__init__(self, sock, map)
        self.out_buf = []
        self.in_buf = ''
        self.in_size = 0
        self.in_handler = None

    def writable(self):
        return (not self.connected) or len(self.out_buf)

    def readable(self):
        return (not self.connected) or self.in_handler

    def send(self, data):
        self.out_buf.append(data)
        self.handle_write()

    def handle_write(self):
        while self.out_buf:
            buf = self.out_buf[0]
            r = asyncore.dispatcher.send(self, buf)
            if r < len(buf):
                if r: self.out_buf[0] = buf[r:]
                return
            self.out_buf.pop(0)

    def next_read(self, size, f):
        self.in_size = size
        self.in_handler = f

    def handle_read(self):
        z = self.in_size
        n = len(self.in_buf)
        if n < z:
             self.in_buf += self.recv(z - n)
             n = len(self.in_buf)
        if n >= z:
            i = self.in_buf[:z]
            handler = self.in_handler
            self.in_buf = self.in_buf[z:]
            self.in_handler = None
            handler(i)

class KVSRequestHandler(KVSDispatcher):
    def __init__(self, pair, server):
        sock, addr = pair
        self.server = server
        # Keep track of any currently waiting get:
        self.waiter = None
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        KVSDispatcher.__init__(self, sock)
        logger.info('Accepted connect from %s', repr(addr))
        self.next_op()

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
            if n <= 0: raise Exception("invalid data len: '%s'" % l)
            self.next_read(n, handler)
        self.next_read(AsciiLenChars, handle_len)

    def handle_op(self, op):
        if 'clos' == op:
            self.socket.shutdown(socket.SHUT_RDWR)
        elif 'down' == op:
            logger.info('Calling server shutdown')
            self.server.shutdown()
        elif 'dump' == op:
            d = self.server.kvs.dump()
            self.send(AsciiLenFormat%(len(d)))
            self.send(d)
            self.next_op()
        elif op in ['get_', 'mkey', 'put_', 'view']:
            self.next_lendata(lambda key:
                    self.handle_opkey(op, key))
        else:
            raise Exception("Unknown op from %s: '%s'", repr(self.addr), op)

    def handle_opkey(self, op, key):
        #DEBUGOFF            logger.debug('(%s) %s key "%s"', whoAmI, reqtxt, key)
        if 'mkey' == op:
            self.next_lendata(lambda val:
                    self.handle_mkey(key, val))
        elif 'put_' == op:
            self.next_read(4, lambda encoding:
                    self.next_lendata(lambda val:
                        self.handle_put(key, encoding, val)))
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
        self.send(encoding)
        self.send(AsciiLenFormat%(len(val)))
        self.send(val)
        self.waiter = None

class KVSWaiter:
    def __init__(self, op, key, handler):
        if op == 'get_': op = 'get'
        self.op = op
        self.delete = op == 'get'
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
        self.opCounts = {'get': 0, 'put': 0, 'view': 0, 'wait': 0}
        self.ac, self.rc = 0, 0

    def _doMonkeys(self, op, k):
        # Don't monitor operations on monitor keys.
        if k in self.monkeys: return
        #DEBUGOFF        logger.debug('doMonkeys: %s %s %s', op, k, repr(self.key2mon[True][op] | self.key2mon[k][op]))
        for mk in self.key2mon[True][op] | self.key2mon[k][op]:
            self.put(mk, ('ASTR', repr((op, k))))
        
    def dump(self):
        '''Utility function that returns a snapshot of the KV store.'''
        def vrep(v):
            # Omit or truncate some values, in which cases add the original length as a third value
            if v[0] == 'JSON' or v[0] == 'HTML': return v
            if v[0] != 'ASTR': return (v[0], None, len(v[1]))
            if v[1][:6].lower() == '<html>': return v # for backwards compatibility only
            if len(v[1]) > 50: return (v[0], v[1][:24] + '...' + v[1][-23:], len(v[1]))
            return v

        return PDS(([self.opCounts['get'], self.opCounts['put'], self.opCounts['view'], self.opCounts['wait'], self.ac, self.rc], [(k, len(v)) for k, v in self.waiters.iteritems() if v], [[k, len(vv), vrep(vv[-1])] for k, vv in self.store.iteritems() if vv]))

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
            self.opCounts['wait'] += 1
            self._doMonkeys('wait', waiter.key)
            #DEBUGOFF                logger.debug('(%s) %s acquiring', repr(waiter), repr(s))
            self.ac += 1

    def cancel_wait(self, waiter):
        ww = self.waiters.get(waiter.key)
        if ww:
            ww.remove(waiter)
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
        if ':' not in v: return #TODO: Add some sort of error handling?
        self.monkeys.add(mkey)
        k, events = v.rsplit(':', 1)
        if not k: k = True
        for e, op  in [('g', 'get'), ('p', 'put'), ('v', 'view'), ('w', 'wait')]:
            if e in events:
                self.key2mon[k][op].add(mkey)
            else:
                try: self.key2mon[k][op].remove(mkey)
                except KeyError: pass
        #DEBUGOFF        logger.debug('monkey: %s', repr(self.key2mon))

    def put(self, k, v):
        '''Add value v to those associated with the key k.'''
        #DEBUGOFF        logger.debug('put: %s, %s', repr(k), repr(v))
        self.opCounts['put'] += 1
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
        self._doMonkeys('put', k)

class KVSServer(Thread, asyncore.dispatcher):
    def __init__(self, host, port):
        if not host: host = socket.gethostname()

        Thread.__init__(self, name='KVSServerThread')
        asyncore.dispatcher.__init__(self)
        self.kvs = KVS()

        snof, hnof = resource.getrlimit(resource.RLIMIT_NOFILE)
        wantnof = min(hnof, 4096)
        if snof < wantnof:
            logger.info('Raising queue size from %d to %d', snof, wantnof)
            resource.setrlimit(resource.RLIMIT_NOFILE, (wantnof, hnof))

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        logger.info('Setting queue size to 4000')
        self.listen(4000)
        self.cinfo = self.socket.getsockname()

        self.running = True
        self.start()

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            KVSRequestHandler(pair, self)

    def handle_close(self):
        logger.info('Server shutting down')
        self.running = False
        asyncore.close_all(ignore_all = True)

    def shutdown(self):
        if self.running:
            self.socket.shutdown(socket.SHUT_RDWR)

    def run(self):
        asyncore.loop(None, True)

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
            env = os.environ.copy()
            env['KVSSTCP_HOST'] = t.cinfo[0]
            env['KVSSTCP_PORT'] = str(t.cinfo[1])
            subprocess.call(args.execcmd, shell=True, env=env)
        else:
            while t.isAlive():
                t.join(60)
    finally:
        t.shutdown()
    t.join()
