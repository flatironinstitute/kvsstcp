#!/usr/bin/env python
from collections import defaultdict as DD
from cPickle import dumps as PDS
import gc
import logging
import os
import resource
import socket
import SocketServer
import sys
from threading import current_thread, Lock, Semaphore as Sem, Thread

from kvscommon import *

logger = logging.getLogger('Key value store')
#logger.config.dictConfig({'format': '%(asctime)s - %(levelname)s: %(message)s', 'filename': 'toodle', 'level': logging.DEBUG3})

gc.disable()
# This never gets reenabled, though python does use perl-style reference counting, so this only impacts cyclic data structures

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    # Don't let a balky client thread prevent a server exit.
    daemon_threads = True

class KVSRequestHandler(SocketServer.BaseRequestHandler):
    # Called per connection. Keep the connection open until we get a
    # "'clos'e" op. TODO: Make this configurable.
    def handle(self):
        kvs, req, whoAmI = self.server.kvs, self.request, repr(current_thread())
        req.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        reqtxt = repr(req.getpeername())
        logger.info('Accepted connect from %s, using thread %s'%(reqtxt, whoAmI))
        # Keep a master list of client connections to be used when it is time to shutdown.
        with self.server.clientLock: self.server.clients.append(req)
        while 1:
            try:
                op = recvall(req, 4)
            except Exception, e: 
                # An error at this point most likely means a client
                # exited without sending close. No big deal.
                logger.info('(%s) handler exiting with "%s".'%(whoAmI, str(e)))
                break
            #DEBUGOFF            logger.debug('(%s) %s op "%s"'%(whoAmI, reqtxt, op))
            if 'clos' == op:
                req.shutdown(socket.SHUT_RDWR)
                req.close()
                break
            elif 'down' == op:
                with self.server.clientLock:
                    # This clientlock doesn't completely seem necessary (list.append is thread-safe), and in particular doesn't ensure that all clients are shutdown
                    # It might be better to shutdown the server first, and then handle closing clients once serve_forever returns, using while clients.pop: shutdown_request
                    for c in self.server.clients:
                        logger.info('Shutdown closing: %s (%s)', repr(c), repr(c.getpeername()))
                        try: c.shutdown(socket.SHUT_RDWR)
                        except Exception, e: logger.info('Ignoring socket.shutdown exception during shutdown: "%s".', e)
                        try: c.close()
                        except Exception, e: logger.info('Ignoring socket.close exception during shutdown: "%s".', e)
                
                logger.info('Calling server shutdown')
                self.server.shutdown()
                break
            elif 'dump' == op:
                d = kvs.dump()
                req.sendall(AsciiLenFormat%(len(d)))
                req.sendall(d)
                continue

            key = recvall(req, int(recvall(req, AsciiLenChars)))
            #DEBUGOFF            logger.debug('(%s) %s key "%s"'%(whoAmI, reqtxt, key))
            if 'get_' == op:
                (encoding, val) = kvs.get(key)
                # If the client closes the connection (cleanly or otherwise)
                # while we're waiting in the above get, we don't find out until
                # the get completes.  Ideally we'd notice the close and cancel
                # the get.  (This would still leave a race condition where the
                # value is lost if the client closes after the get completes
                # but before the send.)
                req.sendall(encoding)
                req.sendall(AsciiLenFormat%(len(val)))
                req.sendall(val)
            elif 'mkey' == op:
                val = recvall(req, int(recvall(req, AsciiLenChars)))
                #DEBUGOFF                logger.debug('(%s) val: %s'%(whoAmI, repr(val)))
                kvs.monkey(key, val)
            elif 'put_' == op:
                encoding = recvall(req, 4)
                val = recvallba(req, int(recvall(req, AsciiLenChars)))
                #DEBUGOFF                logger.debug('(%s) val: %s'%(whoAmI, repr(val)))
                kvs.put(key, (encoding, val))
            elif 'view' == op:
                (encoding, val) = kvs.view(key)
                req.sendall(encoding)
                req.sendall(AsciiLenFormat%(len(val)))
                req.sendall(val)
        logger.info('Closing connection from %s, thread %s'%(reqtxt, whoAmI))

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
        self.lock = Lock()                # Serializes access to the key/value dictionary.
        self.key2mon = DD(lambda:DD(set)) # Maps a normal key to keys that monitor it.
        self.monkeys = set()              # List of monitor keys.
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
            if v[0] != 'ASTR': return (v[0], len(v[1]))
            if v[1][:6].lower() == '<html>': return v
            if len(v[1]) > 50: return (v[0], len(v[1]), v[1][:24] + '...' + v[1][-23:])
            return v

        with self.lock:
            return PDS(([self.opCounts['get'], self.opCounts['put'], self.opCounts['view'], self.opCounts['wait'], self.ac, self.rc], [(k, len(v)) for k, v in self.waiters.iteritems()], [[k, len(vv), vrep(vv[-1])] for k, vv in self.store.iteritems()]))

    def get(self, k):
        '''Atomically remove and return a value associated with key k. If
        none, block.

        '''
        return self._gv(k, 'get')

    def _gv(self, k, op):
        #DEBUGOFF        logger.debug('_gv: %s, %s'%(repr(k), repr(op)))
        self._doMonkeys(op, k)
        delete = op == 'get'
        while 1:
            self.lock.acquire()
            vv = self.store.get(k)
            if vv:
                if delete:
                    v = vv.pop(self.getIndex)
                    if not vv: self.store.pop(k)
                else:
                    v = vv[self.viewIndex]
                self.opCounts[op] += 1
                self.lock.release()
                #DEBUGOFF                logger.debug('_gv (%s): %s => %s (%d)'%(op, k, repr(v[0]), len(v[1])))
                return v
            else:
                s = Sem(0)
                self.waiters[k].append((s, delete))
                self.opCounts['wait'] += 1
                self.lock.release()
                self._doMonkeys('wait', k)
                #DEBUGOFF                logger.debug('(%s) %s acquiring'%(repr(current_thread()), repr(s)))
                self.ac += 1
                s.acquire()

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
        with self.lock:
            self.monkeys.add(mkey)
            k, events = v.split(':')
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
        #DEBUGOFF        logger.debug('put: %s, %s'%(repr(k), repr(v)))
        with self.lock:
            self.opCounts['put'] += 1
            self.store[k].append(v)
            ww = self.waiters.get(k) # No waiters is probably most common, so optimize for
                                     # that. ww will be None if no waiters have been
                                     # registered for key k.
            if ww:
                while ww:
                    s, delete = ww.pop(0)
                    #DEBUGOFF                    logger.debug('%s releasing'%repr(s))
                    self.rc += 1
                    s.release()
                    if delete: break 
                if not ww: self.waiters.pop(k)
        self._doMonkeys('put', k)

    def view(self, k):
        '''Return a value associated with key k. If none, block.'''
        return self._gv(k, 'view')

class KVSServerThreadException(Exception): pass

class KVSServerThread(Thread):
    def __init__(self, host='', port=0, name='KVSServerThread'):
        Thread.__init__(self, name=name)

        if not host: host = socket.gethostname()
        self.server = ThreadedTCPServer((host, port), KVSRequestHandler)
        self.cinfo = (host, self.server.server_address[1])

        logger.info('Setting queue size to 4000')
        self.server.request_queue_size = 4000

        snof, hnof = resource.getrlimit(resource.RLIMIT_NOFILE)
        wantnof = min(hnof, 4096)
        if snof < wantnof:
            logger.info('Raising queue size from %d to %d'%(snof, wantnof))
            resource.setrlimit(resource.RLIMIT_NOFILE, (wantnof, hnof))

        self.server.clientLock = Lock()
        self.server.clients = []
        self.server.kvs = KVS()
        self.start()

    def run(self):
        self.server.serve_forever()
        self.server.server_close()

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

    t = KVSServerThread(args.host, args.port)
    addr = '%s:%d'%t.cinfo
    logger.info('Server running at %s.'%addr)
    if args.addrfile:
        args.addrfile.write(addr)
        args.addrfile.close()

    try:
        if args.execcmd:
            import subprocess
            os.environ['KVSSTCP_HOST'] = t.cinfo[0]
            os.environ['KVSSTCP_PORT'] = str(t.cinfo[1])
            subprocess.call(args.execcmd, shell=True)
        else:
            while t.isAlive():
                t.join(60)
    finally:
        t.server.shutdown();
    t.join()
