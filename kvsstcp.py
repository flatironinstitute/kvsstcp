#!/usr/bin/env python
from collections import defaultdict as DD
from cPickle import dump as PDF, dumps as PDS, load as PLF, loads as PLS
import gc
import logging
import os
import resource
import socket
import SocketServer
import sys
from threading import current_thread, Lock, Semaphore as Sem, Thread

logger = logging.getLogger('Key value store')
#logger.config.dictConfig({'format': '%(asctime)s - %(levelname)s: %(message)s', 'filename': 'toodle', 'level': logging.DEBUG3})

gc.disable()
# This never gets reenabled, though python does use perl-style reference counting, so this only impacts cyclic data structures

# The line protocol is very simple:
#
#  4 byte operation ('clos', 'dump', 'get_', 'mkey', 'put_', 'view')
#
# 'clos': No additional argument
#
# 'dump': No additional argument
#
# 'get_': One key argument, expects a value argument in reply.
#
# 'mkey' (monitor key): Two key arguments. The second may have a ': ...'
#     suffix indicating events to be monitored.
#
# 'put_': One key argument followed by one value argument.
# 
# 'view': One key argument, expects a value argument in reply.
#
# Key representation:
#     10 bytes: A 10 character string (ascii, not null terminated) with the base 10
#               representation of the byte length of the key string
#     length bytes: the key string
#
# Value representatin:
#     4 bytes: coding scheme.
#     10 bytes: A 10 character string (ascii, not null terminated) with the base 10
#               representation of the byte length of the argument
#     length bytes: the string representing the key
#
# Notes:
#
# 1) Coding schemes for values is a work in progress.
#

AsciiLenChars = 10 
AsciiLenFormat = '%%%dd'%AsciiLenChars
# Might be nicer to make this '%%0%dd'.
# Nothing checks to make sure lengths don't exceed 9GB...

def recvall(s, n):
    '''Wrapper to deal with partial recvs when we know there are N bytes to be had.'''
    d = ''
    while n:
        b = s.recv(n)
        if not b: raise Exception('Connection dropped.')
        d += b
        n -= len(b)
    return d

def recvallba(s, n):
    '''Wrapper to deal with partial recvs when we know there are N bytes to be had. Returns a byte array.'''
    d = bytearray(n)
    v = memoryview(d)
    while n:
        b = s.recv_into(v, n)
        if not b: raise Exception('Connection dropped.')
        v = v[b:]
        n -= b
    return d

class KVSClient(object):
    '''KVS convenience wrapper that includes pickling by default.'''
    def __init__(self, host=None, port=None): 
        '''Establish connection to a key value storage server at an address
        given by host, port or "host:port"

        '''
        if not host:
            host = os.environ.get('KVSSTCP_HOST', None)
            # TODO: Silently overrides user supplied value, if there is one.
            port = os.environ.get('KVSSTCP_PORT', None)
            
        if not host: raise Exception('Missing host')

        if not port:
            if host.count(':') != 1: raise Exception('Missing port')
            host, port = host.split(':')
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.socket.connect((host, int(port)))

    def close(self):
        '''Close the connection to the KVS storage server. Does a socket shutdown as well.'''
        try:
            self.socket.sendall('clos')
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        except Exception, e:
            # this is the client --- cannot assume logging is available.
            print >>sys.stderr, 'Ignoring exception during client close: "%s"'%e

    def dump(self):
        self.socket.sendall('dump')
        return self._recvValue(True)

    def get(self, k, usePickle=True):
        return self._gv(k, 'get_', usePickle)

    def _gv(self, k, op, usePickle):
        self.socket.sendall(op)
        self._sendLenAndBytes(k)
        # Ignore encoding for the time being.
        recvall(self.socket, 4)
        return self._recvValue(usePickle)

    def monkey(self, k, v):
        self.socket.sendall('mkey')
        self._sendLenAndBytes(k)
        self._sendLenAndBytes(v)

    def put(self, k, v, usePickle=True):
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

    def shutdown(self):
        '''Tell the KVS server to shutdown (and run the close() method for this client).'''
        self.socket.sendall('down')
        self.close()

    def view(self, k, usePickle=True):
        return self._gv(k, 'view', usePickle)

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
        self.startSem = Sem(0)
        self.start()
        self.startSem.acquire() # TODO: Is this really needed?
        # I can't imagine why -- only if some other thread may be messing with self
        # Also, it may be possible the release could happen first (since start was already called), which would be bad

    def run(self):
        self.startSem.release()
        self.server.serve_forever()
        # May need a self.server.sever_close() here

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

    if args.execcmd:
        import subprocess
        os.environ['KVSSTCP_HOST'] = t.cinfo[0]
        os.environ['KVSSTCP_PORT'] = str(t.cinfo[1])
        subprocess.call(args.execcmd, shell=True)
        t.server.shutdown()
    else:
        t.join()
