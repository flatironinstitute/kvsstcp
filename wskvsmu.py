#!/usr/bin/env python
import argparse, base64, hashlib, json, kvsstcp, os, SimpleHTTPServer, socket, SocketServer, struct as S, sys
from threading import current_thread, Lock, Thread

ScriptDir = os.path.dirname(__file__)

# The web monitor implements two conventions wrt keys:
#
# 1) A key starting with '.' is not displayed.
#
# 2) A wait for a key of the form "key?fmt" creates an input
# box for the key. fmt will be used to format the input string
# to produce a "put" value for the key.
#
# It implements three conventions wrt to values:
#
# 1) An ASCII String encoded value that starts with '<HTML>' (case
# insensitive) will be sent directly to the web client after removing
# the '<HTML>' pefix. I.e., this combination permits the value to be
# render as HTML.
#
# 2) An ASCII String encoded value that does not start with '<HTML>'
# is sent HTML escaped and encoded (distinct from the KVS encoding
# scheme). This combination prevents the value from being interpreted
# as HTML.
#
# 3) In all other cases, python's 'repr' of the (KVS encoding, value)
# tuple is sent to the web client HTML escaped and encoded.
#

def jsonDefault(o):
    if isinstance(o, bytearray):
        return o.decode('latin-1')
    return repr(o)

def dump2json(kvsc):
    r = json.dumps(kvsc.dump(), ensure_ascii = False, check_circular = False, encoding = 'latin-1', default = jsonDefault)
    if isinstance(r, unicode): r = r.encode('latin-1')
    return r

def recvall(s, n):
    d = ''
    while n:
        b = s.recv(n)
        if not b: raise Exception('Connection dropped.')
        d += b
        n -= len(b)
    return d

class KVSWaitThread(Thread):
    def __init__(self, kvsaddr, wslist, mk, spec, name='KVSClientThread'):
        Thread.__init__(self, name=name)
        self.daemon = True
        self.mk = mk
        self.wslist = wslist
        self.kvsc = kvsstcp.KVSClient(kvsaddr)
        self.kvsc.monkey(mk, spec)
        self.start()

    def run(self):
        try: 
            while 1:
                r = self.kvsc.get(self.mk, False)
                j = dump2json(self.kvsc)
                self.wslist.broadcast(j)
        finally:
            self.wslist.broadcast('bye')
            self.kvsc.close()

class WebWaitThread(Thread):
    def __init__(self, kvsaddr, wslist, ws, name='WebWaitThread'):
        Thread.__init__(self, name=name)
        self.daemon = True
        self.ws = ws
        self.kvsc = kvsstcp.KVSClient(kvsaddr)
        self.wslist = wslist
        self.active = True
        self.start()

    def run(self):
        try:
            self.ws.acceptone()
        except socket.error, msg:
            return

        self.wslist.add(self)

        try:
            while self.active:
                op = self.ws.recv()
                if op == '': continue
                if op == 'bye': break
                elif op == 'dump':
                    j = dump2json(self.kvsc)
                    self.ws.send(j)
                elif op.startswith('put\x00'):
                    d, key, v = op.split('\x00')
                    if '?' in key:
                        k, fmt = key.split('?')
                        if fmt: v = fmt%v
                    self.kvsc.put(key, v, False)
                else: raise Exception('Unknown op from websocket: "%s".'%repr(op))
        finally:
            self.wslist.remove(self)
            self.kvsc.close()
            self.ws.close()

    def send(self, p):
        try:
            self.ws.send(p)
        except socket.error, msg:
            self.active = False
                
class WebSocketServer(object):
    wsmagic = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'

    handshake = '\
HTTP/1.1 101 Switching Protocols\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Accept: %s\r\n\r\n\
'

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((socket.gethostname(), 0))
        self.sock.settimeout(60)
        self.sock.listen(1)

    def acceptone(self):
        client, address = self.sock.accept()
        self.sock.close()

        # read the header (ends with '\r\n\r\n')
        header = ''
        while 1:
            header += client.recv(16)
            if header.find('\r\n\r\n') != -1: break

        req, data = header.split('\r\n\r\n', 1)
        assert data == ''
        x = req.index('Sec-WebSocket-Key: ') + 19 # 19 is the length of the search string.
        y = req.index('\r\n', x)
        k = req[x:y]
        reply = WebSocketServer.handshake%(base64.b64encode(hashlib.sha1(k + WebSocketServer.wsmagic).digest()))
        client.sendall(reply)
        self.client = client
        self.lock = Lock()

    def send(self, p):
        with self.lock:
            lp = len(p)
            if lp < 126:
                self.client.sendall(S.pack('BB', 0x81, lp))
            elif lp < 2**16:
                self.client.sendall(S.pack('!BBH', 0x81, 126, lp))
            else:
                self.client.sendall(S.pack('!BBQ', 0x81, 127, lp))
            self.client.sendall(p)

    def recv(self):
        r = self.client.recv(1)
        if not r: return 'bye'
        b = S.unpack('B', r)[0]
        op = b & 0xF
        if op == 0x9:
            print >> sys.stderr, 'Got a ping'
            #TODO: Do something here
            return ''
        elif op == 0XA:
            print >> sys.stderr, 'Got a pong'
            #TODO: Do something here?
            return ''
        elif op == 0x1:
            assert b & 0x80 # For this test, this frame must be marked FIN
            b = S.unpack('B', recvall(self.client, 1))[0]
            assert b & 0x80 # Masking must be on for frames from clients.
            plen = b & 0x7F
            assert plen < 126 # Limit on payload for this test.
            mb = S.unpack('4B', recvall(self.client, 4))
            p = recvall(self.client, plen)
            decode = ''
            for i in xrange(plen): decode += chr(ord(p[i]) ^ mb[i%4])
            return decode
        else:
            print >>sys.stderr, 'ws recv header byte: %02x'%b
            return ''

    def close(self):
        self.client.close()

class WebSocketList(object):
    def __init__(self):
        self.lock = Lock()
        self.wss = set() # of WebWaitThread
    
    def add(self, ws):
        with self.lock:
            self.wss.add(ws)

    def remove(self, ws):
        with self.lock:
            self.wss.remove(ws)

    def __iter__(self):
        with self.lock:
            return iter(self.wss.copy())

    def broadcast(self, p):
        for ws in self:
            # TODO: add timeout for sending to avoid blocking, or lift to separate thread
            ws.send(p)

lastFrontEnd = None
class FrontEndThread(Thread):
    def __init__(self, kvsserver, urlfile, wslist, name='FrontEndThread'):
        Thread.__init__(self, name=name)
        self.daemon = True
        self.kvsserver = kvsserver
        self.urlfile = urlfile
        self.wslist = wslist
        self.start()

    def run(self):
        import StringIO

        kvsserver = self.kvsserver
        wslist = self.wslist

        class FrontEnd(SimpleHTTPServer.SimpleHTTPRequestHandler):
            def send_head(self):
                if self.path.endswith('/kvsviewer'):
                    ws = WebSocketServer()
                    WebWaitThread(kvsserver, wslist, ws)
                    with open(os.path.join(ScriptDir, 'wskvspage.html')) as fep:
                        frontEndPage = fep.read()
                    frontEndPage = frontEndPage%ws.sock.getsockname()
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html; charset=utf-8')
                    self.send_header('Content-length', str(len(frontEndPage)))
                    self.end_headers()

                    return StringIO.StringIO(frontEndPage)
                else:
                    self.send_error(404, 'Not recognized')
                    return None

        self.httpd = SocketServer.TCPServer((socket.gethostname(), 0), FrontEnd)
        myurl = 'http://%s:%s/kvsviewer'%(self.httpd.server_address)
        print >>sys.stderr, 'front end at: '+myurl
        if self.urlfile:
            self.urlfile.write('%s\n'%(myurl))
            self.urlfile.close()

        self.httpd.serve_forever()

def main(kvsserver, urlfile=None, monitorkey='.webmonitor', monitorspec=':w'):
    wslist = WebSocketList()
    feThread = FrontEndThread(kvsserver, urlfile, wslist)
    return KVSWaitThread(kvsserver, wslist, monitorkey, monitorspec)

if '__main__' == __name__:
    argp = argparse.ArgumentParser(description='Start a web monitor for a key-value storage server.')
    argp.add_argument('-m', '--monitorkey', default='.webmonitor', help='Key to use for the monitor.')
    argp.add_argument('-s', '--monitorspec', default=':w', help='What to monitor: comma separted list of "[key]:[gpvw]" specifications.')
    argp.add_argument('-u', '--urlfile', default=None, type=argparse.FileType('w'), help='Write url to this file.')
    argp.add_argument('kvsserver', metavar='host:port', help='KVS server address.')
    args = argp.parse_args()

    kvsThread = main(args.kvsserver, args.urlfile, args.monitorkey, args.monitorspec)
    kvsThread.join()
