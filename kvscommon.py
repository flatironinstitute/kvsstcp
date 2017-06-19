import socket

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
        if not b: raise socket.error('Connection dropped')
        d += b
        n -= len(b)
    return d
