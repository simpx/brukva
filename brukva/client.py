# -*- coding: utf-8 -*-
import socket
from functools import partial
from itertools import izip
import logging
from collections import Iterable, defaultdict
import weakref
import traceback
import time

from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from adisp import async, process

from datetime import datetime
from brukva.exceptions import RedisError, RequestError, \
    ConnectionError, ResponseError, InvalidResponse

log = logging.getLogger('brukva.client')
log_blob = logging.getLogger('brukva.client.blob')

class ExecutionContext(object):
    def __init__(self, callbacks, error_wrapper=None):
        self.callbacks = callbacks
        self.error_wrapper = error_wrapper
        self.is_active = True

    def _call_callbacks(self, callbacks, value):
        if callbacks:
            if isinstance(callbacks, Iterable):
                for cb in callbacks:
                    cb(value)
            else:
                callbacks(value)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, tb):
        if type_ is None:
            return True

        if self.error_wrapper:
            value = self.error_wrapper(value)
        else:
            value = value or Exception(
                'Strange exception with None value type: %s; tb: %s' %
                (type_, '\n'.join(traceback.format_tb(tb))
            ))

        if self.is_active:
            log.error(value, exc_info=(type_, value, tb))
            self.ret_call(value)
            return True
        else:
            return False

    def disable(self):
        self.is_active = False

    def enable(self):
        self.is_active = True

    def ret_call(self, value):
        self.is_active = False
        self._call_callbacks(self.callbacks, value)
        self.is_active = True

    def safe_call(self, callbacks, value):
        self.is_active = False
        self._call_callbacks(callbacks, value)
        self.is_active = True


def execution_context(callbacks, error_wrapper=None):
    """
    Syntax sugar.
    If some error occurred inside with block,
    it will be suppressed and forwarded to callbacks.

    Use contex.ret_call(value) method to call callbacks.

    @type callbacks: callable or iterator over callables
    @rtype: context
    """
    return ExecutionContext(callbacks, error_wrapper)

class Message(object):
    def __init__(self, kind, channel, body):
        self.kind = kind
        self.channel = channel
        self.body = body

    def __unicode__(self):
        return u'<PubSub %s on channel %s: %s >' % (self.kind, self.channel, self.body)

    def __str__(self):
        return '<PubSub %s on channel %s: %s >' % (self.kind, self.channel, self.body)

    def __repr__(self):
        return repr(str(self))

class CmdLine(object):
    def __init__(self, cmd, *args, **kwargs):
        self.cmd = cmd
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return self.cmd + '(' + str(self.args)  + ',' + str(self.kwargs) + ')'

def string_keys_to_dict(key_string, callback):
    return dict([(key, callback) for key in key_string.split()])

def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged

def encode(value):
    if isinstance(value, str):
        return value
    elif isinstance(value, unicode):
        return value.encode('utf-8')
    # pray and hope
    return str(value)

def format(*tokens):
    cmds = []
    for t in tokens:
        e_t = encode(t)
        cmds.append('$%s\r\n%s\r\n' % (len(e_t), e_t))
    return '*%s\r\n%s' % (len(tokens), ''.join(cmds))

def format_pipeline_request(command_stack):
    """
        @command_stack: [CmdLine(), ...]
        @return: str
        Return serialized request to redis for pipeline
    """
    return ''.join(format(c.cmd, *c.args, **c.kwargs) for c in command_stack)


class Connection(object):
    def __init__(self, host, port, idx,
        on_connect, on_disconnect,
        timeout=None, io_loop=None):

        # FIXME: io_loop can't be None
        self.host = host
        self.port = port
        self.idx = idx

        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        self.timeout = timeout
        self._io_loop = io_loop

        self._stream = None
        self.try_left = 2
        self.is_initialized = False
        self.in_progress = False
        self.read_queue = []

    def connect(self, add_to_free=True):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(self.timeout)
            sock.connect((self.host, self.port))
            self._stream = IOStream(sock, io_loop=self._io_loop)
        except socket.error, e:
            log.error(e)
            raise ConnectionError(str(e))
        self._io_loop.add_callback(lambda: self.on_connect(weakref.proxy(self), add_to_free=add_to_free))

    def disconnect(self):
        if self._stream:
            try:
                self._stream.close()
            except socket.error, e:
                pass
            self.is_initialized = False
            self._stream = None

    def write(self, data, callback, try_left=None, before_init=False):
        if try_left is None:
            try_left = self.try_left
        if not self._stream:
            self.connect(add_to_free=False)
            if not self._stream:
                callback(ConnectionError('Tried to write to non-existent connection'))
                return

        if not self.is_initialized and not before_init:
                log_blob.debug('wait for connection initialization')
                self._io_loop.add_callback(lambda: self.write(data, callback, try_left))
                return

        if try_left > 0:
            try:
                log_blob.debug('try write %r to %s', data, self.idx)
                self._stream.write(data)

                log_blob.debug('data written to socket')
                callback(True)
                #self._io_loop.add_callback(lambda: callback(True))
            except IOError, e:
                log.error(e)
                self.disconnect()
                self.write(data, callback=callback, try_left=try_left - 1)
        else:
            callback(ConnectionError('Tried to write to non-existent connection'))

    def read(self, length, callback):
        try:
            if not self._stream:
                self.disconnect()
                raise ConnectionError('Tried to read from non-existent connection')
            log_blob.debug('read %s bytes from %s', length, self.idx)
            self._stream.read_bytes(length, callback)
        except IOError:
            self.on_disconnect()

    def readline(self, callback):
        try:
            if not self._stream:
                self.disconnect()
                raise ConnectionError('Tried to read from non-existent connection')

            log_blob.debug('readline from %s', self.idx)
            self._stream.read_until('\r\n', callback)
        except IOError:
            self.on_disconnect()

class ConnectionPool(object):
    def __init__(self, connection_args, on_connect, on_disconnect, io_loop, pool_size=4):
        """
            connection_args:
            {
                'host' = 'localhost',
                port = 6379,
                timeout = None,

                selected_db = None,
                auth = None,
            }
        """
        self.pool_size = pool_size
        self.connection_args = connection_args

        self._on_connect = on_connect
        self._on_disconnect = on_disconnect

        self.io_loop = io_loop

        self.connection_args['io_loop'] = io_loop
        self.connection_args['on_disconnect'] =  on_disconnect

        self.connection_requests_queue = []

        self.is_connected = False


    def connect(self):
        """
            Create connection pool, connect all connection
                and perform AUTH, SELECT if needed.
        """
        self.connections = {}
        self.free_connections = set()
        connection_args = self.connection_args
        for idx in xrange(self.pool_size):
            @process
            def on_connect(connection, add_to_free=True):
                log_blob.debug('on_connect from pool')
                yield async(self._on_connect)(connection)
                if add_to_free:
                    self.free_connections.add(connection.idx)
                    log.info('connection %s added to pool',
                        connection.idx)
                self.io_loop.add_callback(self._give_out_pending_requests)

            connection_args['on_connect'] = on_connect
            connection_args['idx'] = idx
            connection = Connection(**connection_args)
            connection.connect()
            self.connections[idx] = connection
        self.is_connected = True

    def _give_out_pending_requests(self):
        while self.connection_requests_queue and self.free_connections:
            log_blob.debug('late leasing connection')
            callback = self.connection_requests_queue.pop(0)
            self._lease_connection(callback)

    def return_connection(self, connection):
        self.free_connections.add(connection.idx)
        log_blob.debug('returned connection: %s', connection.idx)
        self._give_out_pending_requests()

    def _lease_connection(self, callback):
        idx = self.free_connections.pop()
        log_blob.debug('leasing connection %s', idx)
        connection = self.connections[idx]
        self.io_loop.add_callback(lambda: callback(connection))

    def request_connection(self, callback):
        if not self.free_connections:
            log_blob.debug('no free connections, waiting')
            self.connection_requests_queue.append(callback)
        else:
            self._lease_connection(callback)

def reply_to_bool(r, *args, **kwargs):
    return bool(r)

def make_reply_assert_msg(msg):
    def reply_assert_msg(r, *args, **kwargs):
        return r == msg
    return reply_assert_msg

def reply_set(r, *args, **kwargs):
    return set(r)

def reply_dict_from_pairs(r, *args, **kwargs):
    return dict(izip(r[::2], r[1::2]))

def reply_str(r, *args, **kwargs):
    return r or ''

def reply_int(r, *args, **kwargs):
    return int(r) if r is not None else None

def reply_float(r, *args, **kwargs):
    return float(r) if r is not None else None

def reply_datetime(r, *args, **kwargs):
    return datetime.fromtimestamp(int(r))

def reply_pubsub_message(r, *args, **kwargs):
    return Message(*r)

def reply_zset(r, *args, **kwargs):
    if (not r ) or (not 'WITHSCORES' in args):
        return r
    return zip(r[::2], map(float, r[1::2]))

def reply_hmget(r, key, *fields, **kwargs):
    return dict(zip(fields, r))

def reply_info(response):
    info = {}
    def get_value(value):
        if ',' not in value:
            return value
        sub_dict = {}
        for item in value.split(','):
            k, v = item.split('=')
            try:
                sub_dict[k] = int(v)
            except ValueError:
                sub_dict[k] = v
        return sub_dict
    for line in response.splitlines():
        key, value = line.split(':')
        try:
            info[key] = int(value)
        except ValueError:
            info[key] = get_value(value)
    return info

def reply_ttl(r, *args, **kwargs):
    return r != -1 and r or None


PUB_SUB_COMMANDS = set([
    'SUBSCRIBE',
    'UNSUBSCRIBE',
    'LISTEN',
])

class _AsyncWrapper(object):
    def __init__(self, obj):
        self.obj = obj
        self.memoized = {}

    def __getattr__(self, item):
        if item not in self.memoized:
            self.memoized[item] = async(getattr(self.obj, item), cbname='callbacks')
        return self.memoized[item]


class Client(object):
    def __init__(self, host='localhost', port=6379, password=None,
            selected_db=None, io_loop=None):
        self._io_loop = io_loop or IOLoop.instance()
        self.host = host
        self.port = port

        self.connection_pool = ConnectionPool({
            'host': host,
            'port': port,
        },
            on_connect = self._initialize_connection,
            on_disconnect = self.on_disconnect,
            io_loop = self._io_loop
        )

        self.async = _AsyncWrapper(weakref.proxy(self))
        self.queue = []
        self.current_cmd_line = None
        self.subscribed = False
        self.password = password
        self.selected_db = selected_db
        self.REPLY_MAP = dict_merge(
                string_keys_to_dict('AUTH BGREWRITEAOF BGSAVE DEL EXISTS EXPIRE HDEL HEXISTS '
                                    'HMSET MOVE MSET MSETNX SAVE SETNX',
                                    reply_to_bool),
                string_keys_to_dict('FLUSHALL FLUSHDB SELECT SET SETEX SHUTDOWN '
                                    'RENAME RENAMENX WATCH UNWATCH',
                                    make_reply_assert_msg('OK')),
                string_keys_to_dict('SMEMBERS SINTER SUNION SDIFF',
                                    reply_set),
                string_keys_to_dict('HGETALL BRPOP BLPOP',
                                    reply_dict_from_pairs),
                string_keys_to_dict('HGET',
                                    reply_str),
                string_keys_to_dict('SUBSCRIBE UNSUBSCRIBE LISTEN',
                                    reply_pubsub_message),
                string_keys_to_dict('ZRANK ZREVRANK',
                                    reply_int),
                string_keys_to_dict('ZSCORE ZINCRBY ZCOUNT ZCARD',
                                    reply_int),
                string_keys_to_dict('ZRANGE ZRANGEBYSCORE ZREVRANGE',
                                    reply_zset),
                {'HMGET': reply_hmget},
                {'PING': make_reply_assert_msg('PONG')},
                {'LASTSAVE': reply_datetime },
                {'TTL': reply_ttl } ,
                {'INFO': reply_info},
                {'MULTI_PART': make_reply_assert_msg('QUEUED')},
            )

        self._waiting_callbacks = defaultdict(list)
        self._pipeline = None

    def __repr__(self):
        return '<Brukva client %s:%s>' % (self.host, self.port)

    def pipeline(self, transactional=False):
        if not self._pipeline:
            if  self.connection_pool.is_connected:
                self._pipeline = Pipeline(
                    selected_db=self.selected_db,
                    password=self.password,
                    io_loop = self._io_loop,
                    transactional=transactional
                )
                self._pipeline.connection_pool = self.connection_pool
            else:
                raise RedisError('Client must be connected befor creating pipeline')
        return self._pipeline

    #### connection
    def connect(self):
        self.connection_pool.connect()

    def disconnect(self):
        raise NotImplementedError()
        pass
        #self.connection.disconnect()

    def on_disconnect(self):
        if self.subscribed:
            self.subscribed = False
        raise ConnectionError("Socket closed on remote end")
    ####

    #### formatting
    def encode(self, value):
        if isinstance(value, str):
            return value
        elif isinstance(value, unicode):
            return value.encode('utf-8')
        # pray and hope
        return str(value)

    def format(self, *tokens):
        cmds = []
        for t in tokens:
            e_t = self.encode(t)
            cmds.append('$%s\r\n%s\r\n' % (len(e_t), e_t))
        return '*%s\r\n%s' % (len(tokens), ''.join(cmds))

    def format_reply(self, cmd_line, data):
        if cmd_line.cmd not in self.REPLY_MAP:
            return data
        try:
            res =  self.REPLY_MAP[cmd_line.cmd](data, *cmd_line.args, **cmd_line.kwargs)
        except Exception, e:
            raise ResponseError(
                'failed to format reply to %s, raw data: %s; err message: %s' %
                (cmd_line, data, e), cmd_line
            )
        return res
    ####

    #### new AsIO
    def call_callbacks(self, callbacks, *args, **kwargs):
        for cb in callbacks:
            cb(*args, **kwargs)

    @process
    def _initialize_connection(self, connection, callback):
        """
            call after connection creation
        """
        log_blob.debug('initializing connection')
        with execution_context(callback) as ctx:
            cmds = []
            if self.password:
                cmds.append(CmdLine('AUTH', self.password))
            if self.selected_db:
                cmds.append(CmdLine('SELECT', self.selected_db))

            if cmds:
                try:
                    for cmd_line in cmds:
                        log_blob.debug('try to write to connection %s: %s', connection.idx, cmd_line)
                        write_res = yield async(connection.write)(self.format(
                            cmd_line.cmd,
                            *cmd_line.args,
                            **cmd_line.kwargs
                        ), before_init=True)
                        log_blob.debug('conn init, write_res: %s', write_res)
                except Exception, e:
                    connection.disconnect()
                    raise e

                for cmd_line in cmds:
                    data = yield async(connection.readline)()
                    response = yield self.process_data(connection, data, cmd_line)
                    result = self.format_reply(cmd_line, response)
                    log_blob.debug('got result %s on cmd %s', result, cmd_line)

            connection.is_initialized = True
            ctx.ret_call(True)


    @process
    def execute_command(self, cmd, callbacks, *args, **kwargs):
        cmd_line = CmdLine(cmd, *args, **kwargs)
        with execution_context(callbacks) as ctx:
            if callbacks is None:
                callbacks = []
            elif not hasattr(callbacks, '__iter__'):
                callbacks = [callbacks]

            if self.subscribed and cmd not in PUB_SUB_COMMANDS:
                ctx.ret_call(RequestError('Calling not pub/sub command during subscribed state', cmd_line))
                return

            log_blob.debug('try to get connection for %s',  cmd_line)
            connection = yield async(self.connection_pool.request_connection)()
            log_blob.debug('got connection %s for %s cmd_line',
                connection.idx, cmd_line)
            try:
                write_res = yield async(connection.write)(self.format(cmd, *args, **kwargs))
            except Exception, e:
                connection.disconnect()
                raise e
            if isinstance(write_res, Exception):
                raise write_res

            #if self.subscribed and cmd in ('SUBSCRIBE', 'UNSUBSCRIBE'):
            #    self._waiting_callbacks[cmd].append(callbacks)
            #    return

            data = yield async(connection.readline)()
            if not data:
                self.connection_pool.return_connection(connection)
                raise Exception('TODO: [no data from connection %s->readline' % connection.idx)
            else:
                response = yield self.process_data(connection, data, cmd_line)
                result = self.format_reply(cmd_line, response)
                self.connection_pool.return_connection(connection)
                log_blob.debug('got result %s on cmd %s with connection %s', result, cmd_line, connection.idx)

            ctx.ret_call(result)

    @async
    @process
    def process_data(self, connection, data, cmd_line, callback):
        with execution_context(callback) as ctx:
            data = data[:-2] # strip \r\n

            if data == '$-1':
                response =  None
            elif data == '*0' or data == '*-1':
                response = []
            else:
                if len(data) == 0:
                    raise IOError('Disconnected')
                head, tail = data[0], data[1:]

                if head == '*':
                    response = yield self.consume_multibulk(connection, int(tail), cmd_line)
                elif head == '$':
                    response = yield self.consume_bulk(connection, int(tail)+2)
                elif head == '+':
                    response = tail
                elif head == ':':
                    response = int(tail)
                elif head == '-':
                    if tail.startswith('ERR'):
                        tail = tail[4:]
                    response = ResponseError(tail, cmd_line)
                else:
                    raise ResponseError('Unknown response type %s' % head, cmd_line)
            ctx.ret_call(response)

    @async
    @process
    def consume_multibulk(self, connection, length, cmd_line, callback):
        with execution_context(callback) as ctx:
            tokens = []
            while len(tokens) < length:
                data = yield async(connection.readline)()
                if not data:
                    raise ResponseError(
                        'Not enough data in response to %s, accumulated tokens: %s'%
                        (cmd_line, tokens), cmd_line
                    )
                token = yield self.process_data(connection, data, cmd_line) #FIXME error
                tokens.append( token )

            ctx.ret_call(tokens)

    @async
    @process
    def consume_bulk(self, connection, length, callback):
        with execution_context(callback) as ctx:
            data = yield async(connection.read)(length)
            if isinstance(data, Exception):
                raise data
            if not data:
                raise ResponseError('EmptyResponse')
            else:
                data = data[:-2]
            ctx.ret_call(data)

    ### MAINTENANCE
    def bgrewriteaof(self, callbacks=None):
        self.execute_command('BGREWRITEAOF', callbacks)

    def dbsize(self, callbacks=None):
        self.execute_command('DBSIZE', callbacks)

    def flushall(self, callbacks=None):
        self.execute_command('FLUSHALL', callbacks)

    def flushdb(self, callbacks=None):
        self.execute_command('FLUSHDB', callbacks)

    def ping(self, callbacks=None):
        self.execute_command('PING', callbacks)

    def info(self, callbacks=None):
        self.execute_command('INFO', callbacks)


    def shutdown(self, callbacks=None):
        self.execute_command('SHUTDOWN', callbacks)

    def save(self, callbacks=None):
        self.execute_command('SAVE', callbacks)

    def bgsave(self, callbacks=None):
        self.execute_command('BGSAVE', callbacks)

    def lastsave(self, callbacks=None):
        self.execute_command('LASTSAVE', callbacks)

    def keys(self, pattern, callbacks=None):
        self.execute_command('KEYS', callbacks, pattern)

    ### BASIC KEY COMMANDS
    def append(self, key, value, callbacks=None):
        self.execute_command('APPEND', callbacks, key, value)

    def expire(self, key, ttl, callbacks=None):
        self.execute_command('EXPIRE', callbacks, key, ttl)

    def ttl(self, key, callbacks=None):
        self.execute_command('TTL', callbacks, key)

    def type(self, key, callbacks=None):
        self.execute_command('TYPE', callbacks, key)

    def randomkey(self, callbacks=None):
        self.execute_command('RANDOMKEY', callbacks)

    def rename(self, src, dst, callbacks=None):
        self.execute_command('RENAME', callbacks, src, dst)

    def renamenx(self, src, dst, callbacks=None):
        self.execute_command('RENAMENX', callbacks, src, dst)

    def move(self, key, db, callbacks=None):
        self.execute_command('MOVE', callbacks, key, db)

    def substr(self, key, start, end, callbacks=None):
        self.execute_command('SUBSTR', callbacks, key, start, end)

    def delete(self, key, callbacks=None):
        self.execute_command('DEL', callbacks, key)

    def set(self, key, value, callbacks=None):
        self.execute_command('SET', callbacks, key, value)

    def setex(self, key, ttl, value, callbacks=None):
        self.execute_command('SETEX', callbacks, key, ttl, value)

    def setnx(self, key, value, callbacks=None):
        self.execute_command('SETNX', callbacks, key, value)

    def mset(self, mapping, callbacks=None):
        items = []
        [ items.extend(pair) for pair in mapping.iteritems() ]
        self.execute_command('MSET', callbacks, *items)

    def msetnx(self, mapping, callbacks=None):
        items = []
        [ items.extend(pair) for pair in mapping.iteritems() ]
        self.execute_command('MSETNX', callbacks, *items)

    def get(self, key, callbacks=None):
        self.execute_command('GET', callbacks, key)

    def mget(self, keys, callbacks=None):
        self.execute_command('MGET', callbacks, *keys)

    def getset(self, key, value, callbacks=None):
        self.execute_command('GETSET', callbacks, key, value)

    def exists(self, key, callbacks=None):
        self.execute_command('EXISTS', callbacks, key)

    def sort(self, key, start=None, num=None, by=None, get=None, desc=False, alpha=False, store=None, callbacks=None):
        if (start is not None and num is None) or (num is not None and start is None):
            raise ValueError("``start`` and ``num`` must both be specified")

        tokens = [key]
        if by is not None:
            tokens.append('BY')
            tokens.append(by)
        if start is not None and num is not None:
            tokens.append('LIMIT')
            tokens.append(start)
            tokens.append(num)
        if get is not None:
            tokens.append('GET')
            tokens.append(get)
        if desc:
            tokens.append('DESC')
        if alpha:
            tokens.append('ALPHA')
        if store is not None:
            tokens.append('STORE')
            tokens.append(store)
        return self.execute_command('SORT', callbacks, *tokens)

    ### COUNTERS COMMANDS
    def incr(self, key, callbacks=None):
        self.execute_command('INCR', callbacks, key)

    def decr(self, key, callbacks=None):
        self.execute_command('DECR', callbacks, key)

    def incrby(self, key, amount, callbacks=None):
        self.execute_command('INCRBY', callbacks, key, amount)

    def decrby(self, key, amount, callbacks=None):
        self.execute_command('DECRBY', callbacks, key, amount)

    ### LIST COMMANDS
    def blpop(self, keys, timeout=0, callbacks=None):
        tokens = list(keys)
        tokens.append(timeout)
        self.execute_command('BLPOP', callbacks, *tokens)

    def brpop(self, keys, timeout=0, callbacks=None):
        tokens = list(keys)
        tokens.append(timeout)
        self.execute_command('BRPOP', callbacks, *tokens)

    def brpoplpush(self, src, dst, timeout=1, callbacks=None):
        tokens = [src, dst, timeout]
        self.execute_command('BRPOPLPUSH', callbacks, *tokens)

    def lindex(self, key, index, callbacks=None):
        self.execute_command('LINDEX', callbacks, key, index)

    def llen(self, key, callbacks=None):
        self.execute_command('LLEN', callbacks, key)

    def lrange(self, key, start, end, callbacks=None):
        self.execute_command('LRANGE', callbacks, key, start, end)

    def lrem(self, key, value, num=0, callbacks=None):
        self.execute_command('LREM', callbacks, key, num, value)

    def lset(self, key, index, value, callbacks=None):
        self.execute_command('LSET', callbacks, key, index, value)

    def ltrim(self, key, start, end, callbacks=None):
        self.execute_command('LTRIM', callbacks, key, start, end)

    def lpush(self, key, value, callbacks=None):
        self.execute_command('LPUSH', callbacks, key, value)

    def rpush(self, key, value, callbacks=None):
        self.execute_command('RPUSH', callbacks, key, value)

    def lpop(self, key, callbacks=None):
        self.execute_command('LPOP', callbacks, key)

    def rpop(self, key, callbacks=None):
        self.execute_command('RPOP', callbacks, key)

    def rpoplpush(self, src, dst, callbacks=None):
        self.execute_command('RPOPLPUSH', callbacks, src, dst)

    ### SET COMMANDS
    def sadd(self, key, value, callbacks=None):
        self.execute_command('SADD', callbacks, key, value)

    def srem(self, key, value, callbacks=None):
        self.execute_command('SREM', callbacks, key, value)

    def scard(self, key, callbacks=None):
        self.execute_command('SCARD', callbacks, key)

    def spop(self, key, callbacks=None):
        self.execute_command('SPOP', callbacks, key)

    def smove(self, src, dst, value, callbacks=None):
        self.execute_command('SMOVE', callbacks, src, dst, value)

    def sismember(self, key, value, callbacks=None):
        self.execute_command('SISMEMBER', callbacks, key, value)

    def smembers(self, key, callbacks=None):
        self.execute_command('SMEMBERS', callbacks, key)

    def srandmember(self, key, callbacks=None):
        self.execute_command('SRANDMEMBER', callbacks, key)

    def sinter(self, keys, callbacks=None):
        self.execute_command('SINTER', callbacks, *keys)

    def sdiff(self, keys, callbacks=None):
        self.execute_command('SDIFF', callbacks, *keys)

    def sunion(self, keys, callbacks=None):
        self.execute_command('SUNION', callbacks, *keys)

    def sinterstore(self, keys, dst, callbacks=None):
        self.execute_command('SINTERSTORE', callbacks, dst, *keys)

    def sunionstore(self, keys, dst, callbacks=None):
        self.execute_command('SUNIONSTORE', callbacks, dst, *keys)

    def sdiffstore(self, keys, dst, callbacks=None):
        self.execute_command('SDIFFSTORE', callbacks, dst, *keys)

    ### SORTED SET COMMANDS
    def zadd(self, key, score, value, callbacks=None):
        self.execute_command('ZADD', callbacks, key, score, value)

    def zcard(self, key, callbacks=None):
        self.execute_command('ZCARD', callbacks, key)

    def zincrby(self, key, value, amount, callbacks=None):
        self.execute_command('ZINCRBY', callbacks, key, amount, value)

    def zrank(self, key, value, callbacks=None):
        self.execute_command('ZRANK', callbacks, key, value)

    def zrevrank(self, key, value, callbacks=None):
        self.execute_command('ZREVRANK', callbacks, key, value)

    def zrem(self, key, value, callbacks=None):
        self.execute_command('ZREM', callbacks, key, value)

    def zcount(self, key, start, end, offset=None, limit=None, with_scores=None, callbacks=None):
        tokens = [key, start, end]
        if offset is not None:
            tokens.append('LIMIT')
            tokens.append(offset)
            tokens.append(limit)
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZCOUNT', callbacks, *tokens)

    def zcard(self, key, callbacks=None):
        self.execute_command('ZCARD', callbacks, key)

    def zscore(self, key, value, callbacks=None):
        self.execute_command('ZSCORE', callbacks, key, value)

    def zrange(self, key, start, num, with_scores, callbacks=None):
        tokens = [key, start, num]
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZRANGE', callbacks, *tokens)

    def zrevrange(self, key, start, num, with_scores, callbacks=None):
        tokens = [key, start, num]
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZREVRANGE', callbacks, *tokens)

    def zrangebyscore(self, key, start, end, offset=None, limit=None, with_scores=False, callbacks=None):
        tokens = [key, start, end]
        if offset is not None:
            tokens.append('LIMIT')
            tokens.append(offset)
            tokens.append(limit)
        if with_scores:
            tokens.append('WITHSCORES')
        self.execute_command('ZRANGEBYSCORE', callbacks, *tokens)

    def zremrangebyrank(self, key, start, end, callbacks=None):
        self.execute_command('ZREMRANGEBYRANK', callbacks, key, start, end)

    def zremrangebyscore(self, key, start, end, callbacks=None):
        self.execute_command('ZREMRANGEBYSCORE', callbacks, key, start, end)

    def zinterstore(self, dest, keys, aggregate=None, callbacks=None):
        return self._zaggregate('ZINTERSTORE', dest, keys, aggregate, callbacks)

    def zunionstore(self, dest, keys, aggregate=None, callbacks=None):
        return self._zaggregate('ZUNIONSTORE', dest, keys, aggregate, callbacks)

    def _zaggregate(self, command, dest, keys, aggregate, callbacks):
        tokens = [dest, len(keys)]
        if isinstance(keys, dict):
            items = keys.items()
            keys = [i[0] for i in items]
            weights = [i[1] for i in items]
        else:
            weights = None
        tokens.extend(keys)
        if weights:
            tokens.append('WEIGHTS')
            tokens.extend(weights)
        if aggregate:
            tokens.append('AGGREGATE')
            tokens.append(aggregate)
        return self.execute_command(command, callbacks, *tokens)

    ### HASH COMMANDS
    def hgetall(self, key, callbacks=None):
        self.execute_command('HGETALL', callbacks, key)

    def hmset(self, key, mapping, callbacks=None):
        items = []
        [ items.extend(pair) for pair in mapping.iteritems() ]
        self.execute_command('HMSET', callbacks, key, *items)

    def hset(self, key, field, value, callbacks=None):
        self.execute_command('HSET', callbacks, key, field, value)

    def hget(self, key, field, callbacks=None):
        self.execute_command('HGET', callbacks, key, field)

    def hdel(self, key, field, callbacks=None):
        self.execute_command('HDEL', callbacks, key, field)

    def hlen(self, key, callbacks=None):
        self.execute_command('HLEN', callbacks, key)

    def hexists(self, key, field, callbacks=None):
        self.execute_command('HEXISTS', callbacks, key, field)

    def hincrby(self, key, field, amount=1, callbacks=None):
        self.execute_command('HINCRBY', callbacks, key, field, amount)

    def hkeys(self, key, callbacks=None):
        self.execute_command('HKEYS', callbacks, key)

    def hmget(self, key, fields, callbacks=None):
        self.execute_command('HMGET', callbacks, key, *fields)

    def hvals(self, key, callbacks=None):
        self.execute_command('HVALS', callbacks, key)

    ### PUBSUB
    def subscribe(self, channels, callbacks=None):
        callbacks = callbacks or []
        if not isinstance(callbacks, Iterable):
            callbacks = [callbacks]
        if isinstance(channels, basestring):
            channels = [channels]
        if not self.subscribed:
            callbacks = list(callbacks) + [self.on_subscribed]
        self.execute_command('SUBSCRIBE', callbacks, *channels)

    def on_subscribed(self, result):
        self.subscribed = True

    def unsubscribe(self, channels, callbacks=None):
        callbacks = callbacks or []
        if not isinstance(callbacks, Iterable):
            callbacks = [callbacks]
        if isinstance(channels, basestring):
            channels = [channels]
        callbacks = list(callbacks)
        self.execute_command('UNSUBSCRIBE', callbacks, *channels)

    def on_unsubscribed(self, *args, **kwargs):
        self.subscribed = False

    def publish(self, channel, message, callbacks=None):
        self.execute_command('PUBLISH', callbacks, channel, message)

    @process
    def _listen(self, callbacks=None):
        # 'LISTEN' is just for receiving information, it is not actually sent anywhere
        def error_wrapper(e):
            if isinstance(e, GeneratorExit):
                return ConnectionError('Connection lost')
            else:
                return e

        with execution_context(callbacks, error_wrapper) as ctx:
            callbacks = callbacks or []
            if not hasattr(callbacks, '__iter__'):
                callbacks = [callbacks]
            yield self.connection.queue_wait()

            cmd_listen = CmdLine('LISTEN')
            while self.subscribed:
                data = yield async(self.connection.readline)()
                if isinstance(data, Exception):
                    raise data

                response = yield self.process_data(data, cmd_listen)
                if isinstance(response, Exception):
                    raise response

                result = self.format_reply(cmd_listen, response)

                if result.kind != 'message':
                    waiting_stack = self._waiting_callbacks[result.kind.upper()]
                    if len(waiting_stack) > 0:
                        ctx.safe_call(waiting_stack.pop(0), result)

                    if result.kind == 'unsubscribe' and result.body == 0:
                        self.on_unsubscribed()
                        self.connection.read_done()
                        break
                else:
                    ctx.ret_call(result)

    ### CAS
    def watch(self, key, callbacks=None):
        self.execute_command('WATCH', callbacks, key)

    def unwatch(self, callbacks=None):
        self.execute_command('UNWATCH', callbacks)

class Pipeline(Client):
    def __init__(self, transactional, *args, **kwargs):
        super(Pipeline, self).__init__(*args, **kwargs)
        self.transactional = transactional
        self.command_stack = []

    def execute_command(self, cmd, callbacks, *args, **kwargs):
        if cmd in PUB_SUB_COMMANDS:
            raise RequestError(
                'Client is not supposed to issue command %s in pipeline' % cmd)
        self.command_stack.append(CmdLine(cmd, *args, **kwargs))

    def discard(self): # actually do nothing with redis-server, just flush command_stack
        self.command_stack = []

    @process
    def execute(self, callbacks):
        with execution_context(callbacks) as ctx:
            command_stack = self.command_stack
            self.command_stack = []

            if callbacks is None:
                callbacks = []
            elif not hasattr(callbacks, '__iter__'):
                callbacks = [callbacks]

            if self.transactional:
                command_stack = [CmdLine('MULTI')] + command_stack + [CmdLine('EXEC')]

            request = format_pipeline_request(command_stack)

            log_blob.debug('try to get connection for %r', request)
            connection = yield async(self.connection_pool.request_connection)()
            log_blob.debug('got connection %s for %r request', \
                connection.idx, request)
            try:
                write_res = yield async(connection.write)(request)
                log_blob.debug('conn  write res: %r', write_res)
            except Exception, e:
                connection.disconnect()
                raise e

            responses = []
            total = len(command_stack)
            cmds = iter(command_stack)

            try:
                while len(responses) < total:
                    data = yield async(connection.readline)()
                    if not data:
                        raise ResponseError('Not enough data after EXEC')
                    try:
                        cmd_line = cmds.next()
                        if self.transactional and cmd_line.cmd != 'EXEC':
                            response = yield self.process_data(connection, data, CmdLine('MULTI_PART'))
                        else:
                            response = yield self.process_data(connection, data, cmd_line)
                        responses.append(response)
                    except Exception,e :
                        responses.append(e)
            finally:
                self.connection_pool.return_connection(connection)

            def format_replies(cmd_lines, responses):
                results = []
                for cmd_line, response in zip(cmd_lines, responses):
                    try:
                        results.append(self.format_reply(cmd_line, response))
                    except Exception, e:
                        results.append(e)
                return results

            if self.transactional:
                command_stack = command_stack[:-1]
                responses = responses[-1] # actual data only from EXEC command
                            #FIXME:  assert all other responses to be 'QUEUED'
                results = format_replies(command_stack[1:], responses)
                log_blob.debug('on request %r pipe results %s', request, results)
            else:
                results = format_replies(command_stack, responses)

            ctx.ret_call(results)
