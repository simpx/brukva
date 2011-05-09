#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import sys
import types
from datetime import datetime
import traceback as tb
import time
from functools import partial
from collections import Iterable

from tornado.ioloop import IOLoop

import brukva
from brukva.adisp import process, async
async = partial(async, cbname='cb')
from brukva.exceptions import ResponseError, RequestError

log_format =  "[%(asctime)s][%(module)s] %(name)s: %(message)s"
import logging; logging.basicConfig(level=logging.DEBUG, format=log_format)

def callable(obj):
    return hasattr(obj, '__call__')

class CustomAssertionError(AssertionError):
    io_loop = None

    def __init__(self, *args, **kwargs):
        super(CustomAssertionError, self).__init__(*args, **kwargs)
        CustomAssertionError.io_loop.stop()

def handle_callback_exception(callback):
    (type, value, tb_) = sys.exc_info()
    raise type, value, tb_

class TornadoTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TornadoTestCase, self).__init__(*args, **kwargs)
        self.failureException = CustomAssertionError
        self.is_dirty = True

    def setUp(self):
        self.loop = IOLoop.instance()
        setattr(self.loop, 'handle_callback_exception', handle_callback_exception)
        CustomAssertionError.io_loop = self.loop
        self.client = brukva.Client(selected_db=9, io_loop=self.loop)
        self.client.connect()
        def on_flushdb_done(callbacks):
            self.is_dirty = False
            self.finish()
        self.client.flushdb(on_flushdb_done)
        self.start()

    def tearDown(self):
        del self.client
        self.is_dirty = True

    def expect(self, expected):
        source_line = '\n' + tb.format_stack()[-2]
        def callback(result):
            if isinstance(expected, Exception):
                self.assertTrue(isinstance(result, expected),
                    msg=source_line+'  Got:'+repr(result))
            if callable(expected):
                self.assertTrue(expected(result),
                    msg=source_line+'  Got:'+repr(result))
            else:
                self.assertEqual(expected, result,
                    msg=source_line+'  Got:'+repr(result))
        callback.__name__ = "expect_%s" % repr(expected)
        return callback

    def pexpect(self, expected_list, list_without_errors=True):
        if list_without_errors:
            expected_list = [(None, el) for el in expected_list]

        source_line = '\n' + tb.format_stack()[-2]
        def callback(result):
            if isinstance(result, Exception):
                self.fail('got exception %s' % result)

            self.assertEqual(len(result), len(expected_list) )
            for result, (exp_e, exp_d)  in zip(result, expected_list):
                if exp_e:
                    self.assertTrue( isinstance(result, exp_e),
                        msg=source_line+'  Error:'+repr(result))
                elif callable(exp_d):
                    self.assertTrue(exp_d(result),
                        msg=source_line+'  Got:'+repr(result))
                else:
                    self.assertEqual(result, exp_d,
                        msg=source_line+'  Got:'+repr(result))
        return callback

    def delayed(self, timeout, cb):
        self.loop.add_timeout(time.time()+timeout, cb)

    def finish(self, *args, **kwargs):
        self.loop.stop()

    def start(self):
        self.loop.start()

    def _run_plan(self, test_plan):
        self.results = []
        @process
        def run():
            """
                @test_plan: list
                One line of test_plan:
                [client method name, callbacks, args=None, kwargs=None]
            """
            while self.is_dirty:
                print 'skipping'
                time.sleep(0.01)
            try:
                for instruction in test_plan:
                    if not hasattr(instruction, '__call__'):
                        if len(instruction) == 2:
                            method_name,  callbacks = instruction
                            args = tuple()
                            kwargs = {}
                        elif len(instruction) == 3:
                            method_name, args, callbacks = instruction
                            kwargs = {}
                        else:
                            method_name, args, kwargs, callbacks = instruction

                        if args is None:
                            args = tuple()
                        elif not isinstance(args, types.TupleType):
                            args = (args,)

                        if kwargs is None:
                            kwargs = {}

                        if not isinstance(callbacks, Iterable):
                            callbacks = [callbacks]
                        else:
                            callbacks = list(callbacks)

                        kwargs.update({'callbacks': callbacks})
                        def instruction(cb):
                            callbacks.append(cb)
                            return getattr(self.client, method_name)(*args, **kwargs)

                    result = yield async(instruction)()
                    self.results.append(result)
            finally:
                self.finish()
        self.loop.add_callback(run)
        self.start()

class ServerCommandsTestCase(TornadoTestCase):
    def test_setget_unicode(self):
        self._run_plan([
            ('set', ('foo', u'бар'), self.expect(True)),
            ('get', 'foo', self.expect('бар')),
        ])

    def test_set(self):
        self._run_plan([
           ('set', ('foo', 'bar'), self.expect(True)),
        ])

    def test_setex(self):
        self._run_plan([
            ('setex', ('foo', 5, 'bar'), self.expect(True)),
            ('ttl', 'foo', self.expect(5)),
        ])

    def test_setnx(self):
        self._run_plan([
            ('setnx', ('a', 1), self.expect(True)),
            ('setnx', ('a', 0), self.expect(False)),
        ])

    def test_get(self):
        self._run_plan([
            ('set', ('foo', 'bar'), self.expect(True)),
            ('get', 'foo', self.expect('bar')),
        ])

    def test_randomkey(self):
        self._run_plan([
            ('set', ('a', 1), self.expect(True)),
            ('set', ('b', 1), self.expect(True)),
            ('randomkey', self.expect(lambda k: k in ['a', 'b'])),
            ('randomkey', self.expect(lambda k: k in ['a', 'b'])),
            ('randomkey', self.expect(lambda k: k in ['a', 'b'])),
        ])

    def test_substr(self):
        self._run_plan([
            ('set', ('foo', 'lorem ipsum'), self.expect(True)),
            ('substr', ('foo', 2, 4), self.expect('rem')),
        ])

    def test_append(self):
        self._run_plan([
            ('set', ('foo', 'lorem ipsum'), self.expect(True)),
            ('append', ('foo', ' bar'), self.expect(15)),
            ('get', ('foo'), self.expect('lorem ipsum bar')),
        ])

    def test_dbsize(self):
        self._run_plan([
            ('set', ('a', 1), self.expect(True)),
            ('set', ('b', 2), self.expect(True)),
            ('dbsize', self.expect(2)),
        ])

    def test_save(self):
        self._run_plan([
            ('save', (), self.expect(True)),
            lambda cb: cb(datetime.now().replace(microsecond=0)),
            ('lastsave', (), self.expect(lambda d: d >= self.results[-1]))
        ])

    def test_keys(self):
        self._run_plan([
            ('set', ('a', 1), self.expect(True)),
            ('set', ('b', 2), self.expect(True)),
            ('keys', '*', self.expect(['a', 'b'])),
            ('keys', '', self.expect([])),

            ('set', ('foo_a', 1), self.expect(True)),
            ('set', ('foo_b', 2), self.expect(True)),
            ('keys', 'foo_*', self.expect(['foo_a', 'foo_b'])),
        ])

    def test_expire(self):
        self._run_plan([
            ('set', ('a', 1), self.expect(True)),
            ('expire', ('a', 10), self.expect(True)),
            ('ttl', 'a', self.expect(10)),
        ])

    def test_type(self):
        self._run_plan([
            ('set', ('a', 1), self.expect(True)),
            ('type', 'a', self.expect('string')),
            ('rpush', ('b', 1), self.expect(True)),
            ('type', 'b', self.expect('list')),
            ('sadd', ('c', 1), self.expect(True)),
            ('type', 'c', self.expect('set')),
            ('hset', ('d', 'a', 1), self.expect(True)),
            ('type', 'd', self.expect('hash')),
            ('zadd', ('e', 1, 1), self.expect(True)),
            ('type', 'e', self.expect('zset')),
        ])

    def test_rename(self):
        self._run_plan([
            ('set', ('a', 1), self.expect(True)),
            ('rename', ('a', 'b'), self.expect(True)),
            ('set', ('c', 1), self.expect(True)),
            ('renamenx', ('c', 'b'), self.expect(False)),
        ])

    def _test_move(self):
        pass
        #FIXME: since removing select command
        #self.client.select(8, self.expect(True))
        #self.client.delete('a', self.expect(True))
        #self.client.select(9, self.expect(True))
        #self.client.set('a', 1, self.expect(True))
        #self.client.move('a', 8, self.expect(True))
        #self.client.exists('a', self.expect(False))
        #self.client.select(8, self.expect(True))
        #self.client.get('a', [self.expect('1'), self.finish])
        #self.start()

    def test_exists(self):
        self._run_plan([
            ('set', ('a', 1), self.expect(True)),
            ('exists', 'a', self.expect(True)),
            ('delete', 'a', self.expect(True)),
            ('exists', 'a', self.expect(False)),
        ])

    def test_mset_mget(self):
        self._run_plan([
            ('mset', {'a': 1, 'b': 2}, self.expect(True)),
            ('get', 'a', self.expect('1')),
            ('get', 'b', self.expect('2')),
            ('mget', ['a', 'b'], self.expect(['1', '2'])),
        ])

    def test_msetnx(self):
        self._run_plan([
            ('msetnx', {'a': 1, 'b': 2}, self.expect(True)),
            ('msetnx', {'b': 3, 'c': 4}, self.expect(False)),
        ])

    def test_getset(self):
        self._run_plan([
            ('set', ('a', 1), self.expect(True)),
            ('getset', ('a', 2), self.expect('1')),
            ('get', 'a', self.expect('2')),
        ])

    def test_hash(self):
        self._run_plan([
            ('hmset', ('foo', {'a': 1, 'b': 2}), self.expect(True)),
            ('hgetall', 'foo', self.expect({'a': '1', 'b': '2'})),
            ('hdel', ('foo', 'a'), self.expect(True)),
            ('hgetall', 'foo', self.expect({'b': '2'})),
            ('hget', ('foo', 'a'), self.expect('')),
            ('hget', ('foo', 'b'), self.expect('2')),
            ('hlen', 'foo', self.expect(1)),
            ('hincrby', ('foo', 'b', 3), self.expect(5)),
            ('hkeys', 'foo', self.expect(['b'])),
            ('hvals', 'foo', self.expect(['5'])),
            ('hset', ('foo', 'a', 1), self.expect(True)),
            ('hmget', ('foo', ['a', 'b']), self.expect({'a': '1', 'b': '5'})),
            ('hexists', ('foo', 'b'), self.expect(True)),
        ])

    def test_incrdecr(self):
        self._run_plan([
            ('incr', 'foo', self.expect(1)),
            ('incrby', ('foo', 10), self.expect(11)),
            ('decr', 'foo', self.expect(10)),
            ('decrby', ('foo', 10), self.expect(0)),
            ('decr', 'foo', self.expect(-1)),
        ])

    def test_ping(self):
        self._run_plan([
            ('ping', self.expect(True)),
        ])

    def test_lists(self):
        self._run_plan([
            ('lpush', ('foo', 1), self.expect(True)),
            ('llen', 'foo', self.expect(1)),
            ('lrange', ('foo', 0, -1), self.expect(['1'])),
            ('rpop', 'foo', self.expect('1')),
            ('llen', 'foo', self.expect(0)),
        ])
    def test_brpop(self):
        self._run_plan([
            ('lpush', ('foo', 'ab'), self.expect(True)),
            ('lpush', ('bar', 'cd'), self.expect(True)),
            ('brpop', (['foo', 'bar'], 1), self.expect({'foo':'ab'})),
            ('llen', 'foo', self.expect(0)),
            ('llen', 'bar', self.expect(1)),
            ('brpop', (['foo', 'bar'], 1), self.expect({'bar':'cd'})),
        ])

    def test_brpoplpush(self):
        self._run_plan([
            ('lpush', ('foo', 'ab'), self.expect(True)),
            ('lpush', ('bar', 'cd'), self.expect(True)),
            ('lrange', ('foo', 0, -1), self.expect(['ab'])),
            ('lrange', ('bar', 0, -1), self.expect(['cd'])),
            ('brpoplpush', ('foo', 'bar'), self.expect('ab')),
            ('llen', 'foo', self.expect(0)),
            ('lrange', ('bar', 0, -1), self.expect(['ab', 'cd'])),
        ])
    def test_sets(self):
        self._run_plan([
            ('smembers', 'foo', self.expect(set())),
            ('sadd', ('foo', 'a'), self.expect(1)),
            ('sadd', ('foo', 'b'), self.expect(1)),
            ('sadd', ('foo', 'c'), self.expect(1)),
            ('srandmember', 'foo', self.expect(lambda x: x in ['a', 'b', 'c'])),
            ('scard', 'foo', self.expect(3)),
            ('srem', ('foo', 'a'), self.expect(True)),
            ('smove', ('foo', 'bar', 'b'), self.expect(True)),
            ('smembers', 'bar', self.expect(set(['b']))),
            ('sismember', ('foo', 'c'), self.expect(True)),
            ('spop',    'foo', self.expect('c')),
        ])

    def test_sets2(self):
        self._run_plan([
            ('sadd', ('foo', 'a'), self.expect(1)),
            ('sadd', ('foo', 'b'), self.expect(1)),
            ('sadd', ('foo', 'c'), self.expect(1)),
            ('sadd', ('bar', 'b'), self.expect(1)),
            ('sadd', ('bar', 'c'), self.expect(1)),
            ('sadd', ('bar', 'd'), self.expect(1)),

            ('sdiff', ['foo', 'bar'], self.expect(set(['a']))),
            ('sdiff', ['bar', 'foo'], self.expect(set(['d']))),
            ('sinter', ['foo', 'bar'], self.expect(set(['b', 'c']))),
            ('sunion', ['foo', 'bar'], self.expect(set(['a', 'b', 'c', 'd']))),
        ])

    def test_sets3(self):
        self._run_plan([
            ('sadd', ('foo', 'a'), self.expect(1)),
            ('sadd', ('foo', 'b'), self.expect(1)),
            ('sadd', ('foo', 'c'), self.expect(1)),
            ('sadd', ('bar', 'b'), self.expect(1)),
            ('sadd', ('bar', 'c'), self.expect(1)),
            ('sadd', ('bar', 'd'), self.expect(1)),

            ('sdiffstore', (['foo', 'bar'], 'zar'), self.expect(1)),
            ('smembers', 'zar', self.expect(set(['a']))),
            ('delete', 'zar', self.expect(True)),

            ('sinterstore', (['foo', 'bar'], 'zar'), self.expect(2)),
            ('smembers', 'zar', self.expect(set(['b', 'c']))),
            ('delete', 'zar', self.expect(True)),

            ('sunionstore', (['foo', 'bar'], 'zar'), self.expect(4)),
            ('smembers', 'zar', self.expect(set(['a', 'b', 'c', 'd']))),
        ])

    def test_zsets(self):
        self._run_plan([
            ('zadd', ('foo', 1, 'a'), self.expect(1)),
            ('zadd', ('foo', 2, 'b'), self.expect(1)),
            ('zscore', ('foo', 'a'), self.expect(1)),
            ('zscore', ('foo', 'b'), self.expect(2)),
            ('zrank', ('foo', 'a'), self.expect(0)),
            ('zrank', ('foo', 'b'), self.expect(1)),
            ('zrevrank', ('foo', 'a'), self.expect(1)),
            ('zrevrank', ('foo', 'b'), self.expect(0)),
            ('zincrby', ('foo', 'a', 1), self.expect(2)),
            ('zincrby', ('foo', 'b', 1), self.expect(3)),
            ('zscore', ('foo', 'a'), self.expect(2)),
            ('zscore', ('foo', 'b'), self.expect(3)),
            ('zrange', ('foo', 0, -1, True), self.expect([('a', 2.0), ('b', 3.0)])),
            ('zrange', ('foo', 0, -1, False), self.expect(['a', 'b'])),
            ('zrevrange', ('foo', 0, -1, True), self.expect([('b', 3.0), ('a', 2.0)])),
            ('zrevrange', ('foo', 0, -1, False), self.expect(['b', 'a'])),
            ('zcard', 'foo', self.expect(2)),
            ('zadd', ('foo', 3.5, 'c'), self.expect(1)),
            ('zrangebyscore', ('foo', '-inf', '+inf', None, None, False), self.expect(['a', 'b', 'c'])),
            ('zrangebyscore', ('foo', '2.1', '+inf', None, None, True), self.expect([('b', 3.0), ('c', 3.5)])),
            ('zrangebyscore', ('foo', '-inf', '3.0', 0, 1, False), self.expect(['a'])),
            ('zrangebyscore', ('foo', '-inf', '+inf', 1, 2, False), self.expect(['b', 'c'])),

            ('delete', 'foo', self.expect(True)),
            ('zadd', ('foo', 1, 'a'), self.expect(1)),
            ('zadd', ('foo', 2, 'b'), self.expect(1)),
            ('zadd', ('foo', 3, 'c'), self.expect(1)),
            ('zadd', ('foo', 4, 'd'), self.expect(1)),
            ('zremrangebyrank', ('foo', 2, 4), self.expect(2)),
            ('zremrangebyscore', ('foo', 0, 2), self.expect(2)),
        ])

    def test_zsets2(self):
        self._run_plan([
            ('zadd', ('a', 1, 'a1'), self.expect(1)),
            ('zadd', ('a', 1, 'a2'), self.expect(1)),
            ('zadd', ('a', 1, 'a3'), self.expect(1)),
            ('zadd', ('b', 2, 'a1'), self.expect(1)),
            ('zadd', ('b', 2, 'a3'), self.expect(1)),
            ('zadd', ('b', 2, 'a4'), self.expect(1)),
            ('zadd', ('c', 6, 'a1'), self.expect(1)),
            ('zadd', ('c', 5, 'a3'), self.expect(1)),
            ('zadd', ('c', 4, 'a4'), self.expect(1)),

            # ZINTERSTORE
            # sum, no weight
            ('zinterstore', ('z', ['a', 'b', 'c']), self.expect(2)),
            ('zrange', ('z', 0, -1), dict(with_scores=True),
                self.expect([('a3', 8), ('a1', 9),])),

            # max, no weight
            ('zinterstore', ('z', ['a', 'b', 'c']), dict(aggregate='MAX'), self.expect(2)),
            ('zrange', ('z', 0, -1), dict(with_scores=True),
                self.expect([('a3', 5), ('a1', 6),])),

            # with weight
            ('zinterstore', ('z', {'a': 1, 'b': 2, 'c': 3}), self.expect(2)),
            ('zrange', ('z', 0, -1), dict(with_scores=True),
                self.expect([('a3', 20), ('a1', 23), ])),

            # ZUNIONSTORE
            # sum, no weight
            ('zunionstore', ('z', ['a', 'b', 'c']), self.expect(4)),
            ('zrange', ('z', 0, -1), dict(with_scores=True),
                self.expect([('a2', 1), ('a4', 6), ('a3', 8), ('a1', 9), ])
            ),
            # max, no weight
            ('zunionstore', ('z', ['a', 'b', 'c']), dict(aggregate='MAX'), self.expect(4)),
            ('zrange', ('z', 0, -1), dict(with_scores=True),
                self.expect([('a2', 1), ('a4', 4), ('a3', 5), ('a1', 6), ])),


            # with weight
            ('zunionstore', ('z', {'a': 1, 'b': 2, 'c': 3}), self.expect(4)),
            ('zrange', ('z', 0, -1), dict(with_scores=True),
                self.expect([('a2', 1), ('a4', 16), ('a3', 20), ('a1', 23), ])),
    ])

    def test_long_zset(self):
        NUM = 200
        long_list = map(str, xrange(0, NUM))
        test_plan = [
            ('zadd', ('foobar', i, i), self.expect(1))
            for i in long_list
        ]
        test_plan.append(
            ('zrange', ('foobar', 0, NUM), dict(with_scores=False), self.expect(long_list))
        )
        self._run_plan(test_plan)

    def test_sort(self):
        def make_list(key, items, expect_value=True):
            return [('delete', key, self.expect(expect_value))] + \
                            [('rpush', (key, i), []) for i in items]

        test_plan = [
            ('sort', 'a', self.expect([])),
        ]
        test_plan.extend(make_list('a', '3214', False))

        test_plan.extend([
            ('sort', 'a', self.expect(['1', '2', '3', '4'])),
            ('sort', 'a', dict(start=1, num=2), self.expect(['2', '3'])),
            ('set', ('score:1', 8), self.expect(True)),
            ('set', ('score:2', 3), self.expect(True)),
            ('set', ('score:3', 5), self.expect(True)),
        ])
        test_plan.extend(make_list('a_values', '123', False))


        test_plan.extend([
            ('sort', 'a_values', dict(by='score:*'), self.expect(['2', '3', '1'])),
            ('set', ('user:1', 'u1'), self.expect(True)),
            ('set', ('user:2', 'u2'), self.expect(True)),
            ('set', ('user:3', 'u3'), self.expect(True)),
        ])

        test_plan.extend(make_list('a', '231'))
        test_plan.extend([
            ('sort', 'a', dict(get='user:*'), self.expect(['u1', 'u2', 'u3'])),
        ])

        test_plan.extend(make_list('a', '231'))
        test_plan.extend([
            ('sort', 'a', dict(desc=True), self.expect(['3', '2', '1'])),
        ])


        test_plan.extend(make_list('a', 'ecdba'))
        test_plan.extend([
            ('sort', 'a', dict(alpha=True), self.expect(['a', 'b', 'c', 'd', 'e'])),
        ])

        test_plan.extend(make_list('a', '231'))
        test_plan.extend([
            ('sort', 'a', dict(store='sorted_values'), self.expect(3)),
            ('lrange', ('sorted_values', 0, -1), self.expect(['1', '2', '3'])),

            ('set', ('user:1:username', 'zeus'), []),
            ('set', ('user:2:username', 'titan'), []),
            ('set', ('user:3:username', 'hermes'), []),
            ('set', ('user:4:username', 'hercules'), []),
            ('set', ('user:5:username', 'apollo'), []),
            ('set', ('user:6:username', 'athena'), []),
            ('set', ('user:7:username', 'hades'), []),
            ('set', ('user:8:username', 'dionysus'), []),
            ('set', ('user:1:favorite_drink', 'yuengling'), []),
            ('set', ('user:2:favorite_drink', 'rum'), []),
            ('set', ('user:3:favorite_drink', 'vodka'), []),
            ('set', ('user:4:favorite_drink', 'milk'), []),
            ('set', ('user:5:favorite_drink', 'pinot noir'), []),
            ('set', ('user:6:favorite_drink', 'water'), []),
            ('set', ('user:7:favorite_drink', 'gin'), []),
            ('set', ('user:8:favorite_drink', 'apple juice'), []),
        ])

        test_plan.extend(make_list('gods', '12345678', False))
        test_plan.extend([
            ('sort', 'gods', dict(
                             start=2,
                             num=4,
                             by='user:*:username',
                             get='user:*:favorite_drink',
                             desc=True,
                             alpha=True,
                             store='sorted'),
                self.expect(4)),
            ('lrange', ('sorted', 0, -1),
                self.expect(['vodka', 'milk', 'gin', 'apple juice', ])
            )
        ])
        self._run_plan(test_plan)


class PipelineTestCase(TornadoTestCase):
    ### Pipeline ###
    def test_pipe_simple(self):
        pipe = self.client.pipeline()
        pipe.set('foo', '123')
        pipe.set('bar', '456')
        pipe.mget( ('foo', 'bar') )

        pipe.execute([self.pexpect([True , True, ['123', '456',]]), self.finish])
        self.start()

    def test_pipe_multi(self):
        pipe = self.client.pipeline(transactional=True)
        pipe.set('foo', '123')
        pipe.set('bar', '456')
        pipe.mget( ('foo', 'bar') )

        pipe.execute([self.pexpect([True , True, ['123', '456',]]), self.finish])
        self.start()

    def test_pipe_error(self):
        pipe = self.client.pipeline()
        pipe.sadd('foo', 1)
        pipe.sadd('foo', 2)
        pipe.rpop('foo')

        pipe.execute([self.pexpect([(None, True), (None, True), (ResponseError, None)], False), self.finish])
        self.start()

    def test_two_pipes(self):
        pipe = self.client.pipeline()

        pipe.rpush('foo', '1')
        pipe.rpush('foo', '2')
        pipe.lrange('foo', 0, -1)
        pipe.execute([self.pexpect([True, 2, ['1', '2']]) ] )

        pipe.sadd('bar', '3')
        pipe.sadd('bar', '4')
        pipe.smembers('bar')
        pipe.scard('bar')
        pipe.execute([self.pexpect([1, 1, set(['3', '4']), 2]), self.finish])

        self.start()

    def test_pipe_watch(self):
        pipe = self.client.pipeline(transactional=True)
        pipe.get('bar')

        self._run_plan([
            ('watch', 'foo', self.expect(True)),
            ('set', ('bar', 'zar'), self.expect(True)),
            lambda cb: pipe.execute([self.pexpect(['zar',]), cb]),
        ])

    def test_pipe_watch2(self):
        pipe = self.client.pipeline(transactional=True)
        pipe.get('foo')

        self._run_plan([
            ('set', ('foo', 'bar'), self.expect(True)),
            ('watch', 'foo', self.expect(True)),
            ('set', ('foo', 'zar'), self.expect(True)),
            lambda cb: pipe.execute([self.pexpect([]), cb]),
        ])

    def _test_pipe_unwatch(self):
        self.client.set('foo', 'bar', self.expect(True))
        self.client.watch('foo', self.expect(True))
        self.client.set('foo', 'zar', self.expect(True))
        self.client.unwatch(callbacks=self.expect(True))
        pipe = self.client.pipeline(transactional=True)
        pipe.get('foo')
        pipe.execute([self.pexpect(['zar']), self.finish])
        self.start()

    def test_pipe_zsets(self):
        pipe = self.client.pipeline(transactional=True)

        pipe.zadd('foo', 1, 'a')
        pipe.zadd('foo', 2, 'b')
        pipe.zscore('foo', 'a')
        pipe.zscore('foo', 'b' )
        pipe.zrank('foo', 'a', )
        pipe.zrank('foo', 'b', )

        pipe.zrange('foo', 0, -1, True )
        pipe.zrange('foo', 0, -1, False)

        pipe.execute([
            self.pexpect([
                1, 1,
                1, 2,
                0, 1,
                [('a', 1.0), ('b', 2.0)],
                ['a', 'b'],
            ]),
            self.finish,
        ])
        self.start()

    def test_pipe_zsets2(self):
        pipe = self.client.pipeline(transactional=False)

        pipe.zadd('foo', 1, 'a')
        pipe.zadd('foo', 2, 'b')
        pipe.zscore('foo', 'a')
        pipe.zscore('foo', 'b' )
        pipe.zrank('foo', 'a', )
        pipe.zrank('foo', 'b', )

        pipe.zrange('foo', 0, -1, True )
        pipe.zrange('foo', 0, -1, False)

        pipe.execute([
            self.pexpect([
                1, 1,
                1, 2,
                0, 1,
                [('a', 1.0), ('b', 2.0)],
                ['a', 'b'],
            ]),
            self.finish,
        ])
        self.start()

    def test_pipe_hsets(self):
        pipe = self.client.pipeline(transactional=True)
        pipe.hset('foo', 'bar', 'aaa')
        pipe.hset('foo', 'zar', 'bbb')
        pipe.hgetall('foo')

        pipe.execute([
            self.pexpect([
                True,
                True,
                {'bar': 'aaa', 'zar': 'bbb'}
            ]),
            self.finish,
        ])
        self.start()

    def test_pipe_hsets2(self):
        pipe = self.client.pipeline(transactional=False)
        pipe.hset('foo', 'bar', 'aaa')
        pipe.hset('foo', 'zar', 'bbb')
        pipe.hgetall('foo')

        pipe.execute([
            self.pexpect([
                True,
                True,
                {'bar': 'aaa', 'zar': 'bbb'}
            ]),
            self.finish,
        ])
        self.start()

    def test_response_error(self):
        self.client.set('foo', 'bar', self.expect(True))
        self.client.llen('foo', [self.expect(ResponseError), self.finish])
        self.start()

class PubSubTestCase(TornadoTestCase):
    def setUp(self, *args, **kwargs):
        super(PubSubTestCase, self).setUp(*args, **kwargs)
        self.client2 = brukva.Client(selected_db=9, io_loop=self.loop)
        self.client2.connection.connect()
        #self.client2.select(9)

    def tearDown(self):
        super(PubSubTestCase, self).tearDown()
        #del self.client2

    def assert_pubsub(self, msg, kind, channel, body):
        self.assertEqual(msg.kind, kind)
        self.assertEqual(msg.channel, channel)
        self.assertEqual(msg.body, body)

    def test_pub_sub(self):
        def on_recv(msg):
            self.assert_pubsub(msg, 'message', 'foo', 'bar')

        def on_subscription(msg):
            self.assert_pubsub(msg, 'subscribe', 'foo', 1)
            self.client2.listen(on_recv)

        self.client2.subscribe('foo', on_subscription)
        self.delayed(0.1, lambda:
            self.client2.set('gtx', 'rd', self.expect(RequestError)))
        self.delayed(0.2, lambda:
            self.client.publish('foo', 'bar',
                lambda *args: self.delayed(0.4, self.finish))
        )
        self.start()

    def test_unsubscribe(self):
        global c
        c = 0
        def on_recv(msg):
            if isinstance(msg, Exception):
                self.fail('Got unexpected exception: %s' % msg)

            global c
            if c == 0:
                self.assert_pubsub(msg, 'message', 'foo', 'bar')
            elif c == 1:
                self.assert_pubsub(msg, 'message', 'so', 'much')
            c += 1

        def on_subscription(msg):
            self.assert_pubsub(msg, 'subscribe', 'foo', 1)
            self.client2.listen(on_recv)

        self.client2.subscribe('foo', on_subscription)
        self.delayed(0.1, lambda: self.client.publish('foo', 'bar'))
        self.delayed(0.2, lambda: self.client2.subscribe('so',))
        self.delayed(0.3, lambda: self.client2.unsubscribe('foo'))
        self.delayed(0.4, lambda: self.client.publish('so', 'much'))
        self.delayed(0.5, lambda: self.client2.unsubscribe('so'))
        self.delayed(0.6, lambda: self.client2.set('zar', 'xar', [self.expect(True), self.finish]))
        self.start()


    def test_pub_sub_disconnect(self):
        def on_recv(msg):
            self.assertIsInstance(msg, brukva.exceptions.ConnectionError)

        def on_subscription(msg):
            self.assertEqual(msg.kind, 'subscribe')
            self.assertEqual(msg.channel, 'foo')
            self.assertEqual(msg.body, 1)
            self.client2.listen(on_recv)

        def on_publish(value):
            self.assertIsNotNone(value)

        self.client2.subscribe('foo', on_subscription)
        self.delayed(0.2, lambda: self.client2.disconnect())
        self.delayed(0.2, lambda: self.client.publish('foo', 'zar', on_publish))
        self.delayed(0.4, lambda: self.client2.publish('foo', 'bar', on_publish))
        self.delayed(0.5, self.finish)
        self.start()

    def test_generator_exit(self):
        def on_recv(msg):
            print msg
            #if msg.body == 'b':
                #raise Exception('Oops')

        def on_subs(msg):
            self.client2.listen(on_recv)

        def dl():
            del self.client2

        self.client2.subscribe('foo', on_subs)
        self.delayed(0.2, lambda: self.client.publish('foo', 'a'))
        self.delayed(0.2, lambda: self.client.publish('foo', 'b'))
        self.delayed(0.3, lambda: self.client.publish('foo', 'c'))
        self.delayed(0.2, dl)
        self.delayed(0.5, self.finish)
        self.start()


class AsyncWrapperTestCase(TornadoTestCase):
    def test_wrapper(self):
        @process
        def simulate(client, callbacks):
            res = yield client.async.set('foo1', 'bar')
            self.assertEquals(res, True)
            res = yield client.async.set('foo2', 'zar')
            self.assertEquals(res, True)
            r1, r2 = yield [client.async.get('foo1'), client.async.get('foo2')]
            self.assertEquals(r1, 'bar')
            self.assertEquals(r2, 'zar')
            callbacks(None)
        self.loop.add_callback(lambda: simulate(self.client, self.finish))
        self.start()


class ReconnectTestCase(TornadoTestCase):
    def test_redis_timeout(self):
        self._run_plan([
            ('set', ('foo', 'bar'), self.expect(True)),
            lambda cb: self.delayed(10, lambda:
                self.client.get('foo', [
                    self.expect('bar'),
                    cb
                ])
            )
        ])

    def test_redis_timeout_with_pipe(self):
        pipe = self.client.pipeline(transactional=True)
        pipe.get('foo')

        self._run_plan([
            ('set', ('foo', 'bar'), self.expect(True)),
            lambda cb: self.delayed(10, lambda:
                pipe.execute([
                    self.pexpect([
                        'bar',
                    ]),
                    cb,
                ])
            )
        ])
if __name__ == '__main__':
    unittest.main()
