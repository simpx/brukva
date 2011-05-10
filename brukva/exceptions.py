#!/usr/bin/env python
# -*- coding: utf-8 -*-

class RedisError(Exception):
    pass


class ConnectionError(RedisError):
    pass


class RequestError(RedisError):
    def __init__(self, message, cmd_line=None):
        self.message = message
        self.cmd_line = cmd_line

    def __repr__(self):
        if self.cmd_line:
            return 'RequestError (on %s [%s, %s]): %s' % (self.cmd_line.cmd, self.cmd_line.args, self.cmd_line.kwargs, self.message)
        return 'RequestError: %s' % self.message

    __str__ = __repr__



class ResponseError(RedisError):
    def __init__(self, message, cmd_line=None):
        self.message = message
        self.cmd_line = cmd_line

    def __repr__(self):
        if self.cmd_line:
            return '%s (on %s [%s, %s]): %s' % (self.__class__, self.cmd_line.cmd, self.cmd_line.args, self.cmd_line.kwargs, self.message)
        return  '%s: %s' % (self.__class__, self.message)

    __str__ = __repr__


class InternalRedisError(ResponseError):
    pass

class InvalidResponse(RedisError):
    pass
