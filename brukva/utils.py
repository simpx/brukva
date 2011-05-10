# -*- coding: utf-8 -*-

import traceback
from collections import Iterable
import logging

log = logging.getLogger('brukva.utils')

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
