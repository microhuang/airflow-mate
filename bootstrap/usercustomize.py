# -*- coding: utf-8 -*-

# 让库变的更好

# usage: PYTHONPATH=......./bootstrap/


######################################################################
# fix json format

import json
from json.encoder import JSONEncoder

json._default_encoder = JSONEncoder(
    skipkeys=False,
    ensure_ascii=False,
    check_circular=True,
    allow_nan=True,
    indent=None,
    separators=None,
    default=None,
)

json_dumps = json.dumps
def wrapper_dumps(obj, *, skipkeys=False, ensure_ascii=True, check_circular=True,
        allow_nan=True, cls=None, indent=None, separators=None,
        default=None, sort_keys=False, **kw):
    ensure_ascii=False
    return json_dumps(obj, skipkeys=skipkeys, ensure_ascii=ensure_ascii, check_circular=check_circular,
        allow_nan=allow_nan, cls=cls, indent=indent, separators=separators,
        default=default, sort_keys=sort_keys, **kw)
json.dumps = wrapper_dumps
######################################################################


######################################################################
# pool.map

from multiprocessing import Pool
from multiprocessing.context import TimeoutError

def wrapper_map(self, func, iterable, chunksize=None):
    rets = []
    for i in iterable:
        ret = self.apply_async(func)
        rets.append(ret)
    res = []
    for ret in rets:
        res.append(ret.get(100))
    return res

# TODO:xxx

# Pool.map = wrapper_map
######################################################################


######################################################################
# global logging config

from contextlib import redirect_stderr, redirect_stdout, suppress, contextmanager
import io
import logging
import logging.config
import sys
import os

#with redirect_stdout(StreamLogWriter(log, logging.INFO)), 
#    redirect_stderr(StreamLogWriter(log, logging.WARN)):
'''
LOGGING_CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
                'airflow': {
                        'format': '[%(asctime)s] {{%(filename)s:%(lineno)d}} %(levelname)s - %(message)s'
                },
                'airflow_coloured': {
                        'format': '[%(blue)s%(asctime)s%(reset)s] {{%(blue)s%(filename)s:%(reset)s%(lineno)d}} %(log_color)s%(levelname)s%(reset)s - %(log_color)s%(message)s%(reset)s',
                        'class': 'airflow.utils.log.colored_log.CustomTTYColoredFormatter'
                }
        },
        'handlers': {
                'console': {
                        'class': 'airflow.utils.log.logging_mixin.RedirectStdHandler',
                        'formatter': 'airflow_coloured',
                        'stream': 'sys.stdout',
                        'filters': ['mask_secrets']
                },
        },
        'root': {
                'handlers': ['console'],
                'level': 'DEBUG',
                'filters': ['mask_secrets']
        }
}
'''
#logging.config.dictconfig(LOGGING_CONFIG)
#logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format='[%(asctime)s] %(pathname)s[line:%(lineno)d][%(levelname)s] %(message)s')


# pprint
# builtins.print -> logger

if not os.getenv('LOGPRING')=='OFF':
  from pprint import pprint
  import builtins
  sys_print = builtins.print
  def cus_print(*value, sep=' ', end='\n', file=sys.stdout, flush=True):
    if file is sys.stdout:
        #stdout = sys.stdout
        stream = io.StringIO()
        sys_print(*value, sep=sep, end=end, file=stream, flush=flush)
        output = stream.getvalue().rstrip()
        #sys_print(*value, sep=sep, end=end, file=file, flush=flush)
        # #{AIRFLOW_HOME/config/log_config.py}
        #logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format='[%(asctime)s] %(pathname)s[line:%(lineno)d][%(levelname)s:print] %(message)s')
        logger = logging.getLogger('print')
        '''
        logger = logging.Logger('abc')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(format, datefmt="%Y-%m-%d %H:%M:%S"))
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        '''
        frame = logging.currentframe
        logging.currentframe = lambda: sys._getframe(4)
        logger.debug(output)
        logging.currentframe = frame
    else:
        sys_print(*value, sep, end, file, flush)
  '''
  def cus_print(*value, sep=' ', end='\n', file=sys.stdout, flush=False):
    if not os.getenv('LOGPRING')=='OFF':
        sys.stdout = io.StringIO()
        sys_print(*value, sep, end, file, flush)
        output = sys.stdout.getvalue()
        sys.stdout = stdout
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format='%(pathname)s[line:%(lineno)d][%(levelname)s:print] %(message)s')
        logger = logging.getLogger()
        logger.debug(output)
    else:
        sys_print(*value, sep, end, file, flush)
  '''
  builtins.print = cus_print
  print = cus_print
######################################################################


######################################################################
# fix pool struct.error: 'i' format requires

import functools
import logging
import struct
import sys

logger = logging.getLogger()

def patch_mp_connection_bpo_17560():
    """Apply PR-10305 / bpo-17560 connection send/receive max size update
    See the original issue at https://bugs.python.org/issue17560 and
    https://github.com/python/cpython/pull/10305 for the pull request.
    This only supports Python versions 3.3 - 3.7, this function
    does nothing for Python versions outside of that range.
    """
    patchname = "Multiprocessing connection patch for bpo-17560"
    if not (3, 3) < sys.version_info < (3, 8):
        logger.info(
            patchname + " not applied, not an applicable Python version: %s",
            sys.version
        )
        return

    from multiprocessing.connection import Connection

    orig_send_bytes = Connection._send_bytes
    orig_recv_bytes = Connection._recv_bytes
    if (
        orig_send_bytes.__code__.co_filename == __file__
        and orig_recv_bytes.__code__.co_filename == __file__
    ):
        logger.info(patchname + " already applied, skipping")
        return

    @functools.wraps(orig_send_bytes)
    def send_bytes(self, buf):
        n = len(buf)
        if n > 0x7fffffff:
            pre_header = struct.pack("!i", -1)
            header = struct.pack("!Q", n)
            self._send(pre_header)
            self._send(header)
            self._send(buf)
        else:
            orig_send_bytes(self, buf)

    @functools.wraps(orig_recv_bytes)
    def recv_bytes(self, maxsize=None):
        buf = self._recv(4)
        size, = struct.unpack("!i", buf.getvalue())
        if size == -1:
            buf = self._recv(8)
            size, = struct.unpack("!Q", buf.getvalue())
        if maxsize is not None and size > maxsize:
            return None
        return self._recv(size)

    Connection._send_bytes = send_bytes
    Connection._recv_bytes = recv_bytes

    logger.info(patchname + " applied")

patch_mp_connection_bpo_17560()
######################################################################


######################################################################
# sqlalchemy log

#logger = logging.getLogger('sqlalchemy.engine')
#logger.setLevel(logging.DEBUG)
######################################################################


######################################################################
# catch fault

import faulthandler
from subprocess import Popen, PIPE
if os.getenv('FAULT_LOG'):
    FAULTHANDLER_TEE = Popen(f'tee -a '+os.getenv('FAULT_LOG'), shell=True, stdin=PIPE)
else:
    FAULTHANDLER_TEE = Popen(f'tee -a /tmp/python_fault.log', shell=True, stdin=PIPE)
faulthandler.enable(file=FAULTHANDLER_TEE.stdin)
######################################################################


import customize






