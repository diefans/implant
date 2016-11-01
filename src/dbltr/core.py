"""The core module is transfered to the remote process and will bootstrap pipe communication."""
import threading
import asyncio
import json
import base64
import concurrent
import functools
import hashlib
import importlib.abc
import importlib.machinery
import inspect
import logging
import logging.config
import os
import pickle
import signal
import struct
import sys
import time
import traceback
import types
import weakref
import zlib
from collections import defaultdict, namedtuple

import pkg_resources

pkg_environment = pkg_resources.Environment()

logger = logging.getLogger(__name__)


_uid = threading.local()
def create_uid():
    logger.debug("create_uid")
    if getattr(_uid, "uid", None) is None:
        thread = threading.current_thread()
        logger.debug("new uid: %s", thread)
        _uid.tid = thread.ident
        _uid.id = id(thread)
        _uid.uid = 0

    _uid.uid += 1
    try:
        uid = (time.time(), _uid.id, _uid.tid, _uid.uid)

        return uid

    finally:
        logger.debug("finish: %s", uid)


class Uid(namedtuple('Uid', ('time', 'id', 'tid', 'seq'))):

    fmt = "!dQQQ"

    def __new__(cls, time=None, id=None, tid=None, seq=None, uid_bytes=None):
        all_args = [time, id, tid, seq].count(None) == 0

        if not all_args:
            if uid_bytes:
                time, id, tid, seq = struct.unpack(cls.fmt, uid_bytes)
            else:
                time, id, tid, seq = create_uid()

        return super(Uid, cls).__new__(cls, time, id, tid, seq)

    @property
    def bytes(self):
        uid_bytes = struct.pack(self.fmt, self.time, self.id, self.tid, self.seq)
        return uid_bytes

    def __str__(self):
        return "{0.time}/{0.id}/{0.tid}/{0.seq}>".format(self)


class aenumerate:
    def __init__(self, aiterable, start=0):
        self._ait = aiterable
        self._i = start

    async def __aiter__(self):
        return self

    async def __anext__(self):
        val = await self._ait.__anext__()
        try:
            return self._i, val
        finally:
            self._i += 1


class reify:

    """Create a property and cache the result."""

    def __init__(self, wrapped):
        self.wrapped = wrapped
        functools.update_wrapper(self, wrapped)

    def __get__(self, inst, objtype=None):
        if inst is None:
            return self
        val = self.wrapped(inst)
        setattr(inst, self.wrapped.__name__, val)
        return val


def create_module(module_name, is_package=False):
    """Create an empty module and all its parent packages."""
    if module_name not in sys.modules:
        package_name, _, module_attr = module_name.rpartition('.')

        module = types.ModuleType(module_name)
        module.__file__ = '<memory>'

        if package_name:
            package = create_module(package_name, is_package=True)
            # see https://docs.python.org/3/reference/import.html#submodules
            setattr(package, module_attr, module)

        if is_package:
            module.__package__ = module_name
            module.__path__ = []

        else:
            module.__package__ = package_name

        sys.modules[module_name] = module

    return sys.modules[module_name]


class IoQueues:

    """Just to keep send and receive queues together."""

    def __init__(self, send=None):
        if send is None:
            send = asyncio.Queue()

        self.send = send
        self.receive = defaultdict(asyncio.Queue)

    def __getitem__(self, channel_name):
        return self.receive[channel_name]

    def __delitem__(self, channel_name):
        try:
            del self.receive[channel_name]

        except KeyError:
            # we ignore missing queues, since not all commands use it
            pass


class Incomming(asyncio.StreamReader):

    """A context for an incomming pipe."""

    def __init__(self, *, pipe=sys.stdin):
        super(Incomming, self).__init__()

        self.pipe = os.fdopen(pipe) if isinstance(pipe, int) else pipe

    async def __aenter__(self):
        protocol = asyncio.StreamReaderProtocol(self)

        await asyncio.get_event_loop().connect_read_pipe(
            lambda: protocol,
            self.pipe,
        )
        return self

    async def __aexit__(self, exc_type, value, tb):
        self._transport.close()

    async def readexactly(self, n):
        """Read exactly n bytes from the stream.

        This is a short and faster implementation the original one
        (see of https://github.com/python/asyncio/issues/394).

        """
        buffer = bytearray()

        missing = n

        while missing:
            if not self._buffer:
                await self._wait_for_data('readexactly')

            if self._eof or not self._buffer:
                raise asyncio.IncompleteReadError(bytes(buffer), n)

            length = min(len(self._buffer), missing)
            buffer.extend(self._buffer[:length])
            del self._buffer[:length]

            missing -= length

            self._maybe_resume_transport()

        return buffer


class ShutdownOnConnectionLost(asyncio.streams.FlowControlMixin):

    """Send SIGHUP when connection is lost."""

    def connection_lost(self, exc):
        """Shutdown process."""
        super(ShutdownOnConnectionLost, self).connection_lost(exc)

        # XXX FIXME is not called
        logger.warning("Connection lost! Shutting down...")
        os.kill(os.getpid(), signal.SIGHUP)


class Outgoing:

    """A context for an outgoing pipe."""

    def __init__(self, *, pipe=sys.stdout, shutdown=False):
        self.pipe = os.fdopen(pipe) if isinstance(pipe, int) else pipe
        self.transport = None
        self.shutdown = shutdown

    async def __aenter__(self):
        self.transport, protocol = await asyncio.get_event_loop().connect_write_pipe(
            ShutdownOnConnectionLost if self.shutdown else asyncio.streams.FlowControlMixin,
            self.pipe
        )
        writer = asyncio.streams.StreamWriter(self.transport, protocol, None, asyncio.get_event_loop())

        return writer

    async def __aexit__(self, exc_type, value, tb):
        self.transport.close()


async def send_outgoing_queue(queue, pipe=sys.stdout):
    """Write data from queue to stdout."""
    async with Outgoing(pipe=pipe, shutdown=True) as writer:
        while True:
            data = await queue.get()
            writer.write(data)
            await writer.drain()
            queue.task_done()


def split_data(data, size=1024):
    """A generator to yield splitted data."""
    data_view = memoryview(data)
    data_len = len(data_view)
    start = 0

    while start < data_len:
        end = min(start + size, data_len)

        yield data_view[start:end]

        start = end


class ChunkFlags(dict):

    _masks = {
        'eom': (1, 0, int, bool),
        'stop_iter': (1, 1, int, bool),
        'send_ack': (1, 2, int, bool),
        'recv_ack': (1, 3, int, bool),
        'compression': (1, 4, int, bool),
    }

    def __init__(self, *, send_ack=False, recv_ack=False, eom=False, stop_iter=False, compression=False):
        self.__dict__ = self
        super(ChunkFlags, self).__init__()

        self.eom = eom
        self.stop_iter = stop_iter
        self.send_ack = send_ack
        self.recv_ack = recv_ack
        self.compression = compression

    def encode(self):
        def _mask_value(k, v):
            mask, shift, enc, _ = self._masks[k]
            return (enc(v) & mask) << shift

        return sum(_mask_value(k, v) for k, v in self.items())

    @classmethod
    def decode(cls, value):
        def _unmask_value(k, v):
            mask, shift, _, dec = v
            return dec((value >> shift) & mask)

        return cls(**{k: _unmask_value(k, v) for k, v in cls._masks.items()})


HEADER_FMT = '!32sQHI'
HEADER_SIZE = struct.calcsize(HEADER_FMT)


class Channel:

    """Channel provides means to send and receive messages."""

    chunk_size = 0x8000

    acknowledgements = weakref.WeakValueDictionary()
    """Global acknowledgment futures distinctive by uuid."""

    def __init__(self, name=None, *, io_queues=None):
        """Initialize the channel.

        :param name: the channel name
        :param io_queues: the queues to send and receive with

        """
        self.name = name
        self.io_queues = io_queues or IoQueues()
        self.io_outgoing = self.io_queues.send
        self.io_incomming = self.io_queues[self.name]

    def __repr__(self):
        return '<{0.name} {in_size} / {out_size}>'.format(
            self,
            in_size=self.io_incomming.qsize(),
            out_size=self.io_outgoing.qsize(),
        )

    def __await__(self):
        """Receive the next message in this channel."""
        async def coro():
            msg = await self.io_incomming.get()

            try:
                return msg
            finally:
                self.io_incomming.task_done()

        return coro().__await__()

    async def __aiter__(self):
        return self

    async def __anext__(self):
        # TODO use iterator and stop_iter header to raise AsyncStopIteration
        data = await self
        if isinstance(data, StopAsyncIteration):
            raise data

        return data

    def stop_iteration(self):       # noqa
        class context:
            async def __aenter__(ctx):      # noqa
                return self

            async def __aexit__(ctx, *args):        # noqa
                await self.send(StopAsyncIteration())

        return context()

    @staticmethod
    def _encode_header(uid, channel_name=None, data=None, *, flags=None):
        """Create chunk header.

        [header length = 30 bytes]
        [!16s]     [!Q: 8 bytes]                     [!H: 2 bytes]        [!I: 4 bytes]
        {data uuid}{flags: compression|eom|stop_iter}{channel_name length}{data length}{channel_name}{data}

        """
        assert isinstance(uid, Uid), "uid must be an Uid instance"

        if flags is None:
            flags = {}

        if channel_name:
            name = channel_name.encode()
            channel_name_length = len(name)
        else:
            channel_name_length = 0

        data_length = data and len(data) or 0
        chunk_flags = ChunkFlags(**flags)

        header = struct.pack(HEADER_FMT, uid.bytes, chunk_flags.encode(), channel_name_length, data_length)
        check = hashlib.md5(header).digest()

        return b''.join((header, check))

    @staticmethod
    def _decode_header(header):
        assert hashlib.md5(header[:-16]).digest() == header[-16:], "Header checksum mismatch!"
        uid_bytes, flags_encoded, channel_name_length, data_length = struct.unpack(HEADER_FMT, header[:-16])

        uid = Uid(uid_bytes=uid_bytes)

        logger.debug("uid: %s", uid)

        return uid, ChunkFlags.decode(flags_encoded), channel_name_length, data_length

    async def send(self, data, ack=False, compress=6):
        """Send data in a pickled form to the channel.

        :param data: the python object to send
        :param ack: request acknowledgement of the reception of that message
        :param compress: compress the data with zlib

        """
        uid = Uid()
        name = self.name.encode()
        loop = asyncio.get_event_loop()

        logger.debug("after uid: %s", uid)
        logger.debug("foo: %s", json.dumps(data))

xxx
        try:
            logger.debug("thread: %s", threading.current_thread())
            with concurrent.futures.ThreadPoolExecutor() as executor:
                pickled_data = await loop.run_in_executor(executor, pickle.dumps, data)
                # json_data = await loop.run_in_executor(executor, json.dumps, data)
            # logger.debug("json: %s", json_data)
            logger.debug("task: %s", asyncio.Task.current_task())

                # pickled_data = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)

        except:
            logger.error("Error: %s", traceback.format_exc())

        logger.debug("after uid: %s", uid)

        logger.debug("Channel %s sends: %s bytes", self.name, len(pickled_data))

        for part in split_data(pickled_data, self.chunk_size):
            if compress:
                raw_len = len(part)
                part = zlib.compress(part, compress)
                comp_len = len(part)

                logger.debug("Compression ratio of %s -> %s: %.2f%%", raw_len, comp_len, comp_len * 100 / raw_len)

            header = self._encode_header(uid, self.name, part, flags={
                'eom': False, 'send_ack': False, 'compression': bool(compress)
            })

            await self.io_outgoing.put((header, name, part))

        header = self._encode_header(uid, self.name, None, flags={
            'eom': True, 'send_ack': ack, 'compression': False
        })

        await self.io_outgoing.put((header, name))
        logger.debug("Channel send queue: %s", self.io_outgoing.qsize())

        # if acknowledgement is asked for we await this future and return its result
        # see _receive_reader for resolution of future
        if ack:
            ack_future = asyncio.Future()
            self.acknowledgements[uid] = ack_future

            return await ack_future

    @classmethod
    async def _send_ack(cls, io_queues, uid):
        # no channel_name, no data
        header = cls._encode_header(uid, None, None, flags={
            'eom': True, 'recv_ack': True
        })

        await io_queues.send.put(header)

    @classmethod
    async def communicate(cls, io_queues, reader, writer):
        """Schedule send and receive tasks.

        :param io_queues: the queues to use
        :param reader: the `StreamReader` instance
        :param writer: the `StreamWriter` instance

        """
        fut_send_recv = asyncio.gather(
            cls._send_writer(io_queues, writer),
            cls._receive_reader(io_queues, reader)
        )

        await fut_send_recv

    @classmethod
    async def _send_writer(cls, io_queues, writer):
        # send outgoing queue to writer
        queue = io_queues.send

        try:
            while True:
                data = await queue.get()
                if isinstance(data, tuple):
                    for part in data:
                        # logger.debug("send to writer: %s bytes", len(part))
                        writer.write(part)
                else:
                    # logger.debug("send to writer: %s bytes", len(data))
                    writer.write(data)

                queue.task_done()
                await writer.drain()

        except asyncio.CancelledError:
            if queue.qsize():
                logger.warning("Send queue was not empty when canceled!")

        except:
            logger.error("Error while sending:\n%s", traceback.format_exc())
            raise

    @classmethod
    async def _receive_single_message(cls, io_queues, reader, buffer):
        # read header
        raw_header = await reader.readexactly(HEADER_SIZE + 16)
        uid, flags, channel_name_length, data_length = cls._decode_header(raw_header)

        if channel_name_length:
            channel_name = (await reader.readexactly(channel_name_length)).decode()

        if data_length:
            if uid not in buffer:
                buffer[uid] = bytearray()

            part = await reader.readexactly(data_length)
            if flags.compression:
                part = zlib.decompress(part)

            buffer[uid].extend(part)

        if channel_name_length:
            logger.debug("Channel %s receives: %s bytes", channel_name, data_length)

        else:
            logger.debug("Message %s, received: %s", uid, flags)

        if flags.send_ack:
            # we have to acknowledge the reception
            await cls._send_ack(io_queues, uid)

        if flags.eom:
            # put message into channel queue
            if uid in buffer and channel_name_length:
                msg = pickle.loads(buffer[uid])
                await io_queues[channel_name].put(msg)
                del buffer[uid]

            # acknowledge reception
            ack_future = cls.acknowledgements.get(uid)
            if ack_future and flags.recv_ack:
                duration = time.time() - uid.time
                ack_future.set_result((uid, duration))

    @classmethod
    async def _receive_reader(cls, io_queues, reader):
        # receive incomming data into queues

        buffer = {}

        try:
            while True:
                await cls._receive_single_message(io_queues, reader, buffer)

        except asyncio.IncompleteReadError:
            # incomplete is always a cancellation
            logger.warning("While waiting for data, we received EOF!")

        except asyncio.CancelledError:
            if buffer:
                logger.warning("Receive buffer was not empty when canceled!")

        except:
            logger.error("Error while receiving:\n%s", traceback.format_exc())
            raise


######################################################################################
PLUGINS_ENTRY_POINT_GROUP = 'dbltr.plugins'


class MetaPlugin(type):

    # FIXME plugin architecture seems ugly

    plugins = {}

    def __new__(mcs, name, bases, dct):
        cls = type.__new__(mcs, name, bases, dct)

        cls.scan_entry_points()

        # add this module as default
        cls.add(cls(__name__))

        return cls

    def scan_entry_points(cls):
        # scan for available plugins
        for entry_point in pkg_resources.iter_entry_points(group=PLUGINS_ENTRY_POINT_GROUP):
            plugin = cls.create_from_entry_point(entry_point)
            # we index entry_points and module names
            cls.add(plugin)
            # mcs.plugins[plugin.module_name] = mcs.plugins[plugin.name] = plugin

    def create_from_entry_point(cls, entry_point):
        plugin = cls(entry_point.module_name, '#'.join((entry_point.dist.key, entry_point.name)))
        cls.add(plugin)

        return plugin

    def create_from_code(cls, code, project_name, entry_point_name, module_name, version='0.0.0'):
        dist = pkg_resources.Distribution(project_name=project_name, version=version)
        dist.activate()
        pkg_environment.add(dist)
        entry_point = pkg_resources.EntryPoint(entry_point_name, module_name, dist=dist)

        module = create_module(module_name)
        plugin = cls.create_from_entry_point(entry_point)

        c = compile(code, "<{}>".format(module_name), "exec", optimize=2)
        exec(c, module.__dict__)        # noqa

        return plugin

    def add(cls, plugin):
        cls.plugins[plugin.module_name] = cls.plugins[plugin.name] = plugin

    def __getitem__(cls, name):
        plugin_name, _, command_name = name.partition(':')
        plugin = cls.plugins[plugin_name]

        if not command_name:
            return plugin

        command = plugin.commands[command_name]
        return command

    def __contains__(cls, name):
        try:
            cls[name]       # noqa
            return True

        except KeyError:
            return False

    def get(cls, name, default=None):
        try:
            return cls[name]

        except KeyError:
            return default


class Plugin(metaclass=MetaPlugin):

    """A plugin module introduced via entry point."""

    def __init__(self, module_name, name=None):
        self.module_name = module_name
        self.name = name or module_name

        self.commands = {}
        """A map of commands the plugin provides."""

    @reify
    def module(self):
        """On demand module loading."""
        return __import__(self.module_name, fromlist=[''])
        # return self.entry_point.load()

    def __repr__(self):
        return "<Plugin {}>".format(self.module_name)


def exclusive(fun):
    """Make an async function call exclusive."""
    lock = asyncio.Lock()

    async def locked_fun(*args, **kwargs):
        logger.debug("Calling locked function: %s -> %s", lock, fun)
        async with lock:
            logger.debug("Executing locked function: %s -> %s", lock, fun)
            return await fun(*args, **kwargs)

    return locked_fun


class CommandMeta(type):

    base = None
    commands = {}

    command_instances = weakref.WeakValueDictionary()
    """Holds references to all active Instances of Commands, to forward to their queue"""

    def __new__(mcs, name, bases, dct):
        """Register command at plugin vice versa."""
        module = dct['__module__']
        dct['plugin'] = Plugin[module]

        cls = type.__new__(mcs, name, bases, dct)
        if mcs.base is None:
            mcs.base = cls

        else:
            # only register classes except base class
            mcs._register_command(cls)

        return cls

    @classmethod
    def _register_command(mcs, cls):
        cls.plugin.commands[cls.__name__] = cls

        for plugin_name in set((cls.plugin.module_name, cls.plugin.name)):
            name = ':'.join((plugin_name, cls.__name__))
            mcs.commands[name] = cls

    def __getitem__(cls, value):
        return cls.command_instances[value]

    @classmethod
    def _lookup_command_classmethods(mcs, *names):
        valid_names = set(['local_setup', 'remote_setup'])
        names = set(names) & valid_names

        for command in mcs.commands.values():
            for name, attr in inspect.getmembers(command, inspect.ismethod):
                if name in names:
                    yield command, name, attr

    @classmethod
    async def local_setup(mcs, *args, **kwargs):
        for _, _, func in mcs._lookup_command_classmethods('local_setup'):
            await func(*args, **kwargs)

    @classmethod
    async def remote_setup(mcs, *args, **kwargs):
        for _, _, func in mcs._lookup_command_classmethods('remote_setup'):
            await func(*args, **kwargs)

    def create_reference(cls, uid, inst):
        logger.debug("create fqin")
        fqin = (cls.fqn, uid)
        logger.debug("create reference")
        cls.command_instances[fqin] = inst
        logger.debug("end reference")


    def __init__(cls, name, bases, dct):
        type.__init__(cls, name, bases, dct)
        cls.command_name = cls.fqn = ':'.join((cls.plugin.module_name, cls.__name__))


class Command(metaclass=CommandMeta):

    """Base command class, which has no other use than provide the common ancestor to all Commands."""

    def __init__(self, io_queues, command_uid=None, **params):
        logger.debug("init %s", self.__class__)

        super(Command, self).__init__()
        logger.debug("init 2 %s", self.__class__)

        self.uid = command_uid or Uid()
        logger.debug("init 3 %s", self.__class__)
        self.io_queues = io_queues
        logger.debug("init 4 %s", self.__class__)
        self.params = params

        logger.debug("init 5 %s", self.__class__)
        self.__class__.create_reference(self.uid, self)
        logger.debug("end init: %s", self.__class__)

    def __getattr__(self, name):
        try:
            return super(Command, self).__getattr__(name)

        except AttributeError:
            try:
                return self.params[name]

            except KeyError:
                raise AttributeError(
                    "'{}' has neither an attribute nor a parameter '{}'".format(self, name)
                )

    def __contains__(self, name):
        return self.params.__contains__(name)

    def __getitem__(self, name):
        return self.params.__getitem__(name)

    @classmethod
    def create_command(cls, io_queues, command_name, command_uid=None, **params):
        """Create a new Command instance and assign a uid to it."""
        command_class = cls.commands.get(command_name)

        if inspect.isclass(command_class) and issubclass(command_class, Command):
            # do something and return result
            command = command_class(io_queues, command_uid=command_uid, **params)

            return command

        raise KeyError('The command `{}` does not exist!'.format(command_name))

    # TODO with loop.set_debug(True) it is possible to define `async def __await__(self)`
    async def __call__(self):
        execute = Execute(self.io_queues, self)
        logger.debug("execute local: %s", execute)
        result = await execute.local()

        return result

    def __await__(self):
        """Execute the command by delegating to execution command."""
        return self().__await__()

    async def local(self, remote_future):
        raise NotImplementedError(
            'You have to implement a `local` method'
            'for your Command to work: {}'.format(self.__class__)
        )

    async def remote(self):
        """An empty remote part."""

    @reify
    def fqin(self):
        """The fully qualified instance name."""
        return (self.command_name, self.uid)

    @reify
    def channel_name(self):
        """Channel name is used in header."""
        return '/'.join(map(str, self.fqin))

    @reify
    def channel(self):
        return Channel(self.channel_name, io_queues=self.io_queues)

    def __repr__(self):
        return self.channel_name


class ExecuteResult(namedtuple('ExecuteResult', ('fqin', 'result', 'exception', 'traceback'))):
    def __new__(cls, fqin, result=None, exception=None, traceback=None):        # noqa
        tb = traceback.format_exc() if exception else traceback

        return super(ExecuteResult, cls).__new__(cls, fqin, result, exception, tb)

    async def __call__(self, io_queues):
        future = Execute.pending_commands[self.fqin]

        if not self.exception:
            future.set_result(self.result)

        else:
            logger.error("Remote exception for %s:\n%s", self.fqin, self.tb)
            future.set_exception(self.exception)


class ExecuteArgs(namedtuple('ExecuteArgs', ('fqin', 'params'))):
    def __new__(cls, fqin, params=None):
        return super(ExecuteArgs, cls).__new__(cls, fqin, params or {})

    async def __call__(self, io_queues):
        command = Execute.create_command(io_queues, *self.fqin, **self.params)
        execute = Execute(io_queues, command)
        asyncio.ensure_future(execute.remote())


class Execute(Command):

    """The executor of all commands.

    This class should not be invoked directly!
    """

    pending_commands = defaultdict(asyncio.Future)

    def __init__(self, io_queues, command):
        super(Execute, self).__init__(io_queues)

        self.command = command

    async def __call__(self):
        # forbid using Execute directly
        raise RuntimeError("Do not invoke `await Execute()` directly, instead use `await Command(**params)`)")

    @reify
    def channel_name(self):
        """Execution is always run on the class channel."""
        return self.__class__.fqn

    @classmethod
    async def execute_io_queues(cls, io_queues):
        # listen to the global execute channel
        channel = Channel(cls.fqn, io_queues=io_queues)

        try:
            async for message in channel:
                logger.debug("*** Received execution message: %s", message)
                await message(io_queues)

        except asyncio.CancelledError:
            pass

        # teardown here
        for fqin, fut in cls.pending_commands.items():
            logger.warning("Teardown pending command: %s, %s", fqin, fut)
            fut.cancel()
            del cls.pending_commands[fqin]

    @classmethod
    async def local_setup(cls, io_queues):
        """Wait for ExecuteResult messages and resolves waiting futures."""
        asyncio.ensure_future(cls.execute_io_queues(io_queues))

    @classmethod
    async def remote_setup(cls, io_queues):
        asyncio.ensure_future(cls.execute_io_queues(io_queues))

    def remote_future(self):        # noqa
        """Create remote command and yield its future."""
        class _context:
            async def __aenter__(ctx):      # noqa
                logger.debug("enter remote context: %s", self.command)
                await self.channel.send(ExecuteArgs(self.command.fqin, self.command.params), ack=True)
                future = self.pending_commands[self.command.channel_name]

                return future

            async def __aexit__(ctx, *args):        # noqa
                del self.pending_commands[self.command.channel_name]

        return _context()

    async def local(self):
        async with self.remote_future() as future:
            try:
                logger.debug("excute command: %s", self.command)
                # execute local side of command
                result = await self.command.local(remote_future=future)
                future.result()
                return result

            except:     # noqa
                logger.error("Error while executing command: %s\n%s", self.command, traceback.format_exc())

            finally:
                # cleanup channel
                del self.io_queues[self.command.channel_name]

    async def remote(self):
        fqin = self.command.channel_name

        try:
            # execute remote side of command
            result = await self.command.remote()
            await self.channel.send(ExecuteResult(fqin, result=result))

            return result

        except Exception as ex:
            logger.error("traceback:\n%s", traceback.format_exc())
            await self.channel.send(ExecuteResult(fqin, exception=ex))

            raise

        finally:
            # cleanup channel
            del self.io_queues[self.command.channel_name]


class Export(Command):

    """Export a plugin to remote."""

    @exclusive
    async def __call__(self):
        return await super(Export, self).__call__()

    async def local(self, remote_future):
        plugin = Plugin[self.plugin_name]

        code = inspect.getsource(plugin.module)
        module_name = plugin.module.__name__

        project_name, entry_point_name = plugin.name.split('#')

        plugin_data = {
            'code': code,
            'project_name': project_name,
            'entry_point_name': entry_point_name,
            'module_name': module_name,
        }

        await self.channel.send(plugin_data)

        result = await remote_future

        return result

    async def remote(self):
        plugin_data = await self.channel
        Plugin.create_from_code(**plugin_data)

        return {
            'commands': list(Command.commands.keys()),
            'plugins': list(Plugin.plugins.keys())
        }


class FindModule(Command):

    """Find a module on the remote side."""

    async def local(self, remote_future):
        module_loaded = await remote_future

        return module_loaded

    def get_module(self, import_missing=False):

        module = sys.modules.get(self.module_name)

        if not module and import_missing:
            # import missing module
            pass

        if module:
            is_package = bool(getattr(module, '__path__', None))
            source = inspect.getsource(module)

            return (is_package, source)

    async def remote(self):
        return self.get_module()


class LoopExecutor(concurrent.futures.Executor):
    """An Executor subclass that uses an event loop
    to execute calls asynchronously."""

    def __init__(self, loop=None):
        """Initialize the executor with a given loop."""
        self.loop = loop or asyncio.get_event_loop()

    def submit(self, fn, *args, **kwargs):
        """Schedule the callable, fn, to be executed as fn(*args **kwargs).
        Return a Future object representing the execution of the callable."""
        coro = asyncio.coroutine(fn)(*args, **kwargs)
        return asyncio.run_coroutine_threadsafe(coro, self.loop)



async def test_coro():
    task = asyncio.Task.current_task()
    loop = task._loop

    logger.debug("loop: %s", loop)

    await asyncio.sleep(0.1)



def find_module_thread(cmd, loop):
    thread_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(thread_loop)

    future = asyncio.Future(loop=thread_loop)

    async def wait_for_module():
        module = asyncio.run_coroutine_threadsafe(test_coro(), loop=thread_loop)
        logger.debug("wait for module: %s", asyncio.Task.current_task())
        result = await future

    thread_loop.run_until_complete(wait_for_module())


    # foo = module.result()

    # logger.debug("result: %s", module)


class Foo():
    pass


async def foo(io_queues, module_name):
    loop = asyncio.get_event_loop()
    logger.debug("foo start: %s", threading.current_thread())
    await asyncio.sleep(1.0, loop=loop)

    bar = Foo()


    # u = uuid.uuid1()
    logger.debug("foo uid")
    u = Uid()
    logger.debug("foo uid: %s", u)
    find_module = FindModule(io_queues, module_name=module_name)
    result = await find_module

    logger.debug("foo finish: %s", result)
    logger.debug("foo finish")

    return 'foo'


async def call_find_module(io_queues, module_name):
    logger.debug("find thread: %s", threading.current_thread())

    loop = asyncio.get_event_loop()
    async def coro():
        find_module = FindModule(io_queues, module_name=module_name)

        result = await find_module

        return result


    future = asyncio.run_coroutine_threadsafe(coro(), loop)
    future.result()
    # loop.call_soon_threadsafe(coro)


class RemoteModuleFinder(importlib.abc.MetaPathFinder):
    # http://stackoverflow.com/questions/32059732/send-asyncio-tasks-to-loop-running-in-other-thread

    def __init__(self, io_queues, loop):
        self.io_queues = io_queues
        self.main_loop = loop

    def find_spec(self, module_name, path, target=None):
        # ask master for this module
        logger.debug("current thread: %s", threading.current_thread())

        logger.debug("Module lookup: %s", module_name)

        loop = asyncio.get_event_loop()

        future = asyncio.Future()

        # loop.call_soon_threadsafe(
        #     asyncio.ensure_future, call_find_module(self.io_queues, module_name, future)
        # )

        # find_module = FindModule(self.io_queues, module_name=module_name)
        # loop = asyncio.new_event_loop()

        # async def coro():
        #     logger.debug("run coro")
        #     future = asyncio.run_coroutine_threadsafe(foo(self.main_loop), loop=loop)
        #     return future.result()
        future = asyncio.run_coroutine_threadsafe(foo(self.io_queues, module_name), loop=self.main_loop)
        result = future.result()

        logger.debug("result: %s", result)

        # future = asyncio.run_coroutine_threadsafe(foo(self.main_loop), loop=loop)

        # loop.run_until_complete(call_find_module(self.io_queues, module_name))

        # result = loop.run_until_complete(get_result())
        # future.result()


        module = None
        # module = future.result(10)
        logger.debug("Module found for %s: %s", module_name, module)

        if module:
            spec = importlib.machinery.ModuleSpec(
                name=module_name,
                loader=RemoteModuleLoader(module_name, path, target),
                origin='remote://{}'.format(module.__file__),
                loader_state=1234,
                is_package=module.__package__ and module.__path__
            )
            return spec

    def load_module(self, module_name):
        logger.debug("Load module: %s", module_name)


class RemoteModuleLoader(importlib.abc.SourceLoader):
    def __init__(self, fullname, path, target):
        pass

    def get_data(self, path):
        logger.debug('get_data: %s', path)

    def get_filename(self, fullname):
        logger.debug('get_filename: %s', fullname)

    # def path_stats(self, path):
    #     logger.debug('path_stats: %s', path)

    # def set_data(self, path, data):
    #     logger.debug('set_data: %s, %s', path, data)

    # def get_code(self, fullname):
    #     logger.debug('get_code: %s', fullname)

    # def exec_module(self, module):
    #     logger.debug('exec_module: %s', module)
    #     logger.debug(dir(module))

    # def get_source(self, fullname):
    #     logger.debug('get_source: %s', fullname)

    # def is_package(self, fullname):
    #     logger.debug('is_package: %s', fullname)



async def run(*tasks):
    """Schedules all tasks and wait for running is done or canceled"""
    # create indicator for running messenger
    running = asyncio.ensure_future(asyncio.gather(*tasks))

    # exit on sigterm or sigint
    for signame in ('SIGINT', 'SIGTERM', 'SIGHUP', 'SIGQUIT'):
        sig = getattr(signal, signame)

        def exit_with_signal(sig):
            try:
                running.cancel()

            except asyncio.InvalidStateError:
                logger.warning("running already done!")

        asyncio.get_event_loop().add_signal_handler(sig, functools.partial(exit_with_signal, sig))

    # wait for running completed
    try:
        result = await running
        return result

    except asyncio.CancelledError:
        pass


def cancel_pending_tasks(loop):
    for task in asyncio.Task.all_tasks():
        if task.done() or task.cancelled():
            continue

        task.cancel()
        try:
            loop.run_until_complete(task)

        except asyncio.CancelledError:
            pass


async def log_tcp_10001():
    try:
        reader, writer = await asyncio.open_connection('localhost', 10001)

        while True:
            msg = await reader.readline()

            logger.info("TCP: %s", msg)

    except asyncio.CancelledError:
        logger.info("close tcp logger")
        writer.close()


async def communicate(io_queues):
    async with Incomming(pipe=sys.stdin) as reader:
        async with Outgoing(pipe=sys.stdout, shutdown=True) as writer:
            await Channel.communicate(io_queues, reader, writer)


class ExecutorConsoleHandler(logging.StreamHandler):

    # FIXME TODO Executor seems to disturb uvloop so that it hangs randomly

    """Run logging in a separate executor, to not block on console output."""

    def __init__(self, *args, **kwargs):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=150)

        super(ExecutorConsoleHandler, self).__init__(*args, **kwargs)

    def emit(self, record):
        # FIXME is not really working on sys.stdout
        asyncio.get_event_loop().run_in_executor(
            self.executor, functools.partial(super(ExecutorConsoleHandler, self).emit, record)
        )

    def __del__(self):
        self.executor.shutdown(wait=True)


def main(debug=False, log_config=None, **kwargs):
    if log_config is None:
        log_config = {
            'version': 1,
            'formatters': {
                'simple': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple',
                    'level': 'DEBUG',
                    'stream': 'ext://sys.stderr'
                }
            },
            'loggers': {
                'dbltr': {
                    'handlers': ['console'],
                    'level': 'INFO',
                    'propagate': False
                }
            },
            'root': {
                'handlers': ['console'],
                'level': 'DEBUG'
            },
        }

    logging.config.dictConfig(log_config)
    if debug:
        logger.setLevel(logging.DEBUG)

    loop = asyncio.get_event_loop()
    loop.set_debug(debug)

    io_queues = IoQueues()

    # setup all plugin commands
    loop.run_until_complete(Command.remote_setup(io_queues))

    # install import hook
    remote_module_finder = RemoteModuleFinder(io_queues, loop)
    sys.meta_path.append(remote_module_finder)

    logger.debug("meta path: %s", sys.meta_path)

    try:
        loop.run_until_complete(
            run(
                communicate(io_queues)
                # log_tcp_10001()
            )
        )
        cancel_pending_tasks(loop)

    finally:
        loop.close()


def decode_options(b64):
    return pickle.loads(base64.b64decode(b64))


def encode_options(**options):
    return base64.b64encode(pickle.dumps(options)).decode()
