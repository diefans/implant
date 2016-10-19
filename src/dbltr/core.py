"""
Remote process
"""
import inspect
import sys
import asyncio
import signal
import functools
import os
import base64
import json
import hashlib
import uuid
import types
import gzip
import lzma
import logging
import weakref
import traceback
import pickle
import struct
import time
from collections import defaultdict
import pkg_resources


pkg_environment = pkg_resources.Environment()


logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)
logger.info('\n%s', '\n'.join(['*' * 80] * 3))


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


def uuid1_time(uid):
    return (uid.time - 0x01b21dd213814000) * 100 / 1e9


def log(_msg, **kwargs):
    if sys.stderr.isatty():
        colors = {
            'color': '\033[01;31m',
            'nocolor': '\033[0m'
        }

    else:
        colors = {
            'color': '',
            'nocolor': ''
        }

    if kwargs:
        error_msg = "{color}{msg} {kwargs}{nocolor}".format(msg=_msg, kwargs=kwargs, **colors)

    else:
        error_msg = "{color}{msg}{nocolor}".format(msg=_msg, **colors)

    logger.debug(error_msg)


def create_loop(*, debug=False):
    """Create the appropriate loop."""

    if sys.platform == 'win32':
        loop_ = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop_)

    else:
        loop_ = asyncio.get_event_loop()

    loop_.set_debug(debug)

    return loop_


class IoQueues:
    """Just to keep send and receive queues together."""

    def __init__(self, send=None, error=None):
        if send is None:
            send = asyncio.Queue()

        self.send = send

        self.receive = defaultdict(asyncio.Queue)

    async def send_to_writer(self, writer):
        """A continuos task to send all data in the send queue to a stream writer."""

        while True:
            data = await self.send.get()
            try:
                writer.write(data)
                await writer.drain()

            finally:
                self.send.task_done()


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
        # see https://github.com/python/asyncio/issues/394
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
    def connection_lost(self, exc):
        """Shutdown process"""
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


class ChunkFlags(dict):

    _masks = {
        'eom': (1, 0, int, bool),
        'stop_iter': (1, 1, int, bool),
        'send_ack': (1, 2, int, bool),
        'recv_ack': (1, 3, int, bool),
        'compression': (3, 4, {
            False: 0,
            True: 1,
            'gzip': 1,
            'lzma': 2,
            'zlib': 3,
        }.get, {
            0: False,
            1: 'gzip',
            2: 'lzma',
            3: 'zlib'
        }.get),
    }

    def __init__(self, *, send_ack=False, recv_ack=False, eom=False, stop_iter=False, compression=False):
        self.__dict__ = self
        super(ChunkFlags, self).__init__()

        self.eom = eom
        self.stop_iter = stop_iter
        self.send_ack = send_ack
        self.recv_ack = recv_ack
        self.compression = compression is True and 'gzip' or compression

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


HEADER_FMT = '!16sQHI'
HEADER_SIZE = struct.calcsize(HEADER_FMT)


class Channel:

    chunk_size = 0x8000

    acknowledgements = weakref.WeakValueDictionary()
    """Global acknowledgment futures distinctive by uuid."""

    def __init__(self, name=None, *, io_queues=None):
        self.name = name
        self.io_queues = io_queues or IoQueues()
        self.io_outgoing = self.io_queues.send
        self.io_incomming = self.io_queues.receive[self.name]

    @staticmethod
    def _encode_header(uid, channel_name=None, data=None, *, flags=None):
        """
        [header length = 30 bytes]
        [!16s]     [!Q: 8 bytes]                     [!H: 2 bytes]        [!I: 4 bytes]
        {data uuid}{flags: compression|eom|stop_iter}{channel_name length}{data length}{channel_name}{data}
        """
        assert isinstance(uid, uuid.UUID), "uid must be an UUID instance"

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

        return uuid.UUID(bytes=uid_bytes), ChunkFlags.decode(flags_encoded), channel_name_length, data_length

    async def send(self, data, ack=False):
        compression = False
        uid = uuid.uuid1()
        name = self.name.encode()

        pickled_data = pickle.dumps(data)

        for part in split_data(pickled_data, self.chunk_size):
            header = self._encode_header(uid, self.name, part, flags={
                'eom': False, 'send_ack': False, 'compression': compression
            })

            await self.io_outgoing.put((header, name, part))

        header = self._encode_header(uid, self.name, None, flags={
            'eom': True, 'send_ack': ack, 'compression': False
        })
        await self.io_outgoing.put((header, name))

        # if acknowledgement is asked for
        # we await this future and return its result
        # see _receive_reader for resolution of future
        if ack:
            ack_future = asyncio.Future()
            self.acknowledgements[uid] = ack_future

            return await ack_future

    async def receive(self):
        """Receive the next message in this channel."""

        msg = await self.io_incomming.get()
        try:
            return msg
        finally:
            self.io_incomming.task_done()

    @classmethod
    async def _send_ack(cls, io_queues, uid):
        # no channel_name, no data
        header = cls._encode_header(uid, None, None, flags={
            'eom': True, 'recv_ack': True
        })

        await io_queues.send.put(header)

    @classmethod
    async def communicate(cls, io_queues, reader, writer):
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
            logger.debug("sending queue to writer")
            while True:
                data = await queue.get()
                if isinstance(data, tuple):
                    for part in data:
                        writer.write(part)
                else:
                    writer.write(data)

                queue.task_done()
                await writer.drain()

        except asyncio.CancelledError:
            if queue.qsize():
                logger.warning("Send queue was not empty when canceld!")

    @classmethod
    async def _receive_single_message(cls, io_queues, reader, buffer):
        # read header
        try:
            logger.debug("waiting for header...")
            raw_header = await reader.readexactly(HEADER_SIZE + 16)
            uid, flags, channel_name_length, data_length = cls._decode_header(raw_header)
            logger.debug("\t<-- header: %s, sizes: %s/%s, %s", uid, channel_name_length, data_length, flags)

            if channel_name_length:
                channel_name = (await reader.readexactly(channel_name_length)).decode()

            if data_length:
                if uid not in buffer:
                    buffer[uid] = bytearray()

                buffer[uid].extend(await reader.readexactly(data_length))

            if flags.send_ack:
                # we have to acknowledge the reception
                await cls._send_ack(io_queues, uid)

            if flags.eom:
                # put message into channel queue
                if uid in buffer:
                    msg = pickle.loads(buffer[uid])
                    await io_queues.receive[channel_name].put(msg)
                    del buffer[uid]

                # acknowledge reception
                ack_future = cls.acknowledgements.get(uid)
                if ack_future and flags.recv_ack:
                    # TODO is there a meaningful result for an acknowledgement
                    # - timestamp of reception
                    # - duration of roundtrip
                    duration = time.time() - uuid1_time(uid)
                    ack_future.set_result((uid, duration))
        except asyncio.CancelledError:
            raise

        except:
            logger.error("Error while receiving:\n%s", traceback.format_exc())

    @classmethod
    async def _receive_reader(cls, io_queues, reader):
        # receive incomming data into queues

        buffer = {}

        try:
            logger.debug("receiving reader into queues")
            while True:
                await cls._receive_single_message(io_queues, reader, buffer)
                logger.info("Message received")

        except asyncio.CancelledError:
            if buffer:
                log.warn("Receive buffer was not empty when canceled!")


######################################################################################
PLUGINS_ENTRY_POINT_GROUP = 'dbltr.plugins'


class MetaPlugin(type):

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
            cls[name]
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

    @classmethod
    def create_from_entry_point(cls, entry_point):
        plugin = cls(entry_point.module_name, '#'.join((entry_point.dist.key, entry_point.name)))
        cls.add(plugin)

        return plugin

    @classmethod
    def create_from_code(cls, code, project_name, entry_point_name, module_name, version='0.0.0'):
        dist = pkg_resources.Distribution(project_name=project_name, version=version)
        dist.activate()
        pkg_environment.add(dist)
        entry_point = pkg_resources.EntryPoint(entry_point_name, module_name, dist=dist)

        module = create_module(module_name)
        plugin = cls.create_from_entry_point(entry_point)

        c = compile(code, "<{}>".format(module_name), "exec", optimize=2)
        exec(c, module.__dict__)

        return plugin

    def __repr__(self):
        return "<Plugin {}>".format(self.module_name)


class CommandMeta(type):

    base = None
    commands = {}

    command_instances = weakref.WeakValueDictionary()
    """Holds references to all active Instances of Commands, to forward to their queue"""

    command_instance_names = weakref.WeakKeyDictionary()
    """Holds all instances mapping to their FQIN."""

    def __new__(mcs, name, bases, dct):
        """Register command at plugin vice versa"""

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
        @functools.singledispatch
        def _get(value):
            return None

        @_get.register(tuple)
        def _tuple(value):
            return cls.command_instances[value]

        @_get.register(bytes)
        def _bytes(value):
            return cls.command_instances[tuple(value.split(b'/', 1))]

        @_get.register(memoryview)
        def _memoryview(value):
            return cls.command_instances[tuple(value.tobytes().split(b'/', 1))]

        return _get(value)

    @property
    def fqn(cls):
        return ':'.join((cls.plugin.module_name, cls.__name__)).encode()

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

    def _create_reference(cls, uid, inst):
        fqin = (cls.fqn, uid)
        cls.command_instance_names[inst] = fqin
        cls.command_instances[fqin] = inst

    def __call__(cls, *args, command_uid=None, **kwargs):
        inst = super(CommandMeta, cls).__call__(*args, **kwargs)

        if command_uid is None:
            command_uid = uuid.uuid1().hex.encode()

        cls._create_reference(command_uid, inst)

        return inst


class Command(metaclass=CommandMeta):

    """Base command class, which has no other use than provide the common ancestor to all Commands."""

    channels = defaultdict(asyncio.Queue)

    def __init__(self):
        self.queue = asyncio.Queue()

    def local(self):
        raise NotImplementedError(
            'You have to implement at least a `local` method'
            'for your Command to work: {}'.format(self.__class__)
        )

    @reify
    def uid(self):
        _, uid = self.fqin

        return uid

    @reify
    def fqin(self):
        """The fully qualified instance name."""

        return self.__class__.command_instance_names[self]

    @reify
    def channel_name(self):
        return b'/'.join(self.fqin)

    @functools.lru_cache(maxsize=10, typed=True)
    def channel(self, channel_type, *, queue=None):
        return channel_type(self.channel_name, queue=queue or self.queue)

    def __repr__(self):
        return self.channel_name.decode()


class RemoteException(Exception):

    def __init__(self, *, fqin, tb, exc_type):
        super(RemoteException, self).__init__()

        self.fqin = fqin
        self.traceback = tb
        self.type = exc_type


class Execute(Command):

    """The executor of all commands."""

    pending_futures = defaultdict(asyncio.Future)

    def __init__(self, command_name, *args, **kwargs):
        super(Execute, self).__init__()

        self.command_name = command_name
        self.args = args
        self.kwargs = kwargs

    @property
    def params(self):
        params = {
            'command_name': self.command_name,
            'args': self.args,
            'kwargs': self.kwargs,
        }
        return params

    def create_command(self):
        """Create a new Command instance and assign a uid to it."""

        command_class = Command.commands.get(self.command_name)

        if inspect.isclass(command_class) and issubclass(command_class, Command):

            # do something and return result
            command = command_class(*self.args, command_uid=self.uid, **self.kwargs)

            return command

        raise KeyError('The command `{}` does not exist!'.format(self.command_name))

    async def local(self, remote):
        command = self.create_command()

        await self._create_remote_command(remote)

        # XXX TODO create a context and remove future when done
        future = self.pending_futures[command.channel_name]
        try:
            result = await command.local(queue_out=remote.queue_in, remote_future=future)
            future.result()

            return result

        except:
            logger.error("Error while executing command: %s\n%s", command, traceback.format_exc())

        finally:
            del self.pending_futures[command.channel_name]

    async def _create_remote_command(self, remote):
        channel_out = JsonChannel(
            self.__class__.fqn,
            queue=remote.queue_in
        )

        # create remote command
        await channel_out.send(self.params, uid=self.uid)

        # wait for sync
        await self.channel(JsonChannel)

    @classmethod
    async def local_setup(cls, remote):
        # distibute to local instances
        logger.info("create local instance distributor")

        channel_in = JsonChannel()

        async def resolve_pending_futures():
            try:
                async for _, message in channel_in:
                    fqin = message['fqin'].encode()
                    future = cls.pending_futures[fqin]

                    if 'result' in message:
                        future.set_result(message['result'])

                    elif 'exception' in message:
                        ex = RemoteException(
                            fqin=fqin,
                            exc_type=message['exception']['type'],
                            traceback=message['exception']['traceback'],
                        )
                        ex_unpickled = pickle.loads(base64.b64decode(message['exception']['pickle']))

                        future.set_exception(ex_unpickled)
            except asyncio.CancelledError:
                pass

            # teardown here
            for fut in cls.pending_futures.values():
                fut.cancel()

        async def distribute_responses():
            try:
                async for line in remote.stdout:
                    if line is b'':
                        break

                    channel_name, _, _, _ = Chunk.view(line)

                    # split command channel from other plugin channels
                    # we want to execute a command
                    if channel_name == cls.fqn:
                        await channel_in.forward(line)

                    else:
                        # forward arbitrary data to their instances
                        try:
                            await cls[channel_name].queue.put(line)

                        except KeyError:
                            logger.error("Channel instance not found: %s", channel_name.tobytes())

            except asyncio.CancelledError:
                pass

            # teardown here

        asyncio.ensure_future(distribute_responses())
        asyncio.ensure_future(resolve_pending_futures())

    async def remote(self, queue_out):
        command = self.create_command()

        result_message = {
            'fqin': command.channel_name.decode(),
        }

        # trigger sync for awaiting local method
        channel_out = self.channel(JsonChannel, queue=queue_out)
        await channel_out.send(None)

        try:
            result = await command.remote(
                queue_out=queue_out,
            )

            result_message['result'] = result

        except Exception as ex:
            result_message['exception'] = {
                'traceback': traceback.format_exc(),
                'type': ex.__class__.__name__,
                'pickle': base64.b64encode(pickle.dumps(ex)).decode()
            }

        return result_message

    @classmethod
    async def remote_setup(cls, queue_out):
        channel_out = JsonChannel(cls.fqn, queue=queue_out)

        # our incomming command queue
        queue_in = asyncio.Queue()
        channel_in = JsonChannel(queue=queue_in)

        async def execute_commands():
            try:
                async for uid, message in channel_in:
                    execute = cls(message['command_name'], *message['args'], command_uid=uid, **message['kwargs'])

                    # TODO spawn Task for each command which might be canceled by local side errors
                    result = await execute.remote(queue_out=queue_out)
                    if 'exception' in result:
                        logger.error("traceback:\n%s", result['exception']['traceback'])

                    await channel_out.send(result, compress='gzip')

            except asyncio.CancelledError:
                pass

            # teardown here

        async def distribute_commands():
            try:
                async with Incomming(pipe=sys.stdin) as reader:
                    while True:
                        line = await reader.readline()
                        if line is b'':
                            break

                        channel_name, _, _, _ = Chunk.view(line)

                        # split command channel from other plugin channels
                        # we want to execute a command
                        if channel_name == cls.fqn:
                            await channel_in.forward(line)

                        else:
                            # forward arbitrary data to their instances
                            await cls[channel_name].queue.put(line)
            except asyncio.CancelledError:
                pass

            # teardown here

        # create a teardown background task to be called when this process finishes
        asyncio.ensure_future(execute_commands())
        asyncio.ensure_future(distribute_commands())


class Import(Command):
    def __init__(self, plugin_name):
        super(Import, self).__init__()

        self.plugin_name = plugin_name

    async def local(self, queue_out, remote_future):
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

        channel_out = self.channel(JsonChannel, queue=queue_out)
        await channel_out.send(plugin_data)

        result = await remote_future

        return result

    async def remote(self, queue_out):
        channel_in = self.channel(JsonChannel)
        _, plugin_data = await channel_in
        Plugin.create_from_code(**plugin_data)

        return {
            'commands': list(Command.commands.keys()),
            'plugins': list(Plugin.plugins.keys())
        }


DEFAULT_CHANNEL_NAME = b''


class BinaryChunk:

    """
    structure:
    - channel_name length must be less than 1024 characters
    -

    [header length = 30 bytes]
    [!I: 4 bytes]             [!Q: 8 bytes]                     [!16s]     [!H: 2 bytes]
    {overall length[0..65536]}{flags: compression|eom|stop_iter}{data uuid}{channel_name length}{channel_name}{data}


    """

    def __init__(self, data=None, *, channel_name=DEFAULT_CHANNEL_NAME, uid=None):
        self.data = data and memoryview(data) or None
        self.channel_name = channel_name
        self.uid = uid

    @classmethod
    def decode(cls, raw):
        pass

    def encode(self, compress=False):
        pass



class Chunk:

    """
    Abstracts the format of the wire data.

    We split messages into chunks to allow parallel communication of messages.
    """

    separator = b'|'

    compressor = {
        'gzip': gzip,
        'lzma': lzma
    }

    def __init__(self, data=None, *, channel=None, uid=None):
        self.data = data
        self.channel = channel
        self.uid = uid

    @classmethod
    @functools.lru_cache(typed=True)
    def view(cls, raw):
        raw_view = memoryview(raw)

        # split raw data at separators
        sep_ord = ord(cls.separator)

        def _gen_separator_index():
            count = 0
            for i, byte in enumerate(raw_view):
                if byte == sep_ord:
                    yield i
                    count += 1

            assert count == 3, 'A Chunk must be composed by three separators, '\
                'e.g. `channel|uid|payload`!'
        channel_end, uid_end, compressor_end = _gen_separator_index()

        raw_len = len(raw_view)
        # 10 == \n: skip newline
        raw_end = raw_len - 1 if raw_view[-1] == 10 else raw_len

        channel_view = raw_view[0:channel_end]
        uid_view = raw_view[channel_end + 1:uid_end]
        compressor_view = raw_view[uid_end + 1:compressor_end]
        data_view = raw_view[compressor_end + 1:raw_end]

        return channel_view, uid_view, compressor_view, data_view

    @classmethod
    def decode(cls, raw):
        """Decodes a bytes string with or without ending \n into a Chunk."""

        channel_view, uid_view, compressor_view, data_view = cls.view(raw)

        data = base64.b64decode(data_view)
        if compressor_view.tobytes().decode() in cls.compressor:
            data = cls.compressor[compressor_view.tobytes().decode()].decompress(data)

        return cls(data, channel=channel_view.tobytes(), uid=uid_view.tobytes())

    def encode(self, eol=True, *, compress=False):
        """Encode the chunk into a bytes string."""

        def _gen_parts():
            yield self.channel or DEFAULT_CHANNEL_NAME
            yield self.separator
            yield self.uid or b''
            yield self.separator
            yield compress and compress.encode() or b''
            yield self.separator

            data = self.data or b''
            if compress in self.compressor:
                data = self.compressor[compress].compress(data)
            yield base64.b64encode(data)

            if eol:
                yield b'\n'

        return b''.join(_gen_parts())

    def __repr__(self):
        def _gen():
            yield b'<Chunk'
            if self.uid or self.channel:
                yield b' '

            if self.uid:
                yield self.uid

            if self.channel:
                yield b'@'
                yield self.channel

            if self.data:
                yield b' len='
                yield str(len(self.data)).encode()

            else:
                yield b' EOM'

            yield b'>'

        return b''.join(_gen()).decode()

    @property
    def is_eom(self):
        return self.data == b''


class ChunkChannel:

    """Iterator over a queues own chunks."""

    max_size = 10240

    def __init__(self, name=DEFAULT_CHANNEL_NAME, *, queue=None):
        self.name = name
        self.queue = queue or asyncio.Queue()

    async def __aiter__(self):
        return self

    async def __anext__(self):
        uid, data = await self

        return uid, data

    async def __await__(self):
        """:returns: a tuple (uid, chunk) from the queue of this channel."""

        line = await self.queue.get()

        try:
            chunk = Chunk.decode(line)
            # logger.debug('Chunk received at %s: %s', self, chunk)

            return chunk.uid, chunk

        finally:
            self.queue.task_done()

    async def forward(self, line):
        await self.queue.put(line)

    async def inject_chunk(self, chunk, compress=False):
        """Allows forwarding of a chunk as is into the queue."""

        await self.queue.put(chunk.encode(compress=compress))

    async def send(self, data, *, uid=None, compress=False):
        """Create a chunk from data and uid and send it to the queue of this channel."""

        if isinstance(data, Chunk):
            data = data.data

        for part in split_data(data, self.max_size):
            chunk = Chunk(part, channel=self.name, uid=uid)

            await self.inject_chunk(chunk, compress=compress)

    async def send_eom(self, *, uid=None):
        """Send EOM (end of message) to the queue."""

        chunk = Chunk(None, channel=self.name, uid=uid)

        await self.queue.put(chunk.encode(compress=False))


def split_data(data, size=1024):
    """A generator to yield splitted data."""

    data_view = memoryview(data)
    data_len = len(data_view)
    start = 0

    while start < data_len:
        end = min(start + size, data_len)

        yield data_view[start:end]

        start = end


class EncodedBufferChannel(ChunkChannel):

    """Buffers chunks, combines them and tries to decode them."""

    def __init__(self, *args, **kwargs):
        super(EncodedBufferChannel, self).__init__(*args, **kwargs)

        # holds message parts until decoding is complete
        self.buffer = {}

    async def __await__(self):
        """Wait until a message is completed by sending an empty chunk."""

        while True:
            # wait for the next data
            uid, chunk = await super(EncodedBufferChannel, self).__await__()

            # create buffer if uid is new
            if uid not in self.buffer:
                self.buffer[uid] = bytearray()

            # message completed -> return decoded chunks
            if chunk.is_eom:
                return uid, self._pop_from_buffer(uid)

            # append data to buffer
            self.buffer[uid].extend(chunk.data)

    def _pop_from_buffer(self, uid):
        try:
            data = self.decode(self.buffer[uid].decode())
            return data

        finally:
            del self.buffer[uid]

    def decode(self, data):
        return data

    def encode(self, data):
        return data

    async def send(self, data, *, uid=None, compress=False):

        if uid is None:
            uid = uuid.uuid1().hex.encode()

        if isinstance(data, Chunk):
            data = data.data

        encoded_data = self.encode(data).encode()

        await super(EncodedBufferChannel, self).send(encoded_data, uid=uid, compress=compress)
        await super(EncodedBufferChannel, self).send_eom(uid=uid)


class JsonChannel(EncodedBufferChannel):

    def decode(self, data):
        data = json.loads(data)
        return data

    def encode(self, data):
        data = json.dumps(data)
        return data


class JsonChannelIterator:
    def __init__(self, channel):
        assert isinstance(channel, JsonChannel), "channel must be a JsonChannel instance"

        self.channel = channel

    async def send(self, iterable):
        for item in iterable:
            await self.channel.send(
                {
                    'item': item,
                }, compress='gzip')
        else:
            await self.channel.send({'stop_iteration': True})

    async def __aiter__(self):
        return self

    async def __anext__(self):
        _, value = await self.channel
        if value.get('stop_iteration', False):
            raise StopAsyncIteration()

        return value


async def run(*tasks):
    """Schedules all tasks and wait for running is done or canceled"""

    # create indicator for running messenger
    running = asyncio.Future()

    # shutdown_tasks = []

    for task in tasks:
        fut = asyncio.ensure_future(task)

    # exit on sigterm or sigint
    for signame in ('SIGINT', 'SIGTERM', 'SIGHUP', 'SIGQUIT'):
        sig = getattr(signal, signame)

        def exit_with_signal(sig):
            logger.info("SIGNAL: %s", sig)
            try:
                running.set_result(sig)

            except asyncio.InvalidStateError:
                logger.warning("running already done!")

        asyncio.get_event_loop().add_signal_handler(sig, functools.partial(exit_with_signal, sig))

    # wait for running completed
    result = await running

    return result


def cancel_pending_tasks():
    loop = asyncio.get_event_loop()

    for task in asyncio.Task.all_tasks():
        if task.done() or task.cancelled():
            continue

        task.cancel()
        try:
            loop.run_until_complete(task)

        except asyncio.CancelledError:
            pass

        logger.debug("*** Task shutdown: %s", task)


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


def main(**kwargs):

    loop = create_loop(debug=True)

    io_queues = IoQueues()

    # setup all plugin commands
    loop.run_until_complete(Command.remote_setup(io_queues))
    try:
        loop.run_until_complete(
            run(
                communicate(io_queues)
                # send_outgoing_queue(queue_out, sys.stdout),
                # log_tcp_10001()
            )
        )
        cancel_pending_tasks()

    finally:
        loop.close()


def decode_options(b64):
    return json.loads(base64.b64decode(b64).decode())


def encode_options(**options):
    return base64.b64encode(json.dumps(options).encode()).decode()


if __name__ == '__main__':
    main()
