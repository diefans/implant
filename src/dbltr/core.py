"""
Remote process
"""
import pkg_resources
import inspect
import sys
import asyncio
import signal
import functools
import os
import base64
import json
import uuid
import types
import gzip
import lzma
import contextlib
import logging


pkg_environment = pkg_resources.Environment()


logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)
logger.info('\n%s', '\n'.join(['*' * 80] * 3))

finalizer = []


def add_finalizer(func):
    finalizer.append(func)


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
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)

    else:
        loop = asyncio.get_event_loop()

    loop.set_debug(debug)

    return loop


# loop is global
loop = create_loop(debug=True)


class Incomming:

    """A context for an incomming pipe."""

    def __init__(self, *, loop=None, pipe=sys.stdin):
        self._loop = loop or asyncio.get_event_loop()
        self.pipe = pipe
        self.transport = None

    async def __aenter__(self):
        reader = asyncio.StreamReader(loop=self._loop)
        protocol = asyncio.StreamReaderProtocol(reader)

        self.transport, _ = await self._loop.connect_read_pipe(
            lambda: protocol,
            # sys.stdin
            os.fdopen(self.pipe.fileno(), 'r')
        )
        return reader

    async def __aexit__(self, exc_type, value, tb):
        self.transport.close()


class Outgoing:

    """A context for an outgoing pipe."""

    def __init__(self, *, loop=None, pipe=sys.stdout):
        self._loop = loop or asyncio.get_event_loop()
        self.pipe = pipe
        self.transport = None

    async def __aenter__(self):
        self.transport, protocol = await self._loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin,
            # sys.stdout
            os.fdopen(self.pipe.fileno(), 'wb')
        )
        writer = asyncio.streams.StreamWriter(self.transport, protocol, None, self._loop)

        return writer

    async def __aexit__(self, exc_type, value, tb):
        self.transport.close()


async def send_outgoing_queue(queue, pipe=sys.stdout):
    """Write data from queue to stdout."""

    async with Outgoing(pipe=pipe) as writer:
        while True:
            data = await queue.get()
            writer.write(data)
            await writer.drain()
            queue.task_done()


async def distribute_incomming_chunks(channels, pipe=sys.stdin):
    """Distribute all chunks from stdin to channels."""

    async with Incomming(pipe=pipe) as reader:
        while True:
            line = await reader.readline()
            if line is b'':
                break

            await channels.distribute(line)


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

    def local(cls, func):
        from pdb import set_trace; set_trace()       # XXX BREAKPOINT
        func.local = True

        return func

    def Command(cls, cmd):
        from pdb import set_trace; set_trace()       # XXX BREAKPOINT
        # iterate through the methods and register methods


    def local_setup(cls, func):
        pass

    def local_teardown(cls, func):
        pass

    def remote(cls, func):
        pass

    def remote_setup(cls, func):
        pass

    def remote_teardown(cls, func):
        pass


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

        return plugin

    @classmethod
    def create_from_code(cls, code, project_name, entry_point_name, module_name, version='0.0.0'):
        dist = pkg_resources.Distribution(project_name=project_name, version=version)
        dist.activate()
        pkg_environment.add(dist)
        entry_point = pkg_resources.EntryPoint(entry_point_name, module_name, dist=dist)

        module = create_module(module_name)
        c = compile(code, "<string>", "exec", optimize=2)
        exec(c, module.__dict__)

        plugin = cls.create_from_entry_point(entry_point)

        return plugin

    class command:
        def __init__(self, name=None):
            self.name = name
            self.local_func = None
            self.command = None

        def __call__(self, func):
            name = self.name or func.__name__
            self.command = Command(name, local=func)

            print("set func:", name, func)

            self._extend_decorators(func)
            return func

        def _extend_decorators(self, func):
            func.local_setup = self.set_local_setup
            func.local_teardown = self.set_local_teardown

            func.remote = self.set_remote
            func.remote_setup = self.set_remote_setup
            func.remote_teardown = self.set_remote_teardown

        def set_remote(self, func):
            self.command.remote = func
            self._extend_decorators(func)
            return func

        def set_local_setup(self, func):
            self.command.local_setup = func
            self._extend_decorators(func)
            return func

        def set_local_teardown(self, func):
            self.command.local_teardown = func
            self._extend_decorators(func)
            return func

        def set_remote_setup(self, func):
            self.command.remote_setup = func
            self._extend_decorators(func)
            return func

        def set_remote_teardown(self, func):
            self.command.remote_teardown = func
            self._extend_decorators(func)
            return func

    @classmethod
    def _command(cls, name=None):
        """Decorates a command."""

        command_name = name

        def decorator(func):

            func_module = inspect.getmodule(func).__name__

            if command_name is None:
                name = func.__name__

            else:
                name = command_name

            plugin = Plugin[func_module]

            if name in plugin.commands:
                raise KeyError("Command `{}` is already defined in plugin `{}`".format(name, func_module))

            command = plugin.commands[name] = Command(name, local=func)

            def remote_decorator(remote_func):
                command.remote = remote_func

                return remote_func

            func.remote = remote_decorator

            def remote_setup_decorator(remote_func):
                command.remote_setup = remote_func

                return remote_func

            func.remote_setup = remote_setup_decorator

            logger.debug("\t%s, new command: %s:%s", id(cls), func_module, name)
            logger.debug("plugins: %s", Plugin.plugins)

            return func

        return decorator

    def __repr__(self):
        return "<Plugin {}>".format(self.module_name)

    @classmethod
    async def distribute_incomming_chunks(cls, channels, pipe=sys.stdin):
        """Distribute all chunks from stdin to channels."""

        async with Incomming(pipe=pipe) as reader:
            while True:
                line = await reader.readline()
                if line is b'':
                    break

                channel, uid, compressor, data = Chunk.view(line)

                await channels.distribute(line)


class CommandMeta(type):

    base = None
    commands = {}

    def __new__(mcs, name, bases, dct):
        """Register command at plugin vice versa"""

        module = dct['__module__']
        plugin = dct['plugin'] = Plugin[module]

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

    @classmethod
    def _lookup_command_classmethods(mcs, *names):
        valid_names = set(['local_setup', 'local_teardown', 'remote_setup', 'remote_teardown'])
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
    async def local_teardown(mcs, *args, **kwargs):
        for _, _, func in mcs._lookup_command_classmethods('local_teardown'):
            await func(*args, **kwargs)

    @classmethod
    async def remote_setup(mcs, *args, **kwargs):
        for _, _, func in mcs._lookup_command_classmethods('remote_setup'):
            await func(*args, **kwargs)

    @classmethod
    async def remote_teardown(mcs, *args, **kwargs):
        for _, _, func in mcs._lookup_command_classmethods('remote_teardown'):
            await func(*args, **kwargs)


class Cmd(metaclass=CommandMeta):

    """Base command class, which has no other use than provide the common ancestor to all Commands."""

    def local(self, *args, **kwargs):
        raise NotImplementedError(
            'You have to implement at least a `local` method'
            'for your Command to work: {}'.format(self.__class__)
        )


class Execute(Cmd):

    """The executor of all commands."""

    def __init__(self, command, *args, **kwargs):
        pass

    @classmethod
    async def local_setup(cls, remote, loop):
        pass

    @classmethod
    async def remote_setup(cls, queue_out, loop):
        channel_out = JsonChannel(queue=queue_out)

        # our incomming command queue
        queue_in = asyncio.Queue(loop=loop)
        channel_in = JsonChannel(queue=queue_in)

        # a queue for each plugin command message
        plugin_command_queues = {}

        async def execute_commands():
            async for uid, message in channel_in:
                logger.info("retrieved command: %s", message)

                # do something and return result
                cmd_args = message.get('args', [])
                cmd_kwargs = message.get('kwargs', {})

                result = await command.remote(
                    command,
                    queue_out=queue_out,
                    command_args=cmd_args,
                    command_kwargs=cmd_kwargs
                )

                result = {
                    'foo': result
                }

                async with channel_out.message(uid) as send:
                    await send(result, compressor='gzip')

        async def distribute_commands():
            async with Incomming(pipe=sys.stdin) as reader:
                while True:
                    line = await reader.readline()
                    if line is b'':
                        break

                    channel, uid, compressor, data = Chunk.view(line)

                    logger.debug("\t\tincomming: %s", line)

                    # split command channel from other plugin channels
                    # we want to execute a command
                    if channel == command.id:
                        await channel_in.forward(line)

                    else:
                        # distribute to plugin channels
                        await channels.distribute(line)

        logger.debug("remote setup %s %s", command, queue_out)

        # create a teardown background task to be called when this process finishes
        setup = asyncio.gather(execute_commands(), distribute_commands(), loop=loop)
        future = asyncio.Future(loop=loop)
        setup.add_done_callback(future.set_result)

        async def teardown():
            await future
            result = future.get_result()

            logger.debug("teardown %s with %s", cls, result)
        asyncio.ensure_future(teardown())


class Echo(Cmd):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    async def local(self):
        pass

    async def remote(self):
        pass

    @classmethod
    async def remote_setup(cls, queue_out, loop):
        logger.info("--------------------------------setup remote: %s", cls)
        async def log(remote):
            logger.info("exit: %s", cls)

        add_finalizer(log)


DEFAULT_CHANNEL_NAME = b''


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

            assert count == 3, 'A Chunk must be composed by two separators, e.g. `channel|uid|payload`!'
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

    def encode(self, eol=True, *, compressor=False):
        """Encode the chunk into a bytes string."""

        def _gen_parts():
            yield self.channel or DEFAULT_CHANNEL_NAME
            yield self.separator
            yield self.uid or b''
            yield self.separator
            yield compressor and compressor.encode() or b''
            yield self.separator

            data = self.data or b''
            if compressor in self.compressor:
                data = self.compressor[compressor].compress(data)
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

    def __init__(self, name=DEFAULT_CHANNEL_NAME, *, queue=None, loop=None, compressor=False):
        self.name = name
        self._loop = loop or asyncio.get_event_loop()
        self.queue = queue or asyncio.Queue(loop=loop)

        self.compressor = compressor

        self.futures = {}
        """Register a message uid to be a resolvable future.
        If that uid is received the futures result will be set.

        For this to work a task must iterate the channel.
        """

    async def __aiter__(self):
        return self

    async def __anext__(self):
        uid, data = await self.data_received()

        # trigger waiting futures
        if uid in self.futures:
            self.futures[uid].set_result(data)

        return uid, data

    @contextlib.contextmanager
    def waiting_for(self, uid):
        """Create a context with a future to wait for."""
        # TODO XXX does this make only sense for complete (json) messages?

        self.futures[uid] = future = asyncio.Future(loop=self._loop)

        yield future

        del self.futures[uid]

    async def data_received(self):
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

    async def inject_chunk(self, chunk, compressor=False):
        """Allows forwarding of a chunk as is into the queue."""

        await self.queue.put(chunk.encode(compressor=compressor))

    async def send(self, data, *, uid=None, compressor=False):
        """Create a chunk from data and uid and send it to the queue of this channel."""

        if isinstance(data, Chunk):
            data = data.data

        for part in split_data(data, self.max_size):
            chunk = Chunk(part, channel=self.name, uid=uid)

            await self.inject_chunk(chunk, compressor=compressor)

    async def send_eom(self, *, uid=None):
        """Send EOM (end of message) to the queue."""

        chunk = Chunk(None, channel=self.name, uid=uid)

        await self.queue.put(chunk.encode(compressor=False))


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

    async def data_received(self):
        """Wait until a message is completed by sending an empty chunk."""

        while True:
            # wait for the next data
            uid, chunk = await super(EncodedBufferChannel, self).data_received()

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

    async def send(self, data, *, uid=None, compressor=False):

        if isinstance(data, Chunk):
            data = data.data

        encoded_data = self.encode(data).encode()

        await super(EncodedBufferChannel, self).send(encoded_data, uid=uid, compressor=compressor)

    def message(self, uid=None):
        class _context:
            def __init__(self, channel):
                self.channel = channel

            async def __aenter__(self):
                return functools.partial(self.channel.send, uid=uid)

            async def __aexit__(self, exc_type, exc, tb):
                # send EOM
                await self.channel.send_eom(uid=uid)

        return _context(self)


class JsonChannel(EncodedBufferChannel):

    def decode(self, data):
        data = json.loads(data)
        return data

    def encode(self, data):
        data = json.dumps(data)
        return data


class Channels:

    """Distributes arbitrary chunks to channels.

    The empty channel is used to send Commands."""

    def __init__(self, *, loop=None, compressor=False):
        self._loop = loop or asyncio.get_event_loop()

        self._channels = {}
        self.add_channel(JsonChannel(loop=self._loop, compressor=compressor))

        self.default = self[DEFAULT_CHANNEL_NAME]

    async def distribute(self, line):
        channel, *_ = Chunk.view(line)

        if channel not in self._channels:
            logger.error('Channel `%s` not found for Chunk', channel)

        await self._channels[channel].forward(line)
        # logger.info("Chunk %s distributed to Channel `%s`", chunk, channel)

    def __getitem__(self, channel_name):
        try:
            return self._channels[channel_name]

        except KeyError:
            raise KeyError("Channel `{}` not found!".format(channel_name))

    def __setitem__(self, channel_name, channel):
        if channel_name in self._channels:
            logger.warning('Overriding existing channel `%s`', channel_name)

        self._channels[channel_name] = channel

    def __delitem__(self, channel_name):
        try:
            # logger.info('Removing channel `%s`', channel_name)
            del self._channels[channel_name]

        except KeyError:
            raise KeyError("Channel `{}` not found!".format(channel_name))

    def add_channel(self, channel):
        self[channel.name] = channel


class Command:

    """Execution context for all plugin commands"""

    def __init__(self, name, local, *,
                 local_setup=None, local_teardown=None,
                 remote=None, remote_setup=None, remote_teardown=None
                 ):
        self.name = name
        self.local = local
        self.local_setup = local_setup
        self.local_teardown = local_teardown
        self.remote = remote
        self.remote_setup = remote_setup
        self.remote_teardown = remote_teardown
        self.plugin = self._connect_plugin()
        self.queue = asyncio.Queue()
        self.queues = {}
        """holds all chunks per message uid"""

    def _connect_plugin(self):
        func_module = inspect.getmodule(self.local).__name__
        plugin = Plugin[func_module]
        plugin.commands[self.name] = self

        return plugin

    @reify
    def id(self):
        return ':'.join((self.plugin.module_name, self.name))

    def __repr__(self):
        return "<Command {}@{}>".format(self.name, self.plugin.module_name)

    async def execute(self, queue_out, queue_in, *args, **kwargs):
        """startes execution of command at the local side."""

        # XXX we need to take ownership of queue_in of remote instance

        return await self.local(self, queue_out, queue_in, command_args=args, command_kwargs=kwargs)


class Commander:

    """
    Execute specific messages/commands

    and return a result by replying in the same channel and with the same uid.
    """

    commands = {}

    def __init__(self, channel_out, channel_in, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        self.channel_out = channel_out
        self.channel_in = channel_in

    async def resolve_pending(self):
        """Completes all RPC style commands by resolving a future per message uid."""

        async for _ in self.channel_in:
            # XXX TODO log incomming?
            pass

    async def execute(self, name, *args, **kwargs):
        """Send a command to channel_out and wait for returning a result at channel_in."""

        if name not in self.commands:
            raise KeyError('Command not found: {}'.format(name))

        command = self.commands[name]
        partial_command = functools.partial(self.rpc, command)

        try:
            result = await command.local(partial_command, *args, **kwargs)

            return result

        except TypeError as ex:
            raise

    async def rpc(self, command, *args, **kwargs):
        # return if nothing for remote todo
        if not command.remote:
            return

        msg = {
            'command': command.name,
            'args': args,
            'kwargs': kwargs,
        }

        uid = uuid.uuid1().hex.encode()

        with self.channel_in.waiting_for(uid) as future:
            async with self.channel_out.message(uid) as send:
                await send(msg, compressor='gzip')

            result = await future
            logger.info("result: %s", result)
            return result

    @classmethod
    async def receive(cls, channel_in, channel_out):
        """The remote part of execute method."""

        # logger.info("Retrieving commands...")

        async for uid, message in channel_in:
            logger.info("retrieved command: %s", message)

            # do something and return result
            command = cls.commands[message['command']]

            cmd_args = message.get('args', [])
            cmd_kwargs = message.get('kwargs', {})

            result = await command.remote(*cmd_args, **cmd_kwargs)

            result = {
                'foo': result
            }

            async with channel_out.message(uid) as send:
                await send(result, compressor='gzip')

    @classmethod
    def command(cls, name=None):
        """Decorates a command."""

        command_name = name

        def decorator(func):

            if command_name is None:
                name = func.__name__

            else:
                name = command_name

            logger.debug("\t%s, new command: %s", id(cls), name)

            cls.commands[name] = Command(name, local=func)

            def remote_decorator(remote_func):
                cls.commands[name] = Command(name, local=func, remote=remote_func)

                return remote_func

            func.remote = remote_decorator

            return func

        return decorator


@Commander.command()
async def import_plugin(cmd, plugin_name):
    plugin = Plugin[plugin_name]

    code = inspect.getsource(plugin.module)
    module_name = plugin.module.__name__

    project_name, entry_point_name = plugin.name.split('#')

    result = await cmd(code, project_name, entry_point_name, module_name)

    return result


@import_plugin.remote
async def import_plugin(code, project_name, entry_point_name, module_name):
    plugin = Plugin.create_from_code(code, project_name, entry_point_name, module_name)
    Plugin.add(plugin)

    # collect commands
    logger.debug("--> %s, commands: %s", id(Commander), Commander.commands)
    commands = list(Commander.commands.keys())

    return {
        'commands': commands,
        'plugins': list(Plugin.plugins.keys())
    }


@Plugin.command()
async def command(command, queue_out, queue_in, command_args, command_kwargs):
    channel = JsonChannel(command, queue=queue_out)
    pass


@command.local_setup
async def command(command, remote, loop):
    """Distribute remote stdout to channels."""


@command.remote
async def command(command, *, queue_out, command_args, command_kwargs):
    pass


async def receive(cls, channel_in, channel_out):
    """The remote part of execute method."""

    # logger.info("Retrieving commands...")

    async for uid, message in channel_in:
        logger.info("retrieved command: %s", message)

        # do something and return result
        command = cls.commands[message['command']]

        cmd_args = message.get('args', [])
        cmd_kwargs = message.get('kwargs', {})

        result = await command.remote(*cmd_args, **cmd_kwargs)

        result = {
            'foo': result
        }

        async with channel_out.message(uid) as send:
            await send(result, compressor='gzip')


@command.remote_setup
async def command(command, queue_out, loop):
    """Starts distribution of incomming chunks."""

    channel_out = JsonChannel(queue=queue_out)

    # our incomming command queue
    queue_in = asyncio.Queue(loop=loop)
    channel_in = JsonChannel(queue=queue_in)

    # a queue for each plugin command message
    plugin_command_queues = {}

    async def execute_commands():
        async for uid, message in channel_in:
            logger.info("retrieved command: %s", message)

            # do something and return result
            cmd_args = message.get('args', [])
            cmd_kwargs = message.get('kwargs', {})

            result = await command.remote(
                command,
                queue_out=queue_out,
                command_args=cmd_args,
                command_kwargs=cmd_kwargs
            )

            result = {
                'foo': result
            }

            async with channel_out.message(uid) as send:
                await send(result, compressor='gzip')

    async def distribute_commands():
        async with Incomming(pipe=sys.stdin) as reader:
            while True:
                line = await reader.readline()
                if line is b'':
                    break

                channel, uid, compressor, data = Chunk.view(line)

                logger.debug("\t\tincomming: %s", line)

                # split command channel from other plugin channels
                # we want to execute a command
                if channel == command.id:
                    await channel_in.forward(line)

                else:
                    # distribute to plugin channels
                    await channels.distribute(line)

    logger.debug("remote setup %s %s", command, queue_out)

    future = asyncio.gather(execute_commands(), distribute_commands())
    await future


@Plugin.command()
async def echo(command, *, queue_out, queue_in, command_args, command_kwargs):


    return await cmd(*args, **kwargs)


@echo.remote
async def echo(*args, **kwargs):
    return {
        'args': args,
        'kwargs': kwargs
    }

async def copy_large_file(channel_out, *args, src=None, dest=None, **kwargs):
    pass


async def copy_large_file_remote(channel_in, *args, src=None, dest=None, **kwargs):
    pass


async def run(*tasks, loop=None):
    """Schedules all tasks and wait for running is done or canceled"""

    # create indicator for running messenger
    running = asyncio.Future(loop=loop)

    shutdown_tasks = []

    for task in tasks:
        fut = asyncio.ensure_future(task, loop=loop)

        def _shutdown_on_error(future):
            try:
                # retrieve result or exception
                future.result()

            except Exception as ex:
                # delegate tsk error to running future to signal exit
                # logger.error(traceback.format_exc())
                running.set_exception(ex)

                # raise

        fut.add_done_callback(_shutdown_on_error)
        shutdown_tasks.append(fut)

    # exit on sigterm or sigint
    for signame in ('SIGINT', 'SIGTERM'):
        sig = getattr(signal, signame)
        loop.add_signal_handler(sig, functools.partial(running.set_result, sig))

    # wait for running completed
    result = await running

    # cleanup
    for task in shutdown_tasks:
        if task.done():
            continue

        log("cancel", task=task)
        task.cancel()
        await asyncio.wait_for(task, None)

    return result


def main(compressor=False, **kwargs):

    # TODO channels must somehow connected with commands
    channels = Channels(loop=loop, compressor=compressor)

    queue_out = asyncio.Queue(loop=loop)
    channel_out = JsonChannel(channels.default.name, queue=queue_out, compressor=compressor)

    # setup all plugin commands
    loop.run_until_complete(Cmd.remote_setup(queue_out, loop))
    try:
        loop.run_until_complete(
            run(
                send_outgoing_queue(queue_out, sys.stdout),
                loop=loop
            )
        )

    finally:
        loop.close()


def decode_options(b64):
    return json.loads(base64.b64decode(b64).decode())


def encode_options(**options):
    return base64.b64encode(json.dumps(options).encode()).decode()


if __name__ == '__main__':
    main()
