"""Remote connection is established by a `Connector`."""
import abc
import asyncio
import logging
import os
import shlex
import sys
import traceback

from debellator import core, bootstrap


log = logging.getLogger(__name__)


class RemoteMisbehavesError(Exception):

    """Exception is raised, when a remote process seems to be not what we expect."""


class Remote:

    """A remote process."""

    def __init__(self, transport, protocol, *, loop=None):
        self.loop = loop if loop is None else asyncio.get_event_loop()
        self._transport = transport
        self._protocol = protocol
        self.stdin = protocol.stdin
        self.stdout = protocol.stdout
        self.stderr = protocol.stderr
        self.pid = transport.get_pid()

        self.io_queues = core.Channels(reader=protocol.stdout, writer=protocol.stdin, loop=self.loop)
        self.dispatcher = core.Dispatcher(self.io_queues, loop=self.loop)

        self._lck_communicate = asyncio.Lock(loop=self.loop)

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.pid)

    @property
    def returncode(self):
        return self._transport.get_returncode()

    async def wait(self):
        """Wait until the process exit and return the process return code."""
        return await self._transport._wait()    # pylint: disable=W0212

    def send_signal(self, signal):
        self._transport.send_signal(signal)

    def terminate(self):
        self._transport.terminate()

    def kill(self):
        self._transport.kill()

    async def execute(self, *args, **kwargs):
        # forward to dispatcher
        return await self.dispatcher.execute(*args, **kwargs)

    async def communicate(self):
        async with self._lck_communicate:
            never_ending = asyncio.Future(loop=self.loop)

            async def enqueue():
                try:
                    await self.io_queues.enqueue()
                except Exception as ex:
                    never_ending.set_exception(ex)

            async def dispatch():
                try:
                    await self.dispatcher.dispatch()
                except Exception as ex:
                    never_ending.set_exception(ex)

            fut_enqueue = asyncio.ensure_future(enqueue(), loop=self.loop)
            fut_dispatch = asyncio.ensure_future(dispatch(), loop=self.loop)

            try:
                result = await never_ending

            except asyncio.CancelledError:
                log.info("Remote communication cancelled.")
                log.info("Send shutdown: %s", self)
                shutdown_event = core.ShutdownRemoteEvent()
                event = self.execute(core.NotifyEvent, event=shutdown_event)
                await event

                fut_dispatch.cancel()
                await fut_dispatch
                # self.dispatcher.shutdown()

                fut_enqueue.cancel()
                await fut_enqueue
                # self.io_queues.shutdown()
                await self.wait()
                log.info("Shutdown end.")

            except Exception:
                log.error("Error while processing:\n%s", traceback.format_exc())
                raise

            finally:
                log.info("Remote process terminated")
                # terminate if process is still running

                if self.returncode is None:
                    log.warning("Terminating remote process: %s", self.pid)
                    self.terminate()
                    await self.wait()
                return self.returncode


class Connector:
    pass


class SubprocessConnector(Connector, metaclass=abc.ABCMeta):

    """A `Connector` uniquely defines a remote target."""

    __slots__ = ()

    def __init__(self, *, loop=None):
        self.loop = loop if loop is None else asyncio.get_event_loop()

    def __hash__(self):
        return hash(frozenset(map(lambda k: (k, getattr(self, k)), self.__slots__)))

    def __eq__(self, other):
        return hash(self) == hash(other)

    @staticmethod
    def bootstrap_code(code=core, options=None):
        """Create the python bootstrap code."""
        if code is None:
            code = core
        bootstrap_code = str(bootstrap.Bootstrap(code, options))
        return bootstrap_code

    @abc.abstractmethod
    def arguments(self, *, code=None, options=None, python_bin=sys.executable):
        """Iterate over the arguments to start a process.

        :param code: the code to bootstrap the remote process
        :param options: options for the remote process

        """

    async def launch(self, *, code=None, options=None, python_bin=sys.executable, **kwargs):
        """Launch a remote process.

        :param code: the python module to bootstrap
        :param options: options to send to remote
        :param python_bin: the path to the python binary to execute
        :param kwargs: further arguments to create the process

        """
        if options is None:
            options = {}

        # TODO handshake
        options['echo'] = echo = b''.join((b'Debellator', os.urandom(64)))

        *command_args, bootstrap_code = self.arguments(
            code=code, options=options, python_bin=python_bin
        )
        log.debug("Connector arguments: %s", ' '.join(command_args))

        remote = await create_subprocess_remote(*command_args, bootstrap_code, loop=self.loop, **kwargs)

        # TODO protocol needs improvement
        # some kind of a handshake, which is independent of sending echo via process options
        try:
            # wait for remote behavior to echo
            remote_echo = await remote.stdout.readexactly(len(echo))
            assert echo == remote_echo, "Remote process misbehaves!"

        except AssertionError:
            raise RemoteMisbehavesError("Remote does not echo `{}`!".format(echo))

        except EOFError:
            errors = []
            async for line in remote.stderr:
                errors.append(line)
            log.error("Remote close stdout on bootstrap:\n%s", (b''.join(errors)).decode('utf-8'))
            raise RemoteMisbehavesError("Remote closed stdout!", errors)

        log.info("Started remote process: %s", remote)
        return remote


_DEFAULT_LIMIT = 2 ** 16


async def create_subprocess_remote(program, *args, loop=None, limit=_DEFAULT_LIMIT, **kwds):
    if loop is None:
        loop = asyncio.events.get_event_loop()

    def preexec_detach_from_parent():
        # prevents zombie processes via ssh
        os.setpgrp()

    def protocol_factory():
        return asyncio.subprocess.SubprocessStreamProtocol(limit=limit, loop=loop)

    transport, protocol = await loop.subprocess_exec(
        protocol_factory,
        program, *args,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        preexec_fn=preexec_detach_from_parent,
        **kwds
    )
    return Remote(transport, protocol)


class Local(SubprocessConnector):

    """A `Connector` to a local python process."""

    __slots__ = ('sudo',)

    def __init__(self, *, sudo=None, loop=None):
        super().__init__(loop=loop)
        self.sudo = sudo

    def arguments(self, *, code=None, options=None, python_bin=sys.executable):
        bootstrap_code = self.bootstrap_code(code, options)

        # sudo
        if self.sudo:
            yield 'sudo'
            # optionally with user
            if self.sudo is not True:
                yield from ('-u', self.sudo)

        yield from (str(python_bin), '-c', bootstrap_code)


class Ssh(Local):

    """A `Connector` for remote hosts reachable via SSH.

    If a hostname is omitted, this connector acts like `Local`.
    """

    __slots__ = ('sudo', 'hostname', 'user')

    def __init__(self, *, hostname=None, user=None, sudo=None, loop=None):
        super().__init__(sudo=sudo, loop=loop)
        self.hostname = hostname
        self.user = user

    def arguments(self, *, code=None, options=None, python_bin=sys.executable):
        *local_arguments, _, _, bootstrap_code = super().arguments(
            code=code, options=options, python_bin=python_bin
        )

        # ssh
        if self.hostname is not None:
            bootstrap_code = shlex.quote(bootstrap_code)
            yield from ('ssh', '-T')
            # optionally with user
            if self.user is not None:
                yield from ('-l', self.user)

            # # remote port forwarding
            # yield '-R'
            # yield '10001:localhost:10000'

            yield self.hostname

        yield from local_arguments
        yield from (str(python_bin), '-c', bootstrap_code)


class Lxd(Ssh):

    """A `Connector` for accessing a lxd container.

    If the hostname is omitted, the lxd container is local.
    """

    __slots__ = ('sudo', 'hostname', 'user', 'container')

    def __init__(self, *, container, hostname=None, user=None, sudo=None, loop=None):
        super().__init__(hostname=hostname, user=user, sudo=sudo, loop=loop)
        self.container = container

    def arguments(self, *, code=None, options=None, python_bin=sys.executable):
        *ssh_arguments, _, _, bootstrap_code = super().arguments(
            code=code, options=options, python_bin=python_bin
        )

        yield from ssh_arguments
        yield from shlex.split('''lxc exec {self.container} {python_bin} -- -c'''.format(**locals()))
        yield bootstrap_code
        # yield from (
        #     '(', 'lxc', 'exec', self.container,
        #     str(python_bin), '--', '-c', bootstrap_code,
        #     '||', 'printf', '"\xff\xff"', ')'
        # )
