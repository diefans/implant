import abc
import asyncio
import logging
import os
import shlex
import sys

from debellator import core, bootstrap


log = logging.getLogger(__name__)


class ProcessNotLaunchedError(Exception):
    pass


class RemoteMisbehavesError(Exception):
    pass


class MetaRemote(type):
    processes = {}


class Remote(core.Dispatcher, metaclass=MetaRemote):

    """A unique representation of a Remote."""

    def __init__(self, connector):
        super().__init__()
        self.connector = connector
        self.process = None
        self._launch_lock = asyncio.Lock()

    def __hash__(self):
        return hash(self.connector)

    def __eq__(self, other):
        assert isinstance(other, Remote)
        return self.connector == other.connector

    async def launch(self, *, code=None, options=None, python_bin=sys.executable, **kwargs):
        """Launch a remote process.

        :param code: the python module to bootstrap
        :param options: options to send to remote
        :param python_bin: the path to the python binary to execute
        :param kwargs: further arguments to create the process

        """

        async with self._launch_lock:
            if options is None:
                options = {}

            options['echo'] = echo = b''.join((b'Debellator', os.urandom(64)))

            *command_args, bootstrap_code = self.connector.arguments(
                code=code, options=options, python_bin=python_bin
            )
            log.debug("Connector arguments: %s", ' '.join(command_args))

            process = await self._launch(*command_args, bootstrap_code, **kwargs)

            log.info("Started process: %s", process)

            # TODO protocol needs improvement
            # some kind of a handshake, which is independent of sending echo via process options
            try:
                # wait for remote behavior to echo
                remote_echo = await process.stdout.readexactly(len(echo))
                assert echo == remote_echo, "Remote process misbehaves!"

            except AssertionError:
                raise RemoteMisbehavesError("Remote does not echo `{}`!".format(echo))

            except EOFError:
                raise RemoteMisbehavesError("Remote closed stdout!")

            self.process = process
            try:
                await self.communicate(reader=process.stdout, writer=process.stdin)
            finally:
                process.terminate()
                await process.wait()

    async def _launch(self, *args, **kwargs):
        def preexec_detach_from_parent():
            # prevents zombie processes via ssh
            os.setpgrp()

        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            preexec_fn=preexec_detach_from_parent,
            **kwargs
        )

        return process

    def cancel(self):
        self._fut_communicate.cancel()

    async def __await__(self):
        if self._fut_communicate is not None:
            await self._fut_communicate

    async def relaunch(self):
        """Wait until terminated and start a new process with the same args."""
        # process = self.__class__.processes.get(self)
        process = self.process
        if not process:
            raise ProcessNotLaunchedError()

        command_args = process._transport._proc.args
        command_kwargs = process._transport._proc.kwargs

        process.terminate()
        await process.wait()

        self.process = await self._launch(*command_args, **command_kwargs)
        return self.process


class Connector(metaclass=abc.ABCMeta):

    __slots__ = ()

    def __hash__(self):
        return hash(frozenset(map(lambda k: (k, getattr(self, k)), self.__slots__)))

    def __eq__(self, other):
        return hash(self) == hash(other)

    @staticmethod
    def bootstrap_code(code=None, options=None):
        """Create the python bootstrap code."""
        # we default to our core
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


class Local(Connector):

    __slots__ = ('sudo',)

    def __init__(self, *, sudo=None):
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

    __slots__ = ('sudo', 'hostname', 'user')

    def __init__(self, *, hostname=None, user=None, sudo=None):
        super().__init__(sudo=sudo)
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
    __slots__ = ('sudo', 'hostname', 'user', 'container')

    def __init__(self, *, container, hostname=None, user=None, sudo=None):
        super().__init__(hostname=hostname, user=user, sudo=sudo)
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
