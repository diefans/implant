import abc
import sys

from debellator import core, bootstrap


class Connector(metaclass=abc.ABCMeta):

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
                yield '-u'
                yield self.sudo

        yield str(python_bin)
        yield '-c'
        yield bootstrap_code


class Ssh(Local):

    __slots__ = ('sudo', 'hostname', 'user')

    def __init__(self, *, hostname=None, user=None, sudo=None):
        super().__init__(sudo=sudo)
        self.hostname = hostname
        self.user = user

    def arguments(self, *, code=None, options=None, python_bin=sys.executable):
        *local_arguments, _, _, bootstrap_code = super().arguments(code=code, options=options, python_bin=python_bin)

        # ssh
        if self.hostname is not None:
            bootstrap_code = "'{}'".format(bootstrap_code)
            yield 'ssh'
            yield '-T'
            # optionally with user
            if self.user is not None:
                yield '-l'
                yield self.user

            # # remote port forwarding
            # yield '-R'
            # yield '10001:localhost:10000'

            yield self.hostname

        yield from local_arguments

        yield str(python_bin)
        yield '-c'
        yield bootstrap_code
