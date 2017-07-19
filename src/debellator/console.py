import asyncio
from collections import namedtuple
import itertools
import logging
import os
import re
import sys
import termios
import traceback
import tty

from debellator import core


log = logging.getLogger(__name__)


if hasattr(os, 'set_blocking'):
    def _set_nonblocking(fd):
        os.set_blocking(fd, False)

    def _get_blocking(fd):
        return os.get_blocking(fd)
else:
    import fcntl

    def _set_nonblocking(fd):
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        flags = flags | os.O_NONBLOCK
        fcntl.fcntl(fd, fcntl.F_SETFL, flags)


re_ansi = re.compile(r'''
    # see http://www.ecma-international.org/publications/files/ECMA-ST/Ecma-048.pdf
    (?:
        (?: (?P<c0>[\x00-\x1f])?
            (?:
                (?:
                    (?P<c1>(?:[\x80-\x9f])|(?<=\x1b)[\x40-\x5f]?)
                    (?:
                        # see chapter 5.4 Control sequences
                        (?P<cseq>(?<=[\x5b\x9b])
                            (?P<cseq_params>[\x30-\x3f]+)?
                            (?P<cseq_inter>(?<=[\x30-\x3f])[\x20-\x2f]+)?
                            (?P<cseq_final>[\x40-\x7e])?
                        )
                        |
                        # see chapter 5.6 Control strings
                        (?P<cstr>(?<=[\x5d\x9d]|[\x5f\x9f]|[\x50\x90]|[\x58\x98]|[\x5e\x9e])
                            (?P<cstr_chars>[\x08-\x0d\x20-\x7e]+)?
                            (?P<cstr_final>[\x07\x5c\x9c])?
                        )
                    )? # optional makes c1 an empty string if only \x1b matches
                )
                |
                # see chapter 5.5 Independent control functions
                (?P<indep>(?<=\x1b)[\x60-\x7e])
            )?
        )
    )?
    (?P<tail>.*)
''', re.VERBOSE + re.DOTALL)


class AnsiMatch(dict):
    def __init__(self, sequence):
        self.sequence = sequence
        self.match = re_ansi.match(sequence)
        if self.match:
            super().__init__(self.match.groupdict())

    @core.reify
    def has_tail(self):
        return self['tail'] != ''

    @core.reify
    def is_c0(self):
        return self.match is not None and self['c0'] is not None

    @core.reify
    def is_c1(self):
        return self.match is not None and self['c1'] is not None

    @core.reify
    def is_ansi(self):
        return self.is_c0 or self.is_c1 or self.is_independent

    @core.reify
    def is_control_sequence(self):
        return self.is_c1 or self['cseq_final'] or self['cseq'] is not None and not self.has_tail

    @core.reify
    def is_control_string(self):
        return self.is_c1 and self['cstr'] is not None

    @core.reify
    def is_independent(self):
        return self.match is not None and self['indep'] is not None

    @core.reify
    def is_control_function(self):
        return bool(
            sum(
                (self.is_c0,
                 self.is_c1,
                 self.is_independent,
                 self.is_control_sequence,
                 self.is_control_string)
            )
        )

    @core.reify
    def is_complete(self):
        return self.match and (self['cseq_final'] or self['cstr_final'])


class KeySequence(str):
    @core.reify
    def ansi(self):
        return AnsiMatch(self)


class Key(namedtuple('KeyPress', ['key', 'shift', 'ctrl', 'alt'])):
    __slots__ = ()

    def __new__(cls, key, shift=False, ctrl=False, alt=False):
        return super().__new__(cls, key, shift, ctrl, alt)


ansi_map = {
    '\x1b[A': Key('up'),
    '\x1b[B': Key('down'),
    '\x1b[C': Key('right'),
    '\x1b[D': Key('left'),
    '\x7f': Key('bs'),
    '\x0a': Key('return'),
    '\x0d': Key('c-return', ctrl=True),
    '\t': Key('tab'),
    '\x1b[3~': Key('delete'),
    '\x1b[2~': Key('insert'),
    '\x1b[F': Key('end'),
    '\x1b[H': Key('home'),
    '\x1b[5~': Key('page_up'),
    '\x1b[6~': Key('page_down'),
}


class Console:
    def __init__(self, stream=sys.stdin):
        self.stream = stream

        self._tty_settings = None
        self._is_blocking = None

        self.queue = asyncio.Queue()
        self.buffer = []

    async def __aenter__(self):
        loop = asyncio.get_event_loop()

        self._tty_settings = termios.tcgetattr(self.stream)
        self._is_blocking = os.get_blocking(self.stream.fileno())

        os.set_blocking(self.stream.fileno(), False)
        tty.setcbreak(self.stream)

        loop.add_reader(self.stream, self._reader)
        return self

    async def __aexit__(self, exc_type, value, tb):
        try:
            termios.tcsetattr(self.stream, termios.TCSADRAIN, self._tty_settings)
            os.set_blocking(self.stream.fileno(), self._is_blocking)
        finally:
            self._tty_settings = None
            self._is_blocking = None

    def _reader(self):
        while True:
            char = self.stream.read(1)
            if char == '':
                break
            self.buffer.append(char)

            # check for ansi escape sequence
            sequence = KeySequence(''.join(self.buffer))
            m = sequence.ansi
            if m.is_c0:
                if m.has_tail:
                    # log.warning('Broken control function: %s', sequence.encode())
                    self.buffer.clear()
                    continue

                if (m.is_control_sequence or m.is_control_string) and not m.is_complete:
                    # log.warning('Incomplete control function: %s', sequence.encode())
                    continue

            asyncio.ensure_future(self.queue.put(sequence))
            self.buffer.clear()

    async def __await__(self):
        """Wait for the next console event."""

        char = await self.queue.get()
        # self.history.extend(char)

        return char

    async def __aiter__(self):
        """Iterate over all console events."""
        return self

    async def __anext__(self):
        return await self


class KeyMap(dict):
    def add(self, sequence, *more_sequences):
        def decorator(fun):
            for seq in itertools.chain((sequence,), more_sequences):
                if seq in self:
                    log.warning('Sequence %s already mapped: %s', seq, self[seq])
                self[seq] = fun
            return fun

        return decorator

    def default(self, fun):
        if None in self:
            log.warning('Default already mapped: %s', self[None])

        self[None] = fun

        return fun


class Readline:

    """Read a meaningful command from stdin."""

    def __init__(self, console, writer):
        self.console = console
        self.writer = writer
        self.buffer = []

    key_map = KeyMap()

    @key_map.default
    async def default(self, sequence):
        if sequence.ansi.is_ansi:
            return
        self.writer.write(sequence.encode())
        await self.writer.drain()

    @key_map.add('\x7f')
    async def backspace(self, sequence):
        # send delete left
        pass

    @key_map.add('\n')
    async def enter(self, sequence):
        self.writer.write(b'\r\n')
        await self.writer.drain()

    @key_map.add('\x1b[3~')
    async def delete(self, sequence):
        self.writer.write(b'\b')
        await self.writer.drain()

    @key_map.add('\x1b[A', '\x1b[B', '\x1b[C', '\x1b[D')
    async def left(self, sequence):
        self.writer.write(sequence.encode())
        await self.writer.drain()

    async def __await__(self):
        while True:
            sequence = await self.console
            event_handler = self.key_map.get(sequence, self.key_map.get(None, None))

            if event_handler is not None:
                line = await event_handler(self, sequence)

                if line:
                    return line

    async def __aiter__(self):
        return self

    async def __anext__(self):
        return await self


async def echo_console():
    async with core.Outgoing(pipe=sys.stdout) as writer:
        async with Console() as console:
            async for cmd in Readline(console, writer):
                log.debug('Input: %s', cmd)


def main(debug=False, log_config=None):

    loop = asyncio.get_event_loop()

    options = {
        'debug': debug,
        'log_config': log_config,
    }

    if debug:
        log.setLevel(logging.DEBUG)

    try:
        loop.run_until_complete(
            echo_console()
        )

        # loop.run_until_complete(core.cancel_pending_tasks(loop))
    except Exception as ex:
        log.error("Error %s:\n%s", type(ex), traceback.format_exc())

    finally:
        loop.close()

