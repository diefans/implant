import asyncio
from collections import namedtuple
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
                    (?P<c1>(?:[x80-\x9f])|(?<=\x1b)[\x40-\x5f]?)
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
    def __init__(self, seq):
        self.match = re_ansi.match(seq)
        if self.match:
            super().__init__(self.match.groupdict())

    @core.reify
    def has_tail(self):
        return self['tail'] is not ''

    @core.reify
    def is_c0(self):
        return self.match is not None and self['c0'] is not None

    @core.reify
    def is_c1(self):
        return self.match is not None and self['c1'] is not None

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

        self.history = bytearray()
        self.buffer = bytearray()
        self.queue = asyncio.Queue()

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
            encoded_char = char.encode()
            self.history.extend(encoded_char)
            self.buffer.extend(encoded_char)

            # # check for ansi escape sequence
            seq = self.buffer.decode()
            m = AnsiMatch(seq)
            if m.is_c0:
                if m.has_tail:
                    log.warning('Cleanup broken control function: %s', self.buffer)
                    self.buffer.clear()
                    continue

                if (m.is_control_sequence or m.is_control_string) and not m.is_complete:
                    continue

                # log.debug("sequence match: %s", seq)
                key = ansi_map.get(seq, Key(seq))
                asyncio.ensure_future(self.queue.put(key))
                self.buffer.clear()
            else:
                # no control function
                key = ansi_map.get(seq, Key(seq))
                asyncio.ensure_future(self.queue.put(key))
                self.buffer.clear()
        # log.debug('history: %s', self.history)

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


async def echo_console():
    async with core.Outgoing(pipe=sys.stdout) as writer:
        async with Console() as console:
            async for event in console:
                if event == Key('left'):
                    writer.write(b'\x1b[C')
                await writer.drain()


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

