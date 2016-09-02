"""
Receiver for debellator.

http://stackoverflow.com/questions/23709916/iterating-over-asyncio-coroutine
http://sahandsaba.com/understanding-asyncio-node-js-python-3-4.html

http://sametmax.com/open-bar-sur-asyncio/


see https://github.com/python/asyncio/issues/314 for Exception on exit

"""

import sys
import base64
import uuid
import json
import functools
import asyncio
import signal


def log(msg, **kwargs):
    if kwargs:
        print("{msg} {kwargs}".format(msg=msg, kwargs=kwargs), file=sys.stderr, flush=True)

    else:
        print("{msg}".format(msg=msg), file=sys.stderr, flush=True)


def split_size(s, size=1024):
    """Split a string into pieces."""

    if size < 1:
        raise ValueError('size must be greater than 0', size)

    length = len(s)
    remains = length % size

    for i in (i * size for i in range(length // size)):
        yield s[i: i + size]

    if length < size:
        yield s

    elif remains:
        yield s[i + size: i + size + remains]


class Message(bytes):

    """
    A Message is a base64 encoded payload.
    """

    limit = 1024

    def __new__(cls, payload, channel=None, uid=None, complete=True):
        return super(Message, cls).__new__(cls, payload)

    def __init__(self, payload, channel=None, uid=None, complete=True):
        super(Message, self).__init__()

        # enforce None for empty strings
        self.channel = channel or None
        self.uid = uid or uuid.uuid1()
        self.complete = complete

    def __hash__(self):
        return self.uid.int

    def __eq__(self, other):
        if isinstance(other, Message):
            return hash(self) == hash(other)

        return bytes(self) == other

    def __repr__(self):
        return b':'.join((
            self.channel or b'',
            self.uid.hex.encode(),
            bytes(self)
        )).decode()

    def send(self, fd=sys.stdout):
        """Encode message to be send to stdout"""

        print(repr(self), file=fd, flush=True)

    @classmethod
    def from_line(cls, line):
        """Create a message from a protocol line.

        incomplete message: <channel>:<uid>:<payload>\n
        complete message: <channel>:<uid>:<payload>:\n

        """

        try:
            channel, uid, payload, *complete = line[:-1].split(b':')
            uid = uuid.UUID(bytes(uid).decode())

        except ValueError:
            # not enough colons
            log("\nbroken message protocol!", line=line)
            uid = None
            channel = None
            payload = line[:-1]
            complete = True

        else:
            complete = bool(complete)
            channel = bytes(channel)

        try:
            # optimistic aproach
            message = Message(
                base64.b64decode(payload),
                channel=channel,
                uid=uid,
                complete=complete
            )

        except base64.binascii.Error:
            # payload is not encoded
            message = Message(payload, complete=True)

        return message


class MessageStreamReader(asyncio.StreamReader):
    async def readline(self):
        if self._exception is not None:
            raise self._exception

        line = bytearray()
        not_enough = True

        while not_enough:
            while self._buffer and not_enough:
                ichar = self._buffer.find(b'\n')
                if ichar < 0:
                    line.extend(self._buffer)
                    self._buffer.clear()
                else:
                    ichar += 1
                    line.extend(self._buffer[:ichar])
                    del self._buffer[:ichar]
                    not_enough = False

                if len(line) > self._limit:
                    self._maybe_resume_transport()
                    raise ValueError('Line is too long')

            if self._eof:
                break

            if not_enough:
                await self._wait_for_data('readline')

        self._maybe_resume_transport()
        # we return the Message here
        if line:
            message = Message.from_line(line)

            return message


class MessageReaderProtocol(asyncio.StreamReaderProtocol):

    async def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self._stream_reader.readline()

        if msg is None:
            # eof
            raise StopAsyncIteration

        return msg


class Channel:

    """A `Channel` receives certain messages dedicated to it."""

    def __init__(self, receiver_factory, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        self._receiver_factory = receiver_factory
        self._receiver = {}
        """Holds receiver instances for each `Message.uid`"""

    async def finalize(self, uid, receiver):
        log('finalize receiver', uid=uid)

        # remove receiver instance
        del self._receiver[uid]

    async def receive(self, msg):
        # find receiver
        if msg.uid in self._receiver:
            receiver = self._receiver[msg.uid]
        else:
            receiver = self._receiver_factory(loop=self._loop)
            self._receiver[msg.uid] = receiver

            # wait for completed message
            self._loop.create_task(receiver.complete(self.finalize))

        await receiver.message_received(msg)


class PingChannel(Channel):

    async def finalize(self, uid, receiver):
        log("data", data=receiver.data())

        msg = Message(receiver.data(), uid=uid)
        msg.send()


class Messenger:

    """
    A `Messenger` provides an iterator for incomming message chunks
    """

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        self._reader = MessageStreamReader(loop=self._loop)
        self._protocol = MessageReaderProtocol(self._reader)

        self._channels = {
            None: PingChannel(JoinMessageReceiver, loop=self._loop)
        }

        self._receiver = {}
        """Holds receiver instances for each `Message.uid`"""

    async def __aenter__(self):
        _, protocol = await self._loop.connect_read_pipe(lambda: self._protocol, sys.stdin)
        return protocol

    async def __aexit__(self, type, value, traceback):
        if type:
            raise value

    async def receive(self):
        """Main loop for receiving messages."""

        async with self as stream:
            async for msg in stream:
                log("received: {}".format(repr(msg)))

                # find channel
                if msg.channel not in self._channels:
                    # no channel found
                    log("No channel found, ignoring...", message=msg)
                    continue

                channel = self._channels[msg.channel]

                await channel.receive(msg)


class MessageReceiver:

    """Base class for message receiving."""

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._waiter = asyncio.Future(loop=self._loop)

    async def message_received(self, msg):
        """Handle an incomming message."""

        await self.add_payload(msg)

        if msg.complete:
            self._waiter.set_result(msg)

    async def add_payload(self, payload):
        """Add payload to complete the message."""

    def data(self):
        """:returns: the data, the reciver has to expose."""

    async def complete(self, finish_cb=None):
        """Process all the message payload."""

        try:
            log("wait for completed message")
            msg = await self._waiter
            log("completed message", uuid=msg.uid)

        finally:
            if finish_cb:
                await finish_cb(msg.uid, self)


class JoinMessageReceiver(MessageReceiver):

    """Collect all parts of that message and exposes their data."""

    def __init__(self, loop=None):
        super(JoinMessageReceiver, self).__init__(loop=loop)

        self._messages = []

    async def add_payload(self, msg):
        log("append", id=id(self), data=msg)
        self._messages.append(msg)

    def data(self):
        data = bytearray()
        log("collect", data=self._messages)
        for msg in self._messages:
            data.extend(msg)

        return data


class ControlMessageReceiver(MessageReceiver):

    """Collects control messages and enqueues them."""


def main(args):

    loop = asyncio.get_event_loop()

    done = asyncio.Future(loop=loop)

    def ask_exit(signame):
        """stop loop on exit."""

        print("got signal %s: exit" % signame)
        done.set_result(None)
        loop.stop()

    # register shutdowen
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            functools.partial(ask_exit, signame)
        )
    try:
        msgr = Messenger(loop)
        loop.run_until_complete(msgr.receive())

    finally:
        loop.close()


if __name__ == '__main__':
    main(sys.argv)
