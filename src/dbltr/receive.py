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

import structlog


log = structlog.get_logger()


@functools.lru_cache(maxsize=1)
def get_event_loop():
    """:returns: platform specific event loop"""

    # create loop
    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()  # for subprocess pipes on Windows
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    return loop


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

    def __new__(cls, payload, uid=None, complete=True):
        return super(Message, cls).__new__(cls, payload)

    def __init__(self, payload, uid=None, complete=True):
        super(Message, self).__init__()
        self.uid = uid or uuid.uuid1()
        self.complete = complete

    def __hash__(self):
        return self.uid.int

    def __eq__(self, msg):
        return hash(self) == hash(msg)

    @property
    def jsonified(self):
        return json.dumps(self.payload)

    def encode(self):
        """Create a serialized version of that message.

        If the payload is greater than self.limit it is split at that boundary.
        """
        b64_jsonified = base64.b64encode(self.jsonified)

        # for split_msg in split_size(b64_jsonified, self.limit):


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
            uid, payload, *complete = line[:-1].split(b':')
            return Message(
                base64.b64decode(payload),
                uid=uuid.UUID(bytes(uid).decode()),
                complete=complete
            )


class MessageReaderProtocol(asyncio.StreamReaderProtocol):

    async def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self._stream_reader.readline()

        if msg is None:
            # eof
            raise StopAsyncIteration

        return msg


class Messenger:
    """
    A `Messenger` provides an iterator for incomming message chunks
    """

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        self._reader = MessageStreamReader(loop=self._loop)
        self._protocol = MessageReaderProtocol(self._reader)

        self._receiver = {}
        """Holds receiver instances for each `Message.uid`"""

    async def __aenter__(self):
        _, protocol = await self._loop.connect_read_pipe(lambda: self._protocol, sys.stdin)
        return protocol

    async def __aexit__(self, type, value, traceback):
        if type:
            raise value

    async def receive(self):
        async with self as stream:
            async for msg in stream:
                print("received:", msg.uid, msg)

                # find receiver
                if msg.uid in self._receiver:
                    receiver = self._receiver[msg.uid]
                else:
                    receiver = ControlMessageReceiver()
                    self._receiver[msg.uid] = receiver

                    def _finalize():
                        log.info('finalize receiver', uuid=id(msg))
                        del self._receiver[msg.uid]

                    self._loop.create_task(receiver.complete(_finalize))

                receiver.message_received(msg)


class MessageReceiver:

    """Base class for message receiving."""

    def __init__(self):
        self._waiter = asyncio.Future()

    def message_received(self, msg):
        """Handle an incomming message."""

        self.add_payload(msg)

        if msg.complete:
            self._waiter.set_result(msg.uid)

    def add_payload(self, payload):
        """Add payload to complete the message."""

    async def complete(self, finish_cb=None):
        """Process all the message payload."""

        try:
            log.info("wait for completed message")
            uid = await self._waiter
            log.info("completed message", uuid=uid)

        finally:
            if finish_cb:
                finish_cb()


class CompleteMessageReceiver(MessageReceiver):

    """Collect all parts of that message."""

    def __init__(self):
        pass


class ControlMessageReceiver(MessageReceiver):

    """Collects control messages and enqueues them."""


def main(args):

    loop = get_event_loop()

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
    print('foo')
    main(sys.argv)
