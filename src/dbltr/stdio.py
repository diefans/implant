"""Test stdio pipes"""

import os
import sys
import asyncio
import signal
import atexit
import logging

logging.basicConfig(level='DEBUG')


def cancel_tasks():
    for task in asyncio.Task.all_tasks():
        print("task", task)
        task.cancel()


def create_loop(*, debug=False):
    """Create the appropriate loop."""

    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)

    else:
        loop = asyncio.get_event_loop()

    loop.set_debug(debug)

    # cancel tasks on exit
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            cancel_tasks
        )

    # close loop on exit
    def finalize():
        loop.close()

    # atexit.register(finalize)
    return loop


# loop is global
loop = create_loop(debug=True)

# pipes are global so we have global queues
stdin = asyncio.Queue(loop=loop)
stdout = asyncio.Queue(loop=loop)
stderr = asyncio.Queue(loop=loop)


async def queue_read_pipe(loop, reader, transport, queue):
    try:
        while True:
            line = await reader.readline()
            if line is b'':
                break
            await queue.put(line)

    except asyncio.CancelledError:
        print("cancel", queue, reader)
        # transport.close()


async def queue_write_pipe(loop, writer, transport, queue):
    try:
        while True:
            data = await queue.get()
            writer.write(data)
            await writer.drain()
            queue.task_done()

    except asyncio.CancelledError:
        print("cancel", queue, writer)
        # transport.close()


async def queue_write_stderr(loop, queue):
    try:
        while True:
            data = await queue.get()
            sys.stderr.write((b' <<< '.join((b'err', data)) + b'\n').decode())
            sys.stderr.buffer.flush()
            queue.task_done()

    except asyncio.CancelledError:
        pass


async def setup_queues(loop):

    stdout_transport, stdout_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin,
        os.fdopen(sys.stdin.fileno(), 'wb')
    )
    stdout_writer = asyncio.streams.StreamWriter(stdout_transport, stdout_protocol, None, loop)

    stdin_reader = asyncio.StreamReader(loop=loop)
    stdin_transport, stdin_protocol = await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(stdin_reader),
        os.fdopen(sys.stdin.fileno(), 'rb')
    )

    future = asyncio.gather(
        queue_read_pipe(loop, stdin_reader, stdin_transport, stdin),
        queue_write_pipe(loop, stdout_writer, stdout_transport, stdout),
        queue_write_pipe(loop, stdout_writer, stdout_transport, stderr),
        # queue_write_stderr(loop, stderr),
        loop=loop
    )
    await future


async def send(*data, queue=stdout):
    for date in data:
        await queue.put(date)


async def send_stdin_to_stderr():
    while True:
        data = await stdin.get()
        await send(data, queue=stderr)
        await send(data)
        stdin.task_done()


async def fix():
    return None


def main():
    try:

        asyncio.ensure_future(send(b'foo', b'bar', queue=stderr))
        asyncio.ensure_future(send_stdin_to_stderr())
        loop.run_until_complete(setup_queues(loop))

        loop.run_until_complete(fix())
    except asyncio.CancelledError as ex:
        pass

    finally:
        print("closing")
        try:
            loop.close()

        except Exception as ex:
            print(ex)


if __name__ == '__main__':
    main()
