import asyncio
import os

from debellator import connect, core


class RemoteTask(connect.Remote):

    def __init__(self, remote_core_fut, *, stdin, stdout):
        super().__init__(stdin=stdin, stdout=stdout)
        self.remote_core_fut = remote_core_fut

    async def wait(self):
        await self.remote_core_fut


class PipeConnector(connect.Connector):
    def __init__(self, *, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.stdin_pipe = os.pipe()
        self.stdout_pipe = os.pipe()
        self.stderr_pipe = os.pipe()

    async def launch(self, *args, **kwargs):
        remote = await create_pipe_remote(self.stdin_pipe, self.stdout_pipe, self.stderr_pipe, loop=self.loop)
        return remote


async def create_pipe_remote(stdin_pipe, stdout_pipe, stderr_pipe, *, loop=None):
    if loop is None:
        loop = asyncio.events.get_event_loop()

    stdin_r, stdin_w = stdin_pipe
    stdout_r, stdout_w = stdout_pipe
    stderr_r, stderr_w = stderr_pipe

    remote_core = core.Core(loop=loop)
    remote_core_fut = asyncio.ensure_future(
        remote_core.connect(stdin=stdin_r, stdout=stdout_w, stderr=stderr_w)
    )

    reader = core.Incomming(pipe=stdout_r)
    await reader.connect()
    writer = await core.Outgoing(pipe=stdin_w).connect()
    remote = RemoteTask(remote_core_fut, stdin=writer, stdout=reader)
    return remote
