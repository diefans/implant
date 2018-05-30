"""Provide a pytest fixture for testing commands.

The :py:obj:`implant_remote_task` fixture is provided as if it is a running
:py:obj:`implant.connect.Remote` instance. You can execute a
:py:obj:`implant.core.Command` passing the whole stack.

"""
import asyncio
import os

import pytest
from async_generator import async_generator, yield_

from implant import connect, core


class RemoteTask(connect.Remote):

    """In-process remote task.

    Useful to run implant for testing.
    """

    def __init__(self, remote_core_fut, *, stdin, stdout):
        super().__init__(stdin=stdin, stdout=stdout)
        self.remote_core_fut = remote_core_fut

    async def wait(self):
        await self.remote_core_fut


class PipeConnector(connect.Connector):

    """A connector which executes the remote core
    in a task in the current process.
    """

    def __init__(self, *, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.stdin_pipe = os.pipe()
        self.stdout_pipe = os.pipe()
        self.stderr_pipe = os.pipe()

    async def launch(self, *args, **kwargs):
        """Launch the remote."""
        remote = await create_pipe_remote(self.stdin_pipe, self.stdout_pipe,
                                          self.stderr_pipe, loop=self.loop)
        return remote


async def create_pipe_remote(stdin_pipe, stdout_pipe, stderr_pipe,
                             *, loop=None):
    """Launch remote core as a background task."""
    if loop is None:
        loop = asyncio.events.get_event_loop()

    stdin_r, stdin_w = stdin_pipe
    stdout_r, stdout_w = stdout_pipe
    _, stderr_w = stderr_pipe

    remote_core = core.Core(loop=loop)
    remote_core_fut = asyncio.ensure_future(
        remote_core.connect(stdin=stdin_r, stdout=stdout_w, stderr=stderr_w)
    )

    reader = core.Incomming(pipe=stdout_r)
    await reader.connect()
    writer = await core.Outgoing(pipe=stdin_w).connect()
    remote = RemoteTask(remote_core_fut, stdin=writer, stdout=reader)
    return remote


@pytest.fixture
@async_generator
async def implant_remote_task(event_loop):
    """Create the remote task as a fixture.

    You use it like:

    .. code-block:: python

        @pytest.mark.asyncio
        async def test_testing(implant_remote_task):
            from implant import core

            result = await implant_remote_task.execute(
                core.InvokeImport, fullname='implant.commands')
            result = await implant_remote_task.execute(
                'implant.commands:Echo', data='foobar')

            assert result['remote_data'] == 'foobar'

    """
    connector = PipeConnector(loop=event_loop)
    remote = await connector.launch()
    com_remote = asyncio.ensure_future(remote.communicate())
    await yield_(remote)
    com_remote.cancel()
    await com_remote
