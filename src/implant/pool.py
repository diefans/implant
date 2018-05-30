import asyncio
import logging
import threading


class RemotesPool(dict):

    """Has references to all created remotes."""

    # pylint: disable=E0602
    log = logging.getLogger(__module__ + '.' + __qualname__)

    def __init__(self, options, *, loop=None):
        super().__init__()
        self.options = options or {}
        self.loop = loop if loop is not None else asyncio.get_event_loop()

    def get_next_loop(self):
        """Just return the main loop."""
        return self.loop

    async def connect(self, connector, **kwargs):
        """Connect to a remote and pool it."""
        loop = self.get_next_loop()
        if connector in self:
            remote, _, _ = self[connector]
            # TODO does this deserve a warning?
            self.log.warning('Process for %s already launched! Using: %s',
                             connector, remote)
        else:
            remote = await connector.launch(
                options=self.options, **kwargs,
                loop=loop
            )
            fut_remote = asyncio.ensure_future(
                remote.communicate(), loop=loop)
            error_log = asyncio.ensure_future(
                self.log_remote_stderr(remote), loop=loop)
            self[connector] = (remote, fut_remote, error_log)

        return remote

    async def shutdown(self, connector):
        """Shutdown a remote identified by its connector."""
        loop = asyncio.get_event_loop()
        _, fut_remote, error_log = self[connector]
        loop.call_soon_threadsafe(fut_remote.cancel)
        await fut_remote
        loop.call_soon_threadsafe(error_log.cancel)
        await error_log

    async def stop(self):
        """Shutdown all remotes."""
        loop = asyncio.get_event_loop()
        for _, fut_remote, error_log in self.values():
            loop.call_soon_threadsafe(fut_remote.cancel)
            await fut_remote
            loop.call_soon_threadsafe(error_log.cancel)
            await error_log

    async def log_remote_stderr(self, remote):
        """Just log remote stderr."""
        # await remote.launched()
        if remote.stderr:
            self.log.info("Logging remote stderr: %s", remote)
            async for line in remote.stderr:
                self.log.debug("\tRemote #%s: %s", remote.pid, line[:-1].decode())

    @classmethod
    async def run_command_on_remotes(cls, *connectors,
                                     options, command_cls, command_params,
                                     loop=None):
        """Connect remotes and run the command."""
        remotes_pool = cls(options, loop=loop)
        # connect
        for connector in connectors:
            await remotes_pool.connect(connector)

        # execute
        results = {connector: await remote.execute(command_cls, **command_params)
                   for connector, (remote, *_) in remotes_pool.items()}

        await remotes_pool.stop()
        return results
