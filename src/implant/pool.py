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


class RemotesThreadPool(RemotesPool):

    """A threaded version of `RemotesPool`.

    I have to test if this concept makes sense at all:
    - scenario with a lot of remotes scales not with one loop in one thread

    see https://docs.python.org/3/library/asyncio-subprocess.html#subprocess-and-threads
    see https://github.com/python/asyncio/issues/390#issuecomment-235915330
        > policy = asyncio.get_event_loop_policy()
        > watcher = asyncio.SafeChildWatcher()
        > watcher.attach_loop(loop)
        > policy.set_child_watcher(watcher)

    """

    def __init__(self, options):
        super().__init__(options)
        self.worker = Worker()
        self.worker.start()

    def get_next_loop(self):
        """Start nect thread and return its loop."""
        return self.worker.loop

    async def run(self, connector, command_cls, command_params):
        # connect
        loop = self.get_next_loop()
        future = asyncio.run_coroutine_threadsafe(
            self.connect(connector), loop)
        remote = future.result()

        # execute
        future = asyncio.run_coroutine_threadsafe(
            remote.execute(command_cls, **command_params), loop)
        result = future.result()
        print("RESULT:", result)

        # loop.call_soon_threadsafe(pool.worker.evt_shutdown.set)
        # print("*" * 100, "shutdown")
        future = asyncio.run_coroutine_threadsafe(self.stop(), loop)
        # result = future.result()
        # print(' JOIN ', * 5)
        self.worker.thread.join()

    async def connect(self, connector, **kwargs):
        """Connect to a remote and pool it."""
        loop = asyncio.get_event_loop()
        print('thread loop', id(loop))
        if connector in self:
            remote, _, _ = self[connector]
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
        loop = asyncio.get_event_loop()
        _, fut_remote, error_log = self[connector]
        loop.call_soon_threadsafe(fut_remote.cancel)
        await fut_remote
        loop.call_soon_threadsafe(error_log.cancel)
        await error_log
        loop.call_soon_threadsafe(self.worker.evt.evt_shutdown.set)

    async def stop(self):
        # XXX FIXME TODO waits indefinitely, since
        # the watcher for the PID gets registered in the wrong loop
        # see asyncio.unix_events._UnixSelectorEventLoop._make_subprocess_transport
        loop = asyncio.get_event_loop()
        for _, fut_remote, error_log in self.values():
            loop.call_soon_threadsafe(fut_remote.cancel)
            await fut_remote
            loop.call_soon_threadsafe(error_log.cancel)
            await error_log
        loop.call_soon_threadsafe(self.worker.evt.evt_shutdown.set)


class Worker:

    """A loop in a thread."""

    def __init__(self):
        self.thread = threading.Thread(target=self._run_event_loop_forever)
        self.loop = asyncio.new_event_loop()
        self.evt_shutdown = asyncio.Event(loop=self.loop)

    def _run_event_loop_forever(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.evt_shutdown.wait())
        self.loop.stop()

    def start(self):
        """Start the thread."""
        self.thread.start()
