"""CLI scripts."""
import asyncio
import importlib
import logging
import logging.config
import threading

import click
import pkg_resources
import yaml

log = logging.getLogger(__name__)


def find_loop_specs():
    """Just find specs for common loops."""
    loop_specs = {
        module_name: importlib.util.find_spec(module_name)
        for module_name in ('asyncio', 'uvloop', 'tokio')
    }
    return loop_specs


def run():
    """Main entry point."""
    return cli(obj={}, auto_envvar_prefix='DEBELLATOR')


@click.group(invoke_without_command=True)
@click.option('event_loop', '--loop', default='asyncio',
              type=click.Choice(find_loop_specs().keys()),
              help='Use a different loop policy.')
@click.option('--debug/--no-debug', default=False, help='Enable or disable debug.')
@click.option('--log-config',
              type=click.File('r'),
              default=None,
              help='Logging configuration in yaml format.')
@click.option('--remote-log-config',
              type=click.File('r'),
              default=None,
              help='Logging configuration in yaml format.')
# @click.option('--config', '-c', default=None, help='General configuration.')
@click.pass_context
def cli(ctx, event_loop, debug, log_config, remote_log_config):
    if log_config is None:
        log_config = pkg_resources.resource_string('debellator', 'logging.yaml')

    if remote_log_config is None:
        remote_log_config = pkg_resources.resource_string('debellator', 'remote-logging.yaml')

    log_config = yaml.load(log_config)
    remote_log_config = yaml.load(remote_log_config)
    logging.config.dictConfig(log_config)

    if event_loop == 'uvloop':
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            log.info("Using uvloop event loop policy")

        except ImportError:
            log.warning("uvloop is not available.")

    elif event_loop == 'tokio':
        try:
            import tokio
            asyncio.set_event_loop_policy(tokio.EventLoopPolicy())
            log.info("Using tokio event loop policy")

        except ImportError:
            log.warning("tokio is not available.")

    if ctx.invoked_subcommand is None:
        # we need to import master lazy because master imports core,
        # which may use exclusive decorator, which tries to get the actual loop,
        # which is only set after running set_event_loop_policy
        from debellator import master

        if debug:
            log.info("Enable asyncio debug")
            master.core.log.setLevel(logging.DEBUG)
            asyncio.get_event_loop().set_debug(debug)

        master.main(log_config=remote_log_config, debug=debug)

    else:
        if debug:
            asyncio.get_event_loop().set_debug(debug)
            log.setLevel(logging.DEBUG)

    ctx.obj['log_config'] = log_config
    ctx.obj['remote_log_config'] = remote_log_config
    ctx.obj['debug'] = debug


@cli.command('cmd')
@click.option('dotted_command_name', '-c', '--command', required=True,
              help='the path to a debellator command')
@click.option('command_params_file', '-p', '--params', type=click.File('rb'))
@click.option('-r', '--remote', help='The remote connection', default='local://')
@click.pass_context
def cli_cmd(ctx, dotted_command_name, command_params_file, remote):
    """Execute a :py:obj:`debellator.core.Command` in a remote process."""
    from debellator import core, connect

    # lookup command
    module_name, command_name = dotted_command_name.split(':')
    module = importlib.import_module(module_name)
    command_cls = getattr(module, command_name)
    assert issubclass(command_cls, core.Command), \
        '{} is not a subclass of {}'.format(dotted_command_name, core.Command)

    connector = connect.Connector.create_connector(remote)
    command_params = yaml.load(command_params_file.read())

    options = {
        'log_config': ctx.obj['remote_log_config'],
        'debug': ctx.obj['debug']
    }

    pool = RemotesPool(options)

    loop = asyncio.get_event_loop()
    print('main loop', id(loop))
    # see https://docs.python.org/3/library/asyncio-subprocess.html#subprocess-and-threads
    # see https://github.com/python/asyncio/issues/390#issuecomment-235915330
    policy = asyncio.get_event_loop_policy()
    watcher = asyncio.SafeChildWatcher()
    watcher.attach_loop(loop)
    policy.set_child_watcher(watcher)

    loop.run_until_complete(pool.run(connector, command_cls, command_params))
    loop.stop()
    loop.close()


class RemotesPool:
    def __init__(self, options):
        self.options = options or {}
        self.remotes = {}

    def get_next_loop(self):
        """Just return the main loop."""
        loop = asyncio.get_event_loop()
        return loop

    async def run(self, connector, command_cls, command_params):
        # connect
        loop = self.get_next_loop()
        remote = await self.connect(connector)

        # execute
        result = await remote.execute(command_cls, **command_params)
        print("RESULT:", result)

        await self.stop()

    async def connect(self, connector, **kwargs):
        """Connect to a remote and pool it."""
        loop = asyncio.get_event_loop()
        print('thread loop', id(loop))
        if connector in self.remotes:
            remote, _, _ = self.remotes[connector]
            # TODO does this deserve a warning?
            log.warning('Process for %s already launched! Using: %s',
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
            self.remotes[connector] = (remote, fut_remote, error_log)

        return remote

    async def shutdown(self, connector):
        loop = asyncio.get_event_loop()
        remote, fut_remote, error_log = self.remotes[connector]
        loop.call_soon_threadsafe(fut_remote.cancel)
        await fut_remote
        loop.call_soon_threadsafe(error_log.cancel)
        await error_log

    async def stop(self):
        # XXX FIXME TODO waits indefinitely, since
        # the watcher for the PID gets registered in the wrong loop
        # see asyncio.unix_events._UnixSelectorEventLoop._make_subprocess_transport
        loop = asyncio.get_event_loop()
        for remote, fut_remote, error_log in self.remotes.values():
            loop.call_soon_threadsafe(fut_remote.cancel)
            await fut_remote
            loop.call_soon_threadsafe(error_log.cancel)
            await error_log

    async def log_remote_stderr(self, remote):
        """Just log remote stderr."""
        # await remote.launched()
        if remote.stderr:
            log.info("Logging remote stderr: %s", remote)
            async for line in remote.stderr:
                log.debug("\tRemote #%s: %s", remote.pid, line[:-1].decode())


class RemotesThreadPool(RemotesPool):
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
        future = asyncio.run_coroutine_threadsafe(self.connect(connector), loop)
        remote = future.result()

        # execute
        future = asyncio.run_coroutine_threadsafe(remote.execute(command_cls, **command_params), loop)
        result = future.result()
        print("RESULT:", result)

        # loop.call_soon_threadsafe(pool.worker.evt_shutdown.set)
        # print("*" * 100, "shutdown")
        future = asyncio.run_coroutine_threadsafe(self.stop(), loop)
        # result = future.result()
        # print(' JOIN ', * 5)
        pool.worker.thread.join()

    async def connect(self, connector, **kwargs):
        """Connect to a remote and pool it."""
        loop = asyncio.get_event_loop()
        print('thread loop', id(loop))
        if connector in self.remotes:
            remote, _, _ = self.remotes[connector]
            log.warning('Process for %s already launched! Using: %s',
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
            self.remotes[connector] = (remote, fut_remote, error_log)

        return remote

    async def shutdown(self, connector):
        loop = asyncio.get_event_loop()
        remote, fut_remote, error_log = self.remotes[connector]
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
        for remote, fut_remote, error_log in self.remotes.values():
            loop.call_soon_threadsafe(fut_remote.cancel)
            await fut_remote
            loop.call_soon_threadsafe(error_log.cancel)
            await error_log
        loop.call_soon_threadsafe(self.worker.evt.evt_shutdown.set)


class Worker:
    def __init__(self):
        self.thread = threading.Thread(target=self._run_event_loop_forever)
        self.loop = asyncio.new_event_loop()
        self.evt_shutdown = asyncio.Event(loop=self.loop)

    def _run_event_loop_forever(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.evt_shutdown.wait())
        self.loop.stop()

    def start(self):
        self.thread.start()
