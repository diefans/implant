"""Controlles a bunch of remotes."""

import asyncio
import logging
import pathlib
import signal
import sys
import traceback

from debellator import core, connect, commands


log = logging.getLogger(__name__)
PLUGINS_ENTRY_POINT_GROUP = 'debellator.plugins'


def parse_command(line):
    """Parse a command from line."""
    args = []
    kwargs = {}
    command, *parts = line.split(' ')

    for part in parts:
        if '=' in part:
            k, v = part.split('=')
            kwargs[k] = v

        else:
            args.append(part)

    return command, args, kwargs


async def _execute_command(io_queues, line):
    default_lines = {
        b'e\n': (b'debellator.commands:Echo data=bar\n', {}),
        b'i\n': (b'debellator.core:InvokeImport fullname=debellator.commands\n', {}),
        b'\n': (b'debellator.commands:Echo data=bar\n', {}),
    }

    if line in default_lines:
        line, _ = default_lines[line]

    command_name, _, params = parse_command(line[:-1].decode())
    log.info("sending: %s %s", command_name, params)

    try:
        result = await io_queues.execute(command_name, **params)

    except Exception as ex:     # noqa
        log.error("Error:\n%s", traceback.format_exc())
    else:
        return result


async def log_remote_stderr(remote):
    # await remote.launched()
    log.info("Logging remote stderr: %s", remote)
    async for line in remote.stderr:
        log.debug("\tRemote #%s: %s", remote.pid, line[:-1].decode())


def log_signal():
    log.debug("signal")


class Console:
    def __init__(self, *, loop=None, **options):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.options = options
        self.connectors = {
            # connect.Local(): {
            #     'python_bin': pathlib.Path('~/.pyenv/versions/3.5.2/bin/python').expanduser(),
            # },
            # connect.Ssh(hostname='localhost'): {
            #     'python_bin': pathlib.Path('~/.pyenv/versions/3.5.2/bin/python').expanduser(),
            # },
            connect.Lxd(
                container='zesty',
                hostname='localhost'
            ): {
                'python_bin': pathlib.Path('/usr/bin/python3').expanduser()
            },
        }

    async def connect(self):
        remotes = {}
        for connector, default_args in self.connectors.items():
            if remotes.get(connector, None) is not None:
                log.warning('Process for %s already launched! Skipping...', connector)
                continue
            remote = await connector.launch(
                options=self.options, **default_args
            )
            fut_remote = asyncio.ensure_future(remote.communicate(), loop=self.loop)
            error_log = asyncio.ensure_future(log_remote_stderr(remote), loop=self.loop)
            remotes[connector] = (remote, fut_remote, error_log)

        return remotes

    async def run(self):
        remotes = await self.connect()

        feeder = asyncio.ensure_future(self.feed_stdin_to_remotes(remotes), loop=self.loop)
        # our real SIGINT handler
        self.loop.add_signal_handler(signal.SIGINT, feeder.cancel)
        await feeder

    async def feed_stdin_to_remotes(self, remotes):
        try:
            # asyncio.get_event_loop().add_signal_handler(signal.SIGINT, current_task.cancel)
            async with core.Incomming(pipe=sys.stdin, loop=self.loop) as reader:
                while True:
                    line = await reader.readline()

                    if line == b'':
                        break

                    result = await asyncio.gather(
                        *(_execute_command(remote, line) for remote, *_ in remotes.values()),
                        loop=self.loop
                    )
                    print("< {}\n >".format(result), end="")

        except asyncio.CancelledError:
            log.info("Terminating...")
            for remote, fut_remote, error_log in remotes.values():
                fut_remote.cancel()
                returncode = await fut_remote
                log.info("Remote %s exited with: %s", remote, returncode)
                error_log.cancel()
                await error_log


def main(debug=False, log_config=None):
    loop = asyncio.get_event_loop()
    # replace existing signal handler with noop as long as our remotes are not fully running
    # otherwise cancellation of process startup will lead to orphaned remote processes
    def noop():
        pass
    loop.add_signal_handler(signal.SIGINT, noop)

    options = {
        'debug': debug,
        'log_config': log_config,
        # 'venv': False,
        # 'venv': True,
        # 'venv': '~/.debellator',
    }

    if debug:
        log.setLevel(logging.DEBUG)

    console = Console(loop=loop, **options)
    try:
        loop.run_until_complete(
            console.run(),
        )

    except Exception as ex:
        core.log.error("Error %s:\n%s", type(ex), traceback.format_exc())

    finally:
        for task in asyncio.Task.all_tasks():
            if not task.done():
                log.info("pending: %s", task)
        log.info(" close " * 10)
        loop.close()
