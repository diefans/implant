"""Controlles a bunch of remotes."""

import asyncio
import functools
import logging
import pathlib
import signal
import sys
import traceback

from debellator import core, connect

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
        b'e\n': (b'debellator.plugins.core:Echo foo=bar bar=123\n', {}),
        b'i\n': (b'debellator.core:InvokeImport fullname=debellator.plugins.core\n', {}),
        b'\n': (b'debellator.core:Echo foo=bar bar=123\n', {}),
    }

    if line in default_lines:
        line, _ = default_lines[line]

    command_name, _, params = parse_command(line[:-1].decode())
    log.info("sending: %s %s", command_name, params)

    try:
        cmd = core.Command.commands[command_name](**params)
        result = await io_queues.execute(cmd)

    except Exception as ex:     # noqa
        log.error("Error:\n%s", traceback.format_exc())
    else:
        return result


async def log_remote_stderr(remote):
    await remote.launched()
    log.info("Logging remote stderr: %s", remote.process)
    async for line in remote.process.stderr:
        log.debug("\tRemote #%s: %s", remote.process.pid, line[:-1].decode())


async def feed_stdin_to_remotes(**options):
    current_task = asyncio.Task.current_task()
    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, current_task.cancel)

    connectors = {
        connect.Local(): {
            'python_bin': pathlib.Path('~/.pyenv/versions/3.5.2/bin/python').expanduser(),
        },
        connect.Ssh(hostname='localhost'): {
            'python_bin': pathlib.Path('~/.pyenv/versions/3.5.2/bin/python').expanduser(),
        },
        connect.Lxd(
            container='zesty',
            hostname='localhost'
        ): {
            'python_bin': pathlib.Path('/usr/bin/python3').expanduser()
        },
    }

    remotes = {}
    remote_error_logs = set()

    for connector, default_args in connectors.items():
        if remotes.get(connector, None) is not None:
            log.warning('Process for %s already launched! Skipping...', connector)
            continue
        remote = remotes[connector] = connect.Remote(connector)
        asyncio.ensure_future(
            remote.launch(
                options=options, **default_args
            )
        )
        remote_error_logs.add(asyncio.ensure_future(log_remote_stderr(remote)))

    try:
        async with core.Incomming(pipe=sys.stdin) as reader:
            while True:
                line = await reader.readline()

                if line == b'':
                    break

                result = await asyncio.gather(
                    *(_execute_command(remote, line) for remote in remotes.values())
                )

                print("< {}\n >".format(result), end="")

    except asyncio.CancelledError:
        log.info("Terminating...")

        await asyncio.gather(
            *(remote.send_shutdown() for remote in remotes.values())
        )

        for remote_error_log in remote_error_logs:
            remote_error_log.cancel()
            await remote_error_log


async def serve_tcp_10000(reader, writer):
    try:
        while True:
            writer.write(b"Hello World\n")
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        writer.close()


async def print_debug(loop):
    while True:
        print(chr(27) + "[2J")  # clear screen
        loop.print_debug_info()
        await asyncio.sleep(0.5, loop=loop)


def main(debug=False, log_config=None):

    loop = asyncio.get_event_loop()

    options = {
        'debug': debug,
        'log_config': log_config,
        # 'venv': False,
        # 'venv': True,
        # 'venv': '~/.debellator',
    }

    if debug:
        log.setLevel(logging.DEBUG)

    try:
        loop.run_until_complete(
            feed_stdin_to_remotes(**options),
        )

    except Exception as ex:
        core.log.error("Error %s:\n%s", type(ex), traceback.format_exc())

    finally:
        loop.close()
