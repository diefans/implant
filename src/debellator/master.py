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


async def _execute_command(io_queues, line, **kw):
    command_name, _, params = parse_command(line[:-1].decode())
    print("sending:", command_name, params)

    try:
        cmd = core.Command.commands[command_name](**params)
        result = await io_queues.execute(cmd)

    except Exception as ex:     # noqa
        log.error("Error:\n%s", traceback.format_exc())
        print("Error: {}\n > ".format(ex))

    else:
        return result


async def log_remote_stderr(remote):
    async for line in remote.stderr:
        log.debug("\tRemote #%s: %s", remote.pid, line[:-1].decode())


async def feed_stdin_to_remotes(**options):
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

    remote_futures = {}

    for connector, default_args in connectors.items():
        if remote_futures.get(connector, None) is not None:
            log.warning('Process for %s already launched! Skipping...', connector)
            continue
        remote_futures[connector] = asyncio.ensure_future(connect.Remote(connector).launch(
            options=options, **default_args
        ))

    default_lines = {
        b'e\n': (b'debellator.plugins.core:Echo foo=bar bar=123\n', {}),
        b'i\n': (b'debellator.core:InvokeImport fullname=debellator.plugins.core\n', {}),
        b'\n': (b'debellator.core:Echo foo=bar bar=123\n', {}),
    }

    # connector = connect.Ssh(
    #     hostname='localhost'
    # )
    connector = connect.Lxd(
        container='zesty',
        hostname='localhost'
    )
    remote = connect.Remote(connector)
    fut_remote = asyncio.ensure_future(
        remote.launch(
            # code=core,
            python_bin=pathlib.Path('/usr/bin/python3').expanduser(),
            # python_bin=pathlib.Path('~/.pyenv/versions/3.6.1/envs/dbltr-remote/bin/python').expanduser(),
            # python_bin=pathlib.Path('~/.pyenv/versions/3.5.2/bin/python').expanduser(),
            options=options
        )
    )

    # XXX FIXME TODO remote_com is a background task, so we have to await
    remote_err = asyncio.ensure_future(log_remote_stderr(remote.process))

    try:
        async with core.Incomming(pipe=sys.stdin) as reader:
            while True:
                line = await reader.readline()

                if line == b'':
                    break

                if line in default_lines:
                    line, kw = default_lines[line]

                if remote.process.returncode is None:
                    result = await _execute_command(remote, line, **kw)
                    # result = await asyncio.gather(
                    #     _execute_command(dispatcher, line, **kw),
                    #     _execute_command(dispatcher, line, **kw),
                    # )

                    print("< {}\n > ".format(result), end='')

    except asyncio.CancelledError:
        shutdown_event = core.ShutdownRemoteEvent()
        event = remote.execute(core.NotifyEvent(shutdown_event))
        await event
        core.log.info("Terminating process: %s", process)

        process.send_signal(signal.SIGKILL)

        remote_err.cancel()
        await remote_err

    if process.returncode is None:
        core.log.info("Terminating process: %s", process)
        # TODO implement gracefull remote shutdown
        # via Command
        process.terminate()
        await process.wait()


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


async def run(*tasks):
    """Schedule all tasks and wait for running is done or canceled."""
    # create indicator for running messenger
    running = asyncio.ensure_future(asyncio.gather(*tasks))

    # exit on sigterm or sigint
    for signame in ('SIGINT', 'SIGTERM', 'SIGHUP', 'SIGQUIT'):
        sig = getattr(signal, signame)

        def exit_with_signal(sig):
            try:
                log.info("\n\n\nAborting: %s", running)
                running.cancel()

            except asyncio.InvalidStateError:
                log.warning("running already done!")

        asyncio.get_event_loop().add_signal_handler(sig, functools.partial(exit_with_signal, sig))

    # wait for running completed
    try:
        result = await running
        return result

    except asyncio.CancelledError:
        raise


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
            run(
                # print_debug(loop),
                # asyncio.start_server(serve_tcp_10000, 'localhost', 10000),
                feed_stdin_to_remotes(**options),
            )
        )

        loop.run_until_complete(core.cancel_pending_tasks(loop))
    except Exception as ex:
        core.log.error("Error %s:\n%s", type(ex), traceback.format_exc())

    finally:
        loop.close()
