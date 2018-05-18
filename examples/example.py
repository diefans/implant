import asyncio
import pathlib

from implant import core, connect, commands


async def remote_tasks():
    # connector = connect.Local()

    connector = connect.Lxd(
        container='zesty',
        hostname='localhost'
    )
    connector_args = {
        'python_bin': pathlib.Path('/usr/bin/python3').expanduser()
    }
    remote = await connector.launch(**connector_args)

    # setup launch specific tasks
    com_remote = asyncio.ensure_future(remote.communicate())
    try:
        # import commands in remote side
        cmd_import = core.InvokeImport(fullname='implant.commands')
        result = await remote.execute(cmd_import)

        # execute command
        cmd = commands.SystemLoad()
        result = await remote.execute(cmd)

        print("Remote system load:", result)

    finally:
        com_remote.cancel()
        await com_remote


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(remote_tasks())
    loop.close()
