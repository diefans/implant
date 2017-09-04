debellator
**********

A proof-of-concept for asynchronous adhoc remote procedure calls in Python.

This is work in progress and serves basically as an exercise.


Limitations
===========

- Python 3.5

- only pure python modules are supported for remote import, if no venv is used

- `Commands` must reside in a module other then `__main__`



Example
=======

.. code:: python

    import asyncio
    import pathlib

    from debellator import core, connect, commands


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
            cmd_import = core.InvokeImport(fullname='debellator.commands')
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


Motivation
^^^^^^^^^^

This project is born by my work, insights and frustrations with Ansible, which I think is a total mess in terms of issue management and code in every aspect.
