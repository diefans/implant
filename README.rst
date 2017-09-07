debellator
**********

A proof-of-concept for asynchronous adhoc remote procedure calls in Python.

This is work in progress and serves basically as an exercise.


Features
========

- Python 3.5 asyncio

- adhoc transferable remote procedures

- events

- quite small core

- tests


Limitations
===========

- Python 3.5

- only pure Python modules are supported for remote import, if no venv is used

- `Commands` must reside in a module other then `__main__`



Example
=======

.. code:: python

    import asyncio
    import pathlib

    from debellator import core, connect, commands


    async def remote_tasks():
        # create a connector for a python process
        connector = connect.Lxd(
            container='zesty',
            hostname='localhost'
        )
        connector_args = {
            'python_bin': pathlib.Path('/usr/bin/python3')
        }
        # connect to a remote python process
        remote = await connector.launch(**connector_args)

        # start remote communication tasks
        com_remote = asyncio.ensure_future(remote.communicate())
        try:
            # execute command
            cmd = commands.SystemLoad()
            result = await remote.execute(cmd)

            print("Remote system load:", result)

        finally:
            # stop communication tasks
            com_remote.cancel()
            await com_remote


    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(remote_tasks())
        loop.close()


Motivation
^^^^^^^^^^

This project is born by my work, insights and frustrations with Ansible, which I think is a total mess in terms of issue management and code in every aspect.
