"""
Input to an evolution is the target, the host/fallback, its scope of variables

.. code-block:: yaml

    ---
    foo: !remote

    nginx: !copy
        src: tmp/foo
        dest: /tmp/bar


.. code-block:: python

    host_key = 'foo'
    target_key = 'nginx'

    host = IHost(specs, host_key)
    target = ITarget(specs, target_key)

    evolution = IEvolution(host, scope, target)

    result = await evolution




"""
import collections
import itertools
import pathlib
import traceback
import logging

import pkg_resources
from zope import interface, component

from . import interfaces
from .config import Configuration

log = logging.getLogger(__name__)


def main(*targets, host=None, root=None):
    """Run the evolution."""
    # lookup and validate hosts
    config = Configuration()
    config.registry.registerUtility(config)

    config.load_registry_entry_points()
    config.register_namespaces(root)

    import asyncio

    loop = asyncio.get_event_loop()

    # evolution = loop.run_until_complete(evolve.run())

    # create main loop
    # host_definition = evolve.lookup_host(host)
    # target_definitions = [evolve.lookup_target(target) for target in targets]


def setup(registry):
    from . import specs

    specs.register_adapters(registry)
