"""Main idea of processing resp. evolving a definition upon a target system.

.. graphviz::

    digraph overview {
        "settings scope"-> request;
        definition -> request;
        request -> loop;
        loop -> target;
    }


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
import asyncio
import collections
import datetime
import itertools
import pathlib
import traceback
import logging

import pkg_resources
from zope import interface, component

from . import interfaces, scope, config

log = logging.getLogger(__name__)


@interface.implementer(interfaces.IIncommingQueue)
class Incomming(asyncio.Queue):
    pass


@interface.implementer(interfaces.IRequest)
class Request:
    def __init__(self, definition, *, scope=None, source=None):
        self.definition = interfaces.IReference(definition)
        self.time = datetime.datetime.utcnow()
        self.source = source
        self.scope = scope

    # def __hash__(self):
    #     """A unique hash for this request to lock duplicates."""

    #     return (hash(self.definition), hash(self.scope))


def create_incomming_queue():
    incomming = Incomming()
    component.provideUtility(incomming)

    return incomming


async def process_request(request):
    log.info('Processing request: %s', request)




async def process_incomming(*, server=False):
    incomming = component.getUtility(interfaces.IIncommingQueue)
    request_locks = collections.defaultdict(asyncio.Lock)

    log.info("processing... %s", incomming)

    while True:
        request = await incomming.get()
        log.debug("request: %s", request)

        # try to process request
        async with request_locks[hash(request)]:
            result = await process_request(request)

        if not server and incomming.empty():
            log.info('Finished all requests.')
            break


def main(*, definitions, settings=None, root=None):
    """Run the evolution."""

    # settings registration
    if settings is None:
        settings = {}

    component.provideUtility(settings, interfaces.ISettings)
    log.debug('Settings: %s', settings)

    config.bootstrap(root)
    incomming = create_incomming_queue()

    # here we have a clean state, where all is prepared, but nothing done yet
    loop = asyncio.get_event_loop()

    # put requests into incomming
    for definition in definitions:

        req = Request(definition, scope=scope.Scope(), source='CLI')
        loop.run_until_complete(incomming.put(req))

    evolution = loop.run_until_complete(process_incomming(server=settings.get('server', False)))


def setup():
    from . import specs, definitions

    specs.register_adapters()
    definitions.register_adapters()
