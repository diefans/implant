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
import logging

import pkg_resources
from zope import interface, component

from . import definitions, specs

log = logging.getLogger(__name__)


class Evolve:

    def __init__(self, root=None):

        # create specs from entry points
        eps = pkg_resources.iter_entry_points('debellator.specs')
        self.specs = specs.Specs(*eps)

        # add optional root directory
        if root:
            log.info("Lookup specs in %s", root)
            self.specs.add_root(root)

        self.targets = []
        self.hosts = []

        # find targets and hosts/groups
        for ns, resources in self.specs.items():
            log.info("Found %s in namespace %s", ', '.join(resources.keys()) or 'nothing', ns)

            for resource_name, spec in resources.items():
                log.debug("%s: %s", resource_name, spec)

                for name, definition in spec.items():
                    log.info('Definition %s found: %s', name, definition)

                if 'evolve' in spec:
                    log.info("evolving %s", spec)

    def lookup_host(self, host):
        """Find the host within all specs."""

    def lookup_target(self, target):
        """Find target in specs."""


def main(*targets, host=None, root=None):
    """Run the evolution."""

    # lookup and validate hosto
    evolve = Evolve(root=root)

    host_definition = evolve.lookup_host(host)
    target_definitions = [evolve.lookup_target(target) for target in targets]

