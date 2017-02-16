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

from . import definitions, specs

log = logging.getLogger(__name__)


ENTRY_POINT_GROUP_REGISTRY = 'debellator.registry'
ENTRY_POINT_GROUP_SPECS = 'debellator.specs'


class Registry:

    """Our adpater registry descriptor."""

    def __init__(self):
        # at the moment we work with global site, but the app should be working with Evolve.registry
        self.registry = component.globalSiteManager

    def __get__(self, inst, type):
        # always return our registry
        return self.registry


class Dependencies(collections.OrderedDict):

    """A sorted version of all installed dependencies."""

    def __init__(self):

        self.env = pkg_resources.Environment()
        super(Dependencies, self).__init__(self._iter_sorted_dists())
        self._keys = list(self)

    def _iter_sorted_dists(self):
        # see http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/
        unsorted = {key: dist[0].requires() for key, dist in self.env._distmap.items()}

        while unsorted:
            acyclic = False

            for name, requires in list(unsorted.items()):
                for require in requires:
                    if require.name in unsorted:
                        break
                else:
                    acyclic = True
                    yield name, requires
                    del unsorted[name]

            if not acyclic:
                raise RuntimeError('A cyclic dependency occurred!')

    def sorted_entry_points(self, group, name=None):
        """Lookup entry points and yield them in the order of dist dependencies."""
        def key(item):
            return self._keys.index(item.dist.key)

        entry_points = sorted(pkg_resources.iter_entry_points(group, name=name), key=key)
        return entry_points


@interface.implementer(interfaces.IEvolve)
class Evolve:

    registry = Registry()

    def __init__(self, root=None):

        # register self as utility
        self.registry.registerUtility(self)

        self.dependencies = Dependencies()

        self._load_registry_entry_points()

        self._register_namespaces(root)

        self.namespaces = {
            ns: {} for _, ns in self.registry.getUtilitiesFor(interfaces.INamespace)
        }

    def _register_namespaces(self, root=None):
        resources = itertools.chain(
            pkg_resources.iter_entry_points(ENTRY_POINT_GROUP_SPECS),
            (pathlib.Path(root),) if root else ()
        )
        for res in resources:
            log.debug('Register namespace for %s(%s)', type(res), res)
            ns = interfaces.INamespace(res)
            self.registry.registerUtility(ns, name=str(ns))

    def _load_registry_entry_points(self):
        log.debug('Iterate over entry point group: %s', ENTRY_POINT_GROUP_REGISTRY)
        for entry_point in self.dependencies.sorted_entry_points(ENTRY_POINT_GROUP_REGISTRY):
            try:
                entry_point_cb = entry_point.load()
                entry_point_cb(self.registry)

            except Exception as ex:
                log.error('Error `%s` while loading entry point: %s\n%s',
                          ex, entry_point, traceback.format_exc())

    def load_spec_entry_points(self):
        # load specs

        # create specs from entry points
        eps = pkg_resources.iter_entry_points(ENTRY_POINT_GROUP_SPECS)
        self.specs = specs.Specs(*eps)

        # add optional root directory
        if self.root:
            log.info("Lookup specs in %s", self.root)
            self.specs.add_root(self.root)

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
    # lookup and validate hosts
    evolve = Evolve(root=root)
    evolve.load_spec_entry_points()
    from pdb import set_trace; set_trace()       # XXX BREAKPOINT

    host_definition = evolve.lookup_host(host)
    target_definitions = [evolve.lookup_target(target) for target in targets]


def setup(registry):
    specs.register_adapters(registry)
