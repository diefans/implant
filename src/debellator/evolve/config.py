"""Configuration of the whole server."""

import collections
import logging
import pathlib
import pkg_resources
from zope import component, interface

from . import interfaces

ENTRY_POINT_GROUP_REGISTRY = 'debellator.registry'
ENTRY_POINT_GROUP_SPECS = 'debellator.specs'

log = logging.getLogger(__name__)


class Registry(component.globalregistry.BaseGlobalComponents):

    """Our adpater registry descriptor."""

    def __init__(self):
        super(Registry, self).__init__('evolve')

    def __get__(self, inst, cls):
        # always return our registry
        return self


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


@interface.implementer(interfaces.IConfiguration)
class Configuration:

    """Keep track of the passive state of plugins."""

    registry = Registry()

    def __init__(self, registry=None):

        # register self as utility
        self.registry = registry or Registry()

        self.dependencies = Dependencies()

    def register_namespaces(self, root=None):
        if root:
            ns = self.registry.getAdapter(pathlib.Path(root), interfaces.INamespace)
            ns.config_specs(self.registry)

            # we register root as a default namespace
            self.registry.registerUtility(ns)

        # iterate over entry points
        for res in pkg_resources.iter_entry_points(ENTRY_POINT_GROUP_SPECS):
            log.debug('Register namespace for %s(%s)', type(res), res)
            ns = self.registry.getAdapter(res, interfaces.INamespace)
            ns.config_specs(self.registry)
            self.registry.registerUtility(ns, name=str(ns))

    def load_registry_entry_points(self):
        log.debug('Iterate over entry point group: %s', ENTRY_POINT_GROUP_REGISTRY)
        for entry_point in self.dependencies.sorted_entry_points(ENTRY_POINT_GROUP_REGISTRY):
            try:
                entry_point_cb = entry_point.load()
                entry_point_cb(self.registry)

            except Exception as ex:     # pylint: disable=W0703
                import traceback
                log.error('Error `%s` while loading entry point: %s\n%s',
                          ex, entry_point, traceback.format_exc())
