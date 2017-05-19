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


def iter_sorted_dists():
    """Topologically sort distributions by their dependencies."""
    env = pkg_resources.Environment()
    # see http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/
    unsorted = {key: dist[0].requires() for key, dist in env._distmap.items()}     # pylint: disable=W0212

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


@interface.implementer(interfaces.IDependencies)
class Dependencies(collections.OrderedDict):

    """A sorted version of all installed dependencies."""

    def __init__(self):
        super(Dependencies, self).__init__(iter_sorted_dists())

    def sorted_entry_points(self, group, name=None):
        """Lookup entry points and yield them in the order of dist dependencies."""
        keys = list(self)

        def key(item):
            return keys.index(item.dist.key)

        entry_points = sorted(pkg_resources.iter_entry_points(group, name=name), key=key)
        return entry_points


def register_namespaces(root=None, entry_point_group=ENTRY_POINT_GROUP_SPECS):
    if root:
        ns = interfaces.INamespace(pathlib.Path(root))
        ns.config_specs()

        # we register root as a default namespace
        component.provideUtility(ns)

    # iterate over entry points
    for res in pkg_resources.iter_entry_points(entry_point_group):
        log.debug('Register namespace for %s(%s)', type(res), res)
        ns = interfaces.INamespace(res)
        ns.config_specs()
        component.provideUtility(ns, name=str(ns))


def find_registered_namespaces():
    return {key: ns for key, ns in component.getUtilitiesFor(interfaces.INamespace)}


def find_definitions():
    namespaces = find_registered_namespaces()

    # for ns_name, ns in namespaces.items():
    #     for spec_name, spec in ns.items():
    #         for def_name in spec:
    #             yield ns_name, spec_name, def_name

    definitions = {
        (ns_name, spec_name, def_name): definition
        for ns_name, ns in namespaces.items()
        for spec_name, spec in ns.items()
        for def_name, definition in spec.items()
    }
    return definitions


def load_registry_entry_points(entry_point_group=ENTRY_POINT_GROUP_REGISTRY):
    log.debug('Iterate over entry point group: %s', entry_point_group)
    dependencies = component.getUtility(interfaces.IDependencies)
    for entry_point in dependencies.sorted_entry_points(entry_point_group):
        try:
            entry_point_cb = entry_point.load()
            entry_point_cb()

        except Exception as ex:     # pylint: disable=W0703
            import traceback
            log.error('Error `%s` while loading entry point: %s\n%s',
                      ex, entry_point, traceback.format_exc())


def bootstrap(root=None):
    component.provideUtility(Dependencies())

    # this will provide adapters
    load_registry_entry_points()

    # this will load all namespaces
    register_namespaces(root)
