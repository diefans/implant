import functools
import os
from collections import OrderedDict

import pkg_resources
import yaml

root_definitions = OrderedDict()


class YAMLSyntaxError(yaml.error.MarkedYAMLError):

    """A general syntax error."""


class DuplicateDefinition(YAMLSyntaxError):

    """Defines a duplicate key in a yaml dictionary."""


class OrderedLoader(yaml.loader.Loader):

    """Create orderewd dictionaries by default."""

    def construct_yaml_map(self, node):
        data = OrderedDict(self.construct_pairs(node))
        yield data

    def construct_no_duplicate_yaml_map(self, node):
        data = OrderedDict(self.generate_no_duplicate_pairs(node))
        yield data

    def generate_no_duplicate_pairs(self, node, deep=False):
        if not isinstance(node, yaml.nodes.MappingNode):
            raise yaml.constructor.ConstructorError(None, None,
                                                    "expected a mapping node, but found %s" % node.id,
                                                    node.start_mark)
        mapping_keys = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            value = self.construct_object(value_node, deep=deep)

            # store k/v for error reporting
            if key not in mapping_keys:
                mapping_keys[key] = (key_node, value_node, value)

            else:
                context_key_node, context_value_node, context_value = mapping_keys[key]

                raise DuplicateDefinition(
                    context='\nPrevious definition:\n{}: {}'.format(
                        context_key_node.value,
                        yaml.dump(context_value, default_flow_style=False)
                    ), context_mark=context_key_node.start_mark,
                    problem='\nConflicting definition:\n{}: {}'.format(
                        key_node.value,
                        yaml.dump(value, default_flow_style=False),
                    ),
                    problem_mark=key_node.start_mark,
                    note='Duplicate definition!'
                )

            yield key, value


class NamespaceMeta(type):

    """Provide a method to create a Namespace from a scalar value."""

    def from_scalar(cls, value):
        if value[0] in (os.path.sep, '.'):
            ns = DirectoryNamespace(value)
        else:
            ns = EntryPointNamespace.from_scalar(value)

        return ns


class Namespace(metaclass=NamespaceMeta):

    """A Namespace provides means to iterate over yaml files in subdirectories."""

    def listdir(self, resource_name):
        raise NotImplementedError()

    def isdir(self, resource_name):
        raise NotImplementedError()

    def open(self, resource_name):
        raise NotImplementedError()

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        assert isinstance(other, Namespace)
        return repr(self) == repr(other)

    def iter_sources(self, resource_name=''):
        resources = [resource_name]

        while resources:
            resource_name = resources.pop(0)

            if self.isdir(resource_name):
                resources.extend([
                    os.path.join(resource_name, child)
                    for child in self.listdir(resource_name)
                ])

            else:
                _, ext = os.path.splitext(resource_name)
                if ext in ('.yml', '.yaml'):
                    yield resource_name


class DirectoryNamespace(Namespace):

    """A Namespace pointing to the contents of a directory."""

    def __init__(self, root=None):
        self.root = root or './'

    def listdir(self, resource_name):
        return os.listdir(self.path(resource_name))

    def isdir(self, resource_name):
        return os.path.isdir(self.path(resource_name))

    def path(self, resource_name):
        return os.path.join(self.root, resource_name)

    def open(self, resource_name):
        return open(self.path(resource_name))

    def __repr__(self):
        return '{}:'.format(self.root)


class EntryPointNamespace(Namespace):

    """A Namespace pointing to the contents of a python package."""

    _entry_point_group = 'debellator.specs'

    def __init__(self, entry_point):
        self.entry_point = entry_point

        module_name = entry_point.module_name

        self.isdir = functools.partial(pkg_resources.resource_isdir, module_name)
        self.listdir = functools.partial(pkg_resources.resource_listdir, module_name)

    def path(self, resource_name):
        return pkg_resources.resource_filename(self.entry_point.module_name, resource_name)

    def open(self, resource_name):
        return pkg_resources.resource_stream(self.entry_point.module_name, resource_name)

    def __repr__(self):
        return '{0.dist.key}#{0.name}:'.format(self.entry_point)

    @classmethod
    def from_scalar(cls, value):
        dist, _, name = value.rpartition('#')
        if not dist:
            # take first ep found
            ep = list(pkg_resources.iter_entry_points(cls._entry_point_group, name))[0]

        else:
            ep = pkg_resources.get_entry_map(dist, group=cls._entry_point_group)[name]

        return cls(ep)


class Spec(OrderedDict):

    """A `Spec` is the top level dictionary of a yaml definition file.

    It contains the top level keys, which may be referenced by other specs.
    """

    def __init__(self, source, *, namespace=None):
        self.namespace = namespace or DirectoryNamespace()
        self.source = source

        super(Spec, self).__init__(self.load())

    def __hash__(self):
        return hash((self.namespace, self.source))

    def __eq__(self, other):
        assert isinstance(other, Spec)
        return self.namespace == other.namespace and self.source == other.source

    def __repr__(self):
        return '<Spec {0.namespace}{0.source}>'.format(self)

    def load(self):
        """Load a yaml source from the namespace."""
        with self.namespace.open(self.source) as spec_file:
            class SpecLoader(OrderedLoader):
                spec = self

                def construct_object(self, node, deep=False):
                    data = super(SpecLoader, self).construct_object(node, deep=deep)

                    # add spec to the definition
                    if isinstance(data, Definition):
                        data.spec = self.spec

                    return data

            definitions = yaml.load(spec_file, Loader=SpecLoader)
            return definitions


class Specs(dict):

    """An index for all namespaces and specs found."""

    def __init__(self, *roots):
        super(Specs, self).__init__()

        for root in roots:
            self.add_root(root)

    def add_root(self, root):
        if isinstance(root, pkg_resources.EntryPoint):
            namespace = EntryPointNamespace(root)

        else:
            namespace = DirectoryNamespace(root)

        if namespace not in self:
            namespace_specs = self[namespace] = {}
        else:
            namespace_specs = self[namespace]

        namespace_specs.update(
            (
                (resource_name, Spec(resource_name, namespace=namespace))
                for resource_name in namespace.iter_sources()
            )
        )



class Definition:

    """A Definition holds a reference to its spec."""

    def __init__(self):
        self.spec = None
