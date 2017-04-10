"""Specifications a.k.a. specs are a set of maybe exported toplevel definitions of a control flow.

Specs can be organized in separate files and directories.
Private specs are only accessible within the defining project/namespace itself.

"""
import collections
import itertools
import logging
import pathlib
import types
import weakref

import pkg_resources
import yaml
from zope import interface, component

from . import interfaces


log = logging.getLogger(__name__)


class YAMLSyntaxError(yaml.error.MarkedYAMLError):

    """A general syntax error."""


class DuplicateDefinition(YAMLSyntaxError):

    """Defines a duplicate key in a yaml dictionary."""


@interface.implementer(interfaces.IYamlLoader)
class ComponentLoader(yaml.loader.Loader):

    """Use component adapters for unknown tags.

    Create ordered dictionaries by default.
    """

    def __init__(self, stream):
        yaml.loader.Loader.__init__(self, stream)

        for tag, constructor in [
                (None, self.construct_by_components),
                ('tag:yaml.org,2002:map', self.create_ordered_dict),
                ('tag:yaml.org,2002:seq', self.create_list),
                ('tag:yaml.org,2002:str', self.create_str_scalar),
                ('tag:yaml.org,2002:int', self.create_int_scalar),
                ('tag:yaml.org,2002:float', self.create_float_scalar),
        ]:
            self.add_constructor(tag, constructor)

    @classmethod
    def construct_by_components(cls, loader, node):
        tag = node.tag

        constructor = None
        if isinstance(node, yaml.nodes.ScalarNode):
            constructor = cls.construct_scalar
        elif isinstance(node, yaml.nodes.SequenceNode):
            constructor = cls.construct_sequence
        elif isinstance(node, yaml.nodes.MappingNode):
            constructor = loader.create_ordered_dict

        if constructor:
            data = constructor(loader, node)

            df_data = component.queryAdapter(data, interfaces.IDefinition, tag)
            if df_data:
                return df_data

        data = component.queryMultiAdapter(
            (node, loader),
            interfaces.IYamlConstructor,
            name=tag
        )

        if data is None:
            data = component.queryMultiAdapter(
                (node, loader),
                interfaces.IYamlConstructor,
            )

        if data is None:
            log.error('Unable to construct `%s`', tag)

        return data

    @staticmethod
    def create_list(loader, node):
        return list(loader.generate_sequence(node))

    @staticmethod
    def create_ordered_dict(loader, node):
        return collections.OrderedDict(loader.generate_no_duplicate_pairs(node))

    @staticmethod
    def create_str_scalar(loader, node):
        return loader.construct_scalar(node)

    @staticmethod
    def create_int_scalar(loader, node):
        return int(loader.construct_scalar(node))

    @staticmethod
    def create_float_scalar(loader, node):
        return float(loader.construct_scalar(node))

    def generate_sequence(self, node, deep=False):
        if not isinstance(node, yaml.nodes.SequenceNode):
            raise yaml.constructor.ConstructorError(None, None,
                                                    "expected a sequence node, but found %s" % node.id,
                                                    node.start_mark)
        return (self.construct_object(child, deep=deep)
                for child in node.value)

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


class SpecLoader(ComponentLoader):

    """Add a spec to the definitions."""

    def __init__(self, *args, spec=None, **kwargs):
        super(SpecLoader, self).__init__(*args, **kwargs)
        self.spec = spec

    def construct_object(self, node, deep=False):
        data = super(SpecLoader, self).construct_object(node, deep=deep)

        # add spec to the definition
        if interfaces.IDefinition.providedBy(data):
            data.spec = self.spec

        return data


def find_files_relative_to(root, *patterns, recurse=False):
    root = pathlib.Path(root)

    # just recurse or not
    glob = root.rglob if recurse else root.glob

    return itertools.chain.from_iterable(glob(pattern) for pattern in patterns)


@interface.implementer(interfaces.INamespace)
class Namespace(dict):

    """A Namespace provides means to iterate over yaml files in subdirectories."""

    def __bool__(self):
        return True

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        assert isinstance(other, Namespace)
        return repr(self) == repr(other)

    def __getitem__(self, key):
        # for convenience cast str to Path
        if isinstance(key, str):
            key = pathlib.Path(key)

        return super(Namespace, self).__getitem__(key)


@component.adapter(pathlib.Path)
class DirectoryNamespace(Namespace):

    """A Namespace pointing to the contents of a directory."""

    def __init__(self, root, *pattern):
        self.root = pathlib.Path(root)

        super(DirectoryNamespace, self).__init__()

    def config_specs(self, *pattern):

        # XXX TODO simplify namespace/spec/specloader config
        if not pattern:
            pattern = [
                '**/*.yaml',
                '**/*.yml'
            ]
        for f in find_files_relative_to(self.root, *pattern):
            log.debug('Found spec: %s', f)

            source_name = f.relative_to(self.root)
            with self.open(source_name) as spec_file:
                spec = Spec(self, source_name)
                spec.update(load_spec_file(spec, spec_file))

            self[source_name] = spec

    def open(self, source_name):
        return (self.root / source_name).open()

    def __repr__(self):
        return '{}'.format(self.root)


@component.adapter(pkg_resources.EntryPoint)
class EntryPointNamespace(DirectoryNamespace):

    """A Namespace pointing to the contents of a python package."""

    _entry_point_group = 'debellator.specs'

    def __init__(self, entry_point):
        self.entry_point = entry_point
        root = pkg_resources.resource_filename(self.entry_point.module_name, '')
        super(EntryPointNamespace, self).__init__(root)

    def path(self, resource_name):
        return pathlib.Path(pkg_resources.resource_filename(self.entry_point.module_name, resource_name))

    def __repr__(self):
        return '{0.dist.key}#{0.name}'.format(self.entry_point)


@component.adapter(str)
@interface.implementer(interfaces.INamespace)
def namespace_adapter(scalar):
    # TODO
    dist, _, name = value.rpartition('#')
    if not dist:
        # take first ep found
        ep = list(pkg_resources.iter_entry_points(cls._entry_point_group, name))[0]

    else:
        ep = pkg_resources.get_entry_map(dist, group=cls._entry_point_group)[name]

    return cls(ep)


def load_spec_file(spec, spec_file):
    """Load yaml into spec."""
    loader = SpecLoader(spec_file, spec=spec)
    definitions = loader.get_single_data()

    assert isinstance(definitions, dict), 'The top level data type of a spec file must be a mapping!'

    try:
        return definitions

    finally:
        loader.dispose()


class Spec(collections.OrderedDict):

    """A `Spec` is the top level dictionary of a yaml definition file.

    It contains the top level keys, which may be referenced by other specs.
    """

    def __init__(self, namespace, source):
        self.namespace = namespace
        self.source = source

        super(Spec, self).__init__()

    def __hash__(self):
        return hash((self.namespace, self.source))

    def __eq__(self, other):
        assert isinstance(other, Spec)
        return self.namespace == other.namespace and self.source == other.source

    def __repr__(self):
        return '<Spec {0}>'.format(self.namespace.root.joinpath(self.source))


class _SpecDescriptior:

    """Initialize the spec attribute via this descriptor.

    This is just to keep __init__ method untouched.
    """

    def __init__(self):
        self.spec = None
        self.instances = weakref.WeakKeyDictionary()

    def __get__(self, inst, cls):
        if inst:
            return self.instances[inst]

        return self.spec

    def __set__(self, inst, value):
        if inst:
            self.instances[inst] = value

        self.spec = value


@interface.implementer(interfaces.IDefinition)
class Definition:

    """A Definition holds a reference to its spec."""

    spec = _SpecDescriptior()


@interface.implementer(interfaces.IEvolvable)
@component.adapter(interfaces.IYamlScalarNode, interfaces.IYamlLoader)
class Null(Definition):

    """Just a hack overcome zope adapter logic."""

    def __init__(self, node, loader):
        pass

    async def evolve(self, scope):
        return None


def register_adapters():
    component.provideAdapter(EntryPointNamespace)
    component.provideAdapter(DirectoryNamespace)

    interface.classImplements(yaml.nodes.ScalarNode, interfaces.IYamlScalarNode)
    interface.classImplements(yaml.nodes.MappingNode, interfaces.IYamlMappingNode)
    interface.classImplements(yaml.nodes.SequenceNode, interfaces.IYamlSequenceNode)
    interface.classImplements(yaml.nodes.CollectionNode, interfaces.IYamlCollectionNode)

    # TODO
    @interface.implementer(interfaces.IEvolve)
    @component.adapter(interfaces.IEvolvable)
    def adapt_evolvable(evolvable):
        return evolvable.evolve(registry)
    component.provideAdapter(adapt_evolvable)
