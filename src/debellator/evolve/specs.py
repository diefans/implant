"""
Specifications a.k.a. specs are a set of maybe exported toplevel definitions of a control flow.

Specs can be organized in separate files and directories.
Private specs are only accessible within the defining project/namespace itself.

"""
import collections
import functools
import logging
import os
import pathlib
import types

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

    """Create orderewd dictionaries by default."""

    def __init__(self, stream, *, registry=None):
        self.registry = registry
        yaml.loader.Loader.__init__(self, stream)

    def construct_yaml_map(self, node):
        data = collections.OrderedDict(self.construct_pairs(node))
        yield data

    def construct_document(self, node):
        data = self.construct_object(node)
        while self.state_generators:
            state_generators = self.state_generators
            self.state_generators = []
            for generator in state_generators:
                for dummy in generator:
                    pass
        self.constructed_objects = {}
        self.recursive_objects = {}
        self.deep_construct = False
        return data

    def construct_object(self, node, deep=False):
        if node in self.constructed_objects:
            return self.constructed_objects[node]
        if deep:
            old_deep = self.deep_construct
            self.deep_construct = True
        if node in self.recursive_objects:
            raise yaml.constructor.ConstructorError(None, None,
                                                    "found unconstructable recursive node", node.start_mark)
        self.recursive_objects[node] = None
        constructor = None
        tag_suffix = None

        # lookup constructor in registry
        try:
            if self.registry:
                try:
                    data = self.registry.getMultiAdapter(
                        (node, self),
                        interfaces.IYamlConstructor,
                        name=node.tag
                    )
                    log.debug('Named yaml constructor lookup: %s', node)
                except component.ComponentLookupError:
                    # fallback
                    data = self.registry.getMultiAdapter(
                        (node, self),
                        interfaces.IYamlConstructor
                    )
                    log.debug('General yaml constructor lookup: %s', node)
                # constructor = self.registry.queryAdapter(node, IYamlConstructor, name=node.tag)

            else:
                raise ValueError('No Registry found.')

        except (ValueError, component.ComponentLookupError):
            log.warning('Yaml constructor not found: %s', node)

            if node.tag in self.yaml_constructors:
                constructor = self.yaml_constructors[node.tag]

            else:
                for tag_prefix in self.yaml_multi_constructors:
                    if node.tag.startswith(tag_prefix):
                        tag_suffix = node.tag[len(tag_prefix):]
                        constructor = self.yaml_multi_constructors[tag_prefix]
                        break
                else:
                    if None in self.yaml_multi_constructors:
                        tag_suffix = node.tag
                        constructor = self.yaml_multi_constructors[None]
                    elif None in self.yaml_constructors:
                        constructor = self.yaml_constructors[None]
                    elif isinstance(node, yaml.nodes.ScalarNode):
                        constructor = self.__class__.construct_scalar
                    elif isinstance(node, yaml.nodes.SequenceNode):
                        constructor = self.__class__.construct_sequence
                    elif isinstance(node, yaml.nodes.MappingNode):
                        constructor = self.__class__.construct_mapping

            if tag_suffix is None:
                data = constructor(self, node)
            else:
                data = constructor(self, tag_suffix, node)

        if isinstance(data, types.GeneratorType):
            generator = data
            data = next(generator)
            if self.deep_construct:
                for dummy in generator:
                    pass
            else:
                self.state_generators.append(generator)
        self.constructed_objects[node] = data
        del self.recursive_objects[node]
        if deep:
            self.deep_construct = old_deep
        return data

    def construct_scalar(self, node):
        if not isinstance(node, yaml.nodes.ScalarNode):
            raise yaml.constructor.ConstructorError(None, None,
                                                    "expected a scalar node, but found %s" % node.id,
                                                    node.start_mark)
        return node.value

    def construct_sequence(self, node, deep=False):
        if not isinstance(node, yaml.nodes.SequenceNode):
            raise yaml.constructor.ConstructorError(None, None,
                                                    "expected a sequence node, but found %s" % node.id,
                                                    node.start_mark)
        return [self.construct_object(child, deep=deep)
                for child in node.value]

    def generate_sequence(self, node, deep=False):
        if not isinstance(node, yaml.nodes.SequenceNode):
            raise yaml.constructor.ConstructorError(None, None,
                                                    "expected a sequence node, but found %s" % node.id,
                                                    node.start_mark)
        return (self.construct_object(child, deep=deep)
                for child in node.value)

    def construct_mapping(self, node, deep=False):
        if not isinstance(node, yaml.nodes.MappingNode):
            raise yaml.constructor.ConstructorError(None, None,
                                                    "expected a mapping node, but found %s" % node.id,
                                                    node.start_mark)
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, collections.Hashable):
                raise yaml.constructor.ConstructorError("while constructing a mapping", node.start_mark,
                                                        "found unhashable key", key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping

    def construct_pairs(self, node, deep=False):
        if not isinstance(node, yaml.nodes.MappingNode):
            raise yaml.constructor.ConstructorError(None, None,
                                                    "expected a mapping node, but found %s" % node.id,
                                                    node.start_mark)
        pairs = []
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            value = self.construct_object(value_node, deep=deep)
            pairs.append((key, value))
        return pairs

    def construct_no_duplicate_yaml_map(self, node):
        data = Mapping(self.generate_no_duplicate_pairs(node))
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


class SpecLoader(ComponentLoader):

    def __init__(self, *args, spec=None, **kwargs):
        super(SpecLoader, self).__init__(*args, **kwargs)
        self.spec = spec

    def construct_object(self, node, deep=False):
        data = super(SpecLoader, self).construct_object(node, deep=deep)

        # add spec to the definition
        if interfaces.IDefinition.providedBy(data):
            data.spec = self.spec

        return data


@interface.implementer(interfaces.INamespace)
class Namespace(dict):

    """A Namespace provides means to iterate over yaml files in subdirectories."""

    def __init__(self):
        super(Namespace, self).__init__(self._load())

    def listdir(self, resource_name):
        return list(self.path(resource_name).iterdir())

    def isdir(self, resource_name):
        return self.path(resource_name).is_dir()

    def open(self, resource_name):
        return self.path(resource_name).open()

    def path(self, resource_name):
        raise NotImplementedError()

    def __bool__(self):
        return True

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        assert isinstance(other, Namespace)
        return repr(self) == repr(other)

    def iter_sources(self, resource_name='', spec_ext=('.yml', '.yaml')):
        resources = [resource_name]

        while resources:
            resource_name = resources.pop(0)

            if self.isdir(resource_name):
                resources.extend([
                    os.path.join(resource_name, str(child))
                    for child in self.listdir(resource_name)
                ])

            else:
                _, ext = os.path.splitext(resource_name)
                if ext in ('.yml', '.yaml'):
                    yield resource_name

    def _load(self):
        for resource_name in self.iter_sources():
            yield resource_name, Spec(self, resource_name)


@component.adapter(pathlib.Path)
class DirectoryNamespace(Namespace):

    """A Namespace pointing to the contents of a directory."""

    def __init__(self, root):
        self.root = root
        super(DirectoryNamespace, self).__init__()

    def path(self, resource_name):
        return self.root.joinpath(resource_name)

    def __repr__(self):
        return '{}'.format(self.root)


@component.adapter(pkg_resources.EntryPoint)
class EntryPointNamespace(Namespace):

    """A Namespace pointing to the contents of a python package."""

    _entry_point_group = 'debellator.specs'

    def __init__(self, entry_point):
        self.entry_point = entry_point
        super(EntryPointNamespace, self).__init__()

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


class Spec(collections.OrderedDict):

    """A `Spec` is the top level dictionary of a yaml definition file.

    It contains the top level keys, which may be referenced by other specs.
    """

    def __init__(self, namespace, source):
        self.namespace = namespace
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
            registry = component.getSiteManager()
            loader = SpecLoader(spec_file, registry=registry, spec=self)
            definitions = loader.get_single_data()

            try:
                return definitions
            finally:
                loader.dispose()


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


class SpecDescriptior:

    """Initialize the spec attribute via this descriptor."""

    def __init__(self):
        self.spec = None

    def __get__(self, inst, type):
        # always return our registry
        if inst:
            return self.spec

    def __set__(self, inst, value):
        self.spec = value


class Definition:

    """A Definition holds a reference to its spec."""

    spec = SpecDescriptior()


@interface.implementer(interfaces.IEvolvable)
@component.adapter(interfaces.IYamlSequenceNode, interfaces.IYamlLoader)
class Sequence(list, Definition):
    def __init__(self, node, loader):
        super(Sequence, self).__init__(loader.generate_sequence(node))

    async def evolve(self, scope):
        return self


@interface.implementer(interfaces.IEvolvable)
@component.adapter(interfaces.IYamlMappingNode, interfaces.IYamlLoader)
class Mapping(collections.OrderedDict, Definition):
    def __init__(self, node, loader):
        super(Mapping, self).__init__(loader.generate_no_duplicate_pairs(node))

    async def evolve(self, scope):
        return self


# TODO eventually use classes for str, int, float
@interface.implementer(interfaces.IYamlConstructor)
@component.adapter(interfaces.IYamlScalarNode, interfaces.IYamlLoader)
def create_str_scalar(node, loader):
    return loader.construct_scalar(node)


@interface.implementer(interfaces.IYamlConstructor)
@component.adapter(interfaces.IYamlScalarNode, interfaces.IYamlLoader)
def create_int_scalar(node, loader):
    return int(loader.construct_scalar(node))


@interface.implementer(interfaces.IYamlConstructor)
@component.adapter(interfaces.IYamlScalarNode, interfaces.IYamlLoader)
def create_float_scalar(node, loader):
    return float(loader.construct_scalar(node))


@interface.implementer(interfaces.IEvolvable)
@component.adapter(interfaces.IYamlScalarNode, interfaces.IYamlLoader)
class Null(Definition):

    """Just a hack overcome zope adapter logic."""

    def __init__(self, node, loader):
        pass

    async def evolve(self, scope):
        return None


def register_adapters(registry):
    registry.registerAdapter(EntryPointNamespace)
    registry.registerAdapter(DirectoryNamespace)

    interface.classImplements(yaml.nodes.ScalarNode, interfaces.IYamlScalarNode)
    interface.classImplements(yaml.nodes.MappingNode, interfaces.IYamlMappingNode)
    interface.classImplements(yaml.nodes.SequenceNode, interfaces.IYamlSequenceNode)
    interface.classImplements(yaml.nodes.CollectionNode, interfaces.IYamlCollectionNode)

    registry.registerAdapter(Mapping, name='tag:yaml.org,2002:map')
    registry.registerAdapter(Sequence, name='tag:yaml.org,2002:seq')
    registry.registerAdapter(create_str_scalar, name='tag:yaml.org,2002:str')
    registry.registerAdapter(create_int_scalar, name='tag:yaml.org,2002:int')
    registry.registerAdapter(create_float_scalar, name='tag:yaml.org,2002:float')
    registry.registerAdapter(Null, name='tag:yaml.org,2002:null')

    # general fallback constructors
    # TODO do we need them?
    # registry.registerAdapter(Mapping)
    # registry.registerAdapter(Sequence)
    # registry.registerAdapter(create_str_scalar)
