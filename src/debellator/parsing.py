import os
import pkg_resources
import yaml
from collections import OrderedDict


root_definitions = OrderedDict()


class SyntaxError(yaml.error.MarkedYAMLError):
    pass


class DuplicateDefinition(SyntaxError):
    pass


class Loader(
        yaml.loader.Reader,
        yaml.loader.Scanner,
        yaml.loader.Parser,
        yaml.loader.Composer,
        yaml.loader.Constructor,
        yaml.loader.Resolver):

    def __init__(self, stream):
        yaml.loader.Reader.__init__(self, stream)
        yaml.loader.Scanner.__init__(self)
        yaml.loader.Parser.__init__(self)
        yaml.loader.Composer.__init__(self)
        yaml.loader.Constructor.__init__(self)
        yaml.loader.Resolver.__init__(self)

        print("Loader:", id(self))

        self.mapping_keys = {}

    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        for k, v in self.contruct_pairs(node):
            data[k] = v
        # value = self.construct_mapping(node)
        # data.update(value)

    def construct_pairs(self, node, deep=False):
        if not isinstance(node, yaml.nodes.MappingNode):
            raise yaml.constructor.ConstructorError(None, None,
                                                    "expected a mapping node, but found %s" % node.id,
                                                    node.start_mark)
        pairs = []
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            value = self.construct_object(value_node, deep=deep)

            # store k/v for error reporting
            if key not in self.mapping_keys:
                self.mapping_keys[key] = (key_node, value_node, value)

            else:
                context_key_node, context_value_node, context_value = self.mapping_keys[key]

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

            pairs.append((key, value))
        return pairs


class Spec:
    def __init__(self, namespace, filename):
        self.namespace = namespace
        self.filename = filename

    def __hash__(self):
        return hash((self.namespace, self.filename))

    def __eq__(self, other):
        assert isinstance(other, Spec)
        return self.namespace, self.filename == other.namespace, other.filename

    def load(self):
        with open(self.filename) as spec_file:
            definitions = yaml.load(spec_file, Loader=Loader)

            # set spec of each definition
            for definition in definition.values():
                definition.spec = self

        return definitions


class NonameSpec(Spec):
    def __init__(self, filename):
        super(NonameSpec, self).__init__(None, filename)


class YamlMixin(yaml.YAMLObject):
    yaml_loader = Loader
    yaml_flow_style = False

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "<Def: {} {}>".format(self.yaml_tag, self.__dict__)


class MappingConstructor(YamlMixin):
    yaml_tag = 'tag:yaml.org,2002:map'

    @classmethod
    def from_yaml(cls, loader, node):
        # collect all root mappings
        # and verifiy its uniqueness
        return loader.construct_pairs(node)


class Definition:
    def __init__(self):
        self.spec = None


class RemoteDefinition(Definition, YamlMixin):
    yaml_tag = '!remote'

    def __init__(self, hostname=None, user=None, sudo=None):
        super(RemoteDefinition, self).__init__()
        self.hostname = hostname
        self.user = user
        self.sudo = sudo or False

    @classmethod
    def to_yaml(cls, dumper, data):
        mapping = [
            ('hostname', data.hostname),
            ('user', data.user),
            ('sudo', data.sudo),
        ]

        node = dumper.represent_mapping(cls.yaml_tag, mapping)
        return node

    @classmethod
    def from_yaml(cls, loader, node):
        mapping = loader.construct_mapping(node)
        return cls(*mapping)


class GroupDefinition(Definition, YamlMixin):
    yaml_tag = '!group'

    def __init__(self, members):
        super(GroupDefinition, self).__init__()
        self.members = members

    @classmethod
    def to_yaml(cls, dumper, data):
        node = dumper.represent_sequence(cls.yaml_tag, data.members)
        return node

    @classmethod
    def from_yaml(cls, loader, node):
        members = loader.construct_sequence(node)
        return cls(members)


class ReferenceDefinition(Definition, YamlMixin):
    yaml_tag = '!ref'

    def __init__(self, name):
        super(ReferenceDefinition, self).__init__()
        self.name = name

    @classmethod
    def to_yaml(cls, dumper, data):
        node = dumper.represent_scalar(cls.yaml_tag, data.name)
        return node

    @classmethod
    def from_yaml(cls, loader, node):
        name = loader.construct_scalar(node)
        return cls(name)


class Specs(set):
    def __init__(self, *roots):
        super(Specs, self).__init__()

        for root in roots:
            if isinstance(root, pkg_resources.EntryPoint):
                self._collect_from_entry_point(root)

            else:
                self._collect_from_directory(root)

    def _collect_from_entry_point(self, entry_point, resource_name=''):
        module_name = entry_point.module_name

        assert pkg_resources.resource_isdir(module_name, resource_name),\
            'Entrypoint {} is no directory'.format(entry_point)

        resources = [resource_name]

        while resources:
            resource_name = resources.pop(0)

            if pkg_resources.resource_isdir(module_name, resource_name):
                resources.extend([
                    os.path.join(resource_name, child)
                    for child in pkg_resources.resource_listdir(module_name, resource_name)
                ])

            else:
                _, ext = os.path.splitext(resource_name)
                if ext in ('.yml', '.yaml'):
                    self.add(Spec(entry_point.name, pkg_resources.resource_filename(module_name, resource_name)))

    def _collect_from_directory(self, path):
        pass


def collect_definitions(specs):
    """traverse specs and create definitions."""

