"""YAML parsing and general plugin layout.

Entry point group: debellator.specs

Each entry point must point to a package, where the package will setup specific yaml extensions and containing yaml
resource files will be collected for public definitions.  The name and location of a yaml spec file has no semantic and
serves only as a means to keep things structured for the spec provider.

Thus a definition name has to be unique for an entry point. To address this feature, a definition may be referenced
by a namespace prefix which is equal to the entry point name:

---
# hosts.yaml in entry point group `default`
local: !remote
    groups:
        - !ref group

    hostname: localhost
    # host vars
    scope: !scope
        defaults:
            var1: 123
            var2: 345


# groups.yaml
group: !group
  members:
    - !ref default:local
    # a reference without namespace points always to the namespace where it is contained
    - !ref local
  # group vars will secondary after host vars
  scope: !scope
    var1: foo
    var3: bar


# common task params
task: !task
    # a task has always an implicite scope
    # which will be exported
    scope: !scope
        defaults: !ref other_scope
        privates:
            foo: bar

        publics:
            baz: bam

    # target
    target: !remote
        hostname: localhost


- get_var('foobar', remote)
        |
        v
    remote.empty_root_scope
    group scope
    host scope
    facts



# export facts into the parent scope
facts: !load_facts
    # push scope




nginx_package: !package
    name: nginx-full
    state: present

nginx_service: !service
    name: nginx
    state: started

nginx_default_config: !template
    scope: !scope
        server_name: (!ref default:local).hostname
    depends:
    src: !asset <namespace>:templates/nginx.conf.j2
    engine: jinja2
    dest: /tec/nginx/nginx.conf


setup: !queue
    - !ref nginx_default_config

"""
import os
import pkg_resources
import yaml
from collections import OrderedDict


root_definitions = OrderedDict()


class SyntaxError(yaml.error.MarkedYAMLError):
    pass


class DuplicateDefinition(SyntaxError):
    pass


class Loader(yaml.loader.Loader):

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


class Spec:
    def __init__(self, source):
        self.namespace = None
        self.source = self.filename = source

    def __hash__(self):
        return hash((self.namespace, self.source))

    def __eq__(self, other):
        assert isinstance(other, Spec)
        return self.namespace, self.source == other.namespace, other.source

    def __repr__(self):
        return '<Spec {0.namespace}:{0.source}>'.format(self)

    def load(self):
        with open(self.filename) as spec_file:
            class SpecLoader(Loader):
                spec = self

            definitions = yaml.load(spec_file, Loader=SpecLoader)

            # set spec of each definition
            for definition in definitions.values():
                definition.spec = self

        return definitions


class EntryPointSpec(Spec):

    _entry_point_group = 'debellator.specs'

    def __init__(self, entry_point, resource_name):
        super(EntryPointSpec, self).__init__(resource_name)

        if isinstance(entry_point, str):
            entry_point = list(pkg_resources.iter_entry_points(self._entry_point_group, name=entry_point))[0]

        self.entry_point = entry_point
        self.namespace = entry_point.name
        self.filename = pkg_resources.resource_filename(entry_point.module_name, resource_name)


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
        from pdb import set_trace; set_trace()       # XXX BREAKPOINT
        # and verifiy its uniqueness
        return loader.construct_no_duplicate_yaml_map(node)


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

    _partition = ':'

    def __init__(self, name, *, namespace=None, source=None):
        if source is None:
            assert namespace is None, "A reference to a namespace without source may be ambiguous!"

        super(ReferenceDefinition, self).__init__()
        self.name = name
        self.namespace = namespace
        self.source = source

    @classmethod
    def to_yaml(cls, dumper, data):
        node = dumper.represent_scalar(cls.yaml_tag, data.name)
        return node

    @classmethod
    def from_yaml(cls, loader, node):
        if isinstance(node, yaml.nodes.ScalarNode):
            fq_name = loader.construct_scalar(node)

            ns_source, _, name = fq_name.rpartition(cls._partition)
            ns, _, source = ns_source.rpartition(cls._partition)

            return cls(name, namespace=ns, source=source)

        else:
            # assert mapping
            mapping = loader.construct_mapping(node)
            return cls(mapping['name'], namespace=mapping['namespace'], source=mapping['source'])


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
                    self.add(EntryPointSpec(entry_point, resource_name))

    def _collect_from_directory(self, path):
        pass


def collect_definitions(specs):
    """traverse specs and create definitions."""

