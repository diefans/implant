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



upstream1: foo
upstream2: bar

node: !node
    join:
        - !condition
            when: foo
            do: !export
                name: upstream_result
                value: !ref upstream1
        - !copy
            src: !scope upstream_result
            dest: foo
    spawn:
        - !service
            name: nginx
            state: started
        - !event startup

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

nginx: !composition
    - !ref


setup: !queue
    - !ref nginx_package
    - !ref nginx_default_config
    - !ref nginx_service


---

# Component to remote attribution
# group to node mapping



"""
import os
import pkg_resources
import yaml
from collections import OrderedDict
import functools


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


class NamespaceMeta(type):
    def from_scalar(cls, value):
        if value[0] in (os.path.sep, '.'):
            ns = Namespace(value)
        else:
            ns = EntryPointNamespace.from_scalar(value)

        return ns


class Namespace(metaclass=NamespaceMeta):
    def __init__(self, path=None):
        self.path = path or './'

    def listdir(self, resource_name):
        return os.path.listdir(self.path(resource_name))

    def isdir(self, resource_name):
        return os.path.isdir(self.path(resource_name))

    def path(self, resource_name):
        return os.path.join(self.path, resource_name)

    def open(self, resource_name):
        return open(self.path(resource_name))

    def __repr__(self):
        return self.path

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


class EntryPointNamespace(Namespace):
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
    def __init__(self, source, *, namespace=None):
        self.namespace = namespace or Namespace()
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
            class SpecLoader(Loader):
                spec = self

                def construct_object(self, node, deep=False):
                    data = super(SpecLoader, self).construct_object(node, deep=deep)
                    if isinstance(data, Definition):
                        data.spec = self.spec

                    return data

            definitions = yaml.load(spec_file, Loader=SpecLoader)

            # set spec of each definition
            for definition in definitions.values():
                definition.spec = self

        return definitions


class Specs(dict):
    # TODO Specs should be a dict of namespaces and sources pointing to a spec
    def __init__(self, *roots):
        super(Specs, self).__init__()

        for root in roots:
            if isinstance(root, pkg_resources.EntryPoint):
                namespace = EntryPointNamespace(root)

            else:
                namespace = Namespace(root)

            if namespace not in self:
                namespace_specs = self[namespace] = {}

            namespace_specs.update(
                ((resource_name, Spec(resource_name, namespace=namespace)) for resource_name in namespace.iter_sources())
            )


class YamlMixin(yaml.YAMLObject):
    # yaml_loader = Loader
    yaml_flow_style = False

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "<Def: {} {}>".format(self.yaml_tag, self.__dict__)


class MappingConstructor(YamlMixin):
    yaml_tag = 'tag:yaml.org,2002:map'

    @classmethod
    def from_yaml(cls, loader, node):
        return loader.construct_no_duplicate_yaml_map(node)


class Definition:
    def __init__(self):
        self.spec = None


class NamespaceDefinition(Definition, YamlMixin):
    yaml_tag = '!ns'

    def __init__(self, namespace):
        super(NamespaceDefinition, self).__init__()
        self.namespace = namespace

    @classmethod
    def from_yaml(cls, loader, node):
        value = loader.construct_scalar(node)
        return cls(Namespace.from_scalar(value))


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

            namespace = ns or loader.spec.namespace
            source = source or loader.spec.source

        else:
            # assert mapping
            mapping = loader.construct_mapping(node)
            name = mapping['name']
            namespace = mapping.get('namespace', loader.spec.namespace)
            source = mapping.get('source', loader.spec.source)

        return cls(name, namespace=namespace, source=source)
