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
import yaml
from zope import interface

from . import specs, interfaces


class YamlMixin(yaml.YAMLObject):

    """Common Yaml properties."""

    # yaml_loader = Loader
    yaml_flow_style = False

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "<Def: {} {}>".format(self.yaml_tag, self.__dict__)

    @classmethod
    def from_yaml(cls, loader, node):
        """Construct an object by mapping."""
        mapping = loader.construct_mapping(node)
        return cls(**mapping)


class MappingConstructor(YamlMixin):

    """Create an ordered dict from a mapping and complain about duplicate keys."""

    yaml_tag = 'tag:yaml.org,2002:map'

    @classmethod
    def from_yaml(cls, loader, node):
        return loader.construct_no_duplicate_yaml_map(node)


class NamespaceDefinition(specs.Definition, YamlMixin):

    """A definition for a Namespace."""

    yaml_tag = '!ns'

    def __init__(self, namespace):
        super(NamespaceDefinition, self).__init__()
        self.namespace = namespace

    @classmethod
    def from_yaml(cls, loader, node):
        value = loader.construct_scalar(node)
        return cls(specs.Namespace.from_scalar(value))


@interface.implementer(interfaces.IEvolvable)
class RemoteDefinition(specs.Definition, YamlMixin):

    """A definition of a remote host.

    .. code-block:: yaml

        ---
        host: !remote
            hostname: foobar.example.com
            user: dbltr
            sudo: true
            fallback: !remote
                hostname: foobar.example.com
                user: root

    """

    yaml_tag = '!remote'

    def __init__(self, *, hostname=None, user=None, sudo=None, fallback=None):
        super(RemoteDefinition, self).__init__()
        self.hostname = hostname
        self.user = user
        self.sudo = sudo or False
        self.fallback = fallback

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
        if isinstance(node, yaml.nodes.ScalarNode):
            return cls()

        mapping = loader.construct_mapping(node)
        return cls(**mapping)


class GroupDefinition(specs.Definition, YamlMixin):

    """A definition of a group of hosts or groups."""

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


class ReferenceDefinition(specs.Definition, YamlMixin):

    """A reference definition to another definition."""

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


class ForDefinition(specs.Definition, YamlMixin):
    yaml_tag = '!for'

    def __init__(self, *, iterate, name, do):
        super(ForDefinition, self).__init__()
        self.iterate = iterate
        self.name = name
        self.do = do


class SpawnDefinition(specs.Definition, YamlMixin):
    yaml_tag = '!spawn'

    def __init_(self):
        super(SpawnDefinition, self).__init__()


class CopyDefinition(specs.Definition, YamlMixin):
    yaml_tag = '!copy'

    def __init__(self, *, src, dest):
        self.src = src
        self.dest = dest
