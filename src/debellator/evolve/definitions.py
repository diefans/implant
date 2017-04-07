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
import re
import pathlib
import logging
import yaml
from zope import interface, component

from . import specs, interfaces, config


log = logging.getLogger(__name__)


@interface.implementer(interfaces.IEvolvable)
@component.adapter(interfaces.IYamlMappingNode, interfaces.IYamlLoader)
class Debug_mapping(specs.Definition):
    def __init__(self, node, loader):
        pass

    async def evolve(self, scope):
        return None


@interface.implementer(interfaces.IEvolvable)
@component.adapter(interfaces.IYamlMappingNode, interfaces.IYamlLoader)
class For(specs.Definition):
    def __init__(self, node, loader):
        pass

    async def evolve(self, scope):
        return None


# XXX TODO FIXME do we need IEvolable to be declared or can we just test, if a node provides this IF
# and when not cast it
@interface.implementer(interfaces.IEvolvable)
@interface.implementer(interfaces.IReference)
@component.adapter(str)
class Reference(specs.Definition):
    def __init__(self, ref_string):
        """A `Reference` may constructed via string.

        dist#ep:spec:key    - full reference
        ep:spec:key         - dist local reference
        spec:key            - entrypoint local reference
        key                 - spec local reference
        """
        self.ref_string = ref_string

    @property
    def target(self):
        return _lookup_reference(self.ref_string, self.spec)


@interface.implementer(interfaces.IReference)
@component.adapter(interfaces.IYamlScalarNode, interfaces.IYamlLoader)
def adapt_ref_scalar(node, loader):
    scalar = loader.construct_scalar(node)
    return interfaces.IReference(scalar)


class NamespaceNotFound(Exception):
    pass


def _lookup_namespace(ns_name):
    try:
        namespace = component.getUtility(interfaces.INamespace, ns_name)

    except component.ComponentLookupError:
        # namespace is not defined
        # try to guess
        if ns_name and '#' not in ns_name:
            # without '#' we assume entrypoint name
            for ep in pkg_resources.iter_entry_points(config.ENTRY_POINT_GROUP_SPECS, ns_name):
                ns_name = '#'.join((ep.dist.key, ep.name))
                namespace = component.queryUtility(interfaces.INamespace, ns_name)

                if namespace:
                    break

        else:
            # error
            log.error('Namespace for `%s` not registered or misspelled', ns_name)
            raise NamespaceNotFound('Namespace for `{}` not registered or misspelled'.format(ns_name))

    return namespace


re_reference_string = re.compile(r'^(?:(?:(?P<ns>.*):)?(?P<spec>[^:]+):)?(?P<def>[^:]+)$')


def _lookup_reference(ref_string, spec=None):
    match = re_reference_string.match(ref_string)

    if match is None:
        raise RuntimeError(
            'Reference syntax of `{}` wrong! See {} for details'.format(ref_string, re_reference_string.pattern)
        )

    ns_name, spec_name, def_name = match.groups()

    if (spec_name is None or ns_name is None) and not spec:
        raise RuntimeError('Reference lookup without any spec context!')

    # an empty string is the root namespace
    ns = _lookup_namespace(ns_name) if ns_name is not None else spec.namespace

    if spec_name is not None:
        spec = ns[spec_name]

    ref = spec[def_name]

    return ref


def register_adapters():
    component.provideAdapter(Debug_mapping, name='!debug')
    component.provideAdapter(For, name='!for')
    component.provideAdapter(adapt_ref_scalar, provides=interfaces.IYamlConstructor, name='!ref')
    component.provideAdapter(Reference, provides=interfaces.IReference)


# @interface.implement(interfaces.IEvolvable)
# class NamespaceDefinition:

#     """A definition for a Namespace."""

#     def __init__(self, loader, node):

#         self.namespace = interfaces.INamespace(nodynamespace

#     @classmethod
#     def from_yaml(cls, loader, node):
#         value = loader.construct_scalar(node)
#         return cls(specs.Namespace.from_scalar(value))


# @interface.implementer(interfaces.IEvolvable)
# class RemoteDefinition(specs.Definition, YamlMixin):

#     """A definition of a remote host.

#     .. code-block:: yaml

#         ---
#         host: !remote
#             hostname: foobar.example.com
#             user: dbltr
#             sudo: true
#             fallback: !remote
#                 hostname: foobar.example.com
#                 user: root

#     """

#     yaml_tag = '!remote'

#     def __init__(self, *, hostname=None, user=None, sudo=None, fallback=None):
#         super(RemoteDefinition, self).__init__()
#         self.hostname = hostname
#         self.user = user
#         self.sudo = sudo or False
#         self.fallback = fallback

#     @classmethod
#     def to_yaml(cls, dumper, data):
#         mapping = [
#             ('hostname', data.hostname),
#             ('user', data.user),
#             ('sudo', data.sudo),
#         ]

#         node = dumper.represent_mapping(cls.yaml_tag, mapping)
#         return node

#     @classmethod
#     def from_yaml(cls, loader, node):
#         if isinstance(node, yaml.nodes.ScalarNode):
#             return cls()

#         mapping = loader.construct_mapping(node)
#         return cls(**mapping)


# class GroupDefinition(specs.Definition, YamlMixin):

#     """A definition of a group of hosts or groups."""

#     yaml_tag = '!group'

#     def __init__(self, members):
#         super(GroupDefinition, self).__init__()
#         self.members = members

#     @classmethod
#     def to_yaml(cls, dumper, data):
#         node = dumper.represent_sequence(cls.yaml_tag, data.members)
#         return node

#     @classmethod
#     def from_yaml(cls, loader, node):
#         members = loader.construct_sequence(node)
#         return cls(members)


# class ReferenceDefinition(specs.Definition, YamlMixin):

#     """A reference definition to another definition."""

#     yaml_tag = '!ref'

#     _partition = ':'

#     def __init__(self, name, *, namespace=None, source=None):
#         if source is None:
#             assert namespace is None, "A reference to a namespace without source may be ambiguous!"

#         super(ReferenceDefinition, self).__init__()
#         self.name = name
#         self.namespace = namespace
#         self.source = source

#     @classmethod
#     def to_yaml(cls, dumper, data):
#         node = dumper.represent_scalar(cls.yaml_tag, data.name)
#         return node

#     @classmethod
#     def from_yaml(cls, loader, node):
#         if isinstance(node, yaml.nodes.ScalarNode):
#             fq_name = loader.construct_scalar(node)

#             ns_source, _, name = fq_name.rpartition(cls._partition)
#             ns, _, source = ns_source.rpartition(cls._partition)

#             namespace = ns or loader.spec.namespace
#             source = source or loader.spec.source

#         else:
#             # assert mapping
#             mapping = loader.construct_mapping(node)
#             name = mapping['name']
#             namespace = mapping.get('namespace', loader.spec.namespace)
#             source = mapping.get('source', loader.spec.source)

#         return cls(name, namespace=namespace, source=source)


# class ForDefinition(specs.Definition, YamlMixin):
#     yaml_tag = '!for'

#     def __init__(self, *, iterate, name, do):
#         super(ForDefinition, self).__init__()
#         self.iterate = iterate
#         self.name = name
#         self.do = do


# class SpawnDefinition(specs.Definition, YamlMixin):
#     yaml_tag = '!spawn'

#     def __init_(self):
#         super(SpawnDefinition, self).__init__()


# class CopyDefinition(specs.Definition, YamlMixin):
#     yaml_tag = '!copy'

#     def __init__(self, *, src, dest):
#         self.src = src
#         self.dest = dest
