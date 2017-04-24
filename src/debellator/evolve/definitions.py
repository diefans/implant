import collections
import logging
import os
import pathlib
import re

import yaml
from zope import component, interface

from . import config, interfaces, specs

log = logging.getLogger(__name__)


@interface.implementer(interfaces.IEvolvable)
@component.adapter(dict)
class EvolvableMapping:

    """An evolvable adapter for mapping like data."""

    def __init__(self, mapping):
        self.mapping = mapping

    async def evolve(self, scope):
        lst = [(key, await interfaces.IEvolvable(value).evolve(scope)) for key, value in self.mapping.items()]
        odict = collections.OrderedDict(lst)
        return odict


@interface.implementer(interfaces.IEvolvable)
@component.adapter(list)
class EvolvableSequence:

    """An evolvable adapter for mapping like data."""

    def __init__(self, sequence):
        self.sequence = sequence

    async def evolve(self, scope):
        sequence = [await interfaces.IEvolvable(item).evolve(scope) for item in self.sequence]
        return sequence


@interface.implementer(interfaces.IEvolvable)
@component.adapter(None)
class EvolvableDefault:

    """Default adapter for evolables."""

    def __init__(self, data):
        self.data = data

    async def evolve(self, scope):
        return self.data


@component.adapter(dict)
class DebugMapping(collections.OrderedDict, specs.Definition):
    async def evolve(self, scope):
        # TODO
        return None


@interface.implementer(interfaces.IEvolvable)
class For(specs.Definition):
    @classmethod
    @component.adapter(dict)
    def adapt_dict(cls, dct):
        return For(**{f'for_{key}': value for key, value in dct.items()})

    def __init__(self, *, for_in, for_do, for_item='item'):
        self.item_name = for_item
        self.iterable = for_in
        self.evolving = for_do

    async def evolve(self, scope):
        lst = []
        for item in await interfaces.IEvolvable(self.iterable).evolve(scope):
            scope[self.item_name] = item
            log.debug('Scope: %s', dict(scope))
            lst.append(await interfaces.IEvolvable(self.evolving).evolve(scope))

        return lst


re_reference_string = re.compile(r'^(?:(?:(?P<ns>.*):)?(?P<spec>[^:]+):)?(?P<def>[^:]+)$')


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

        match = re_reference_string.match(self.ref_string)
        if match is None:
            raise RuntimeError(
                'Reference syntax of `{}` wrong! See {} for details'.format(
                    self.ref_string, re_reference_string.pattern)
            )

        self.ns_name, self.spec_name, self.def_name = match.groups()

    @property
    def target(self):
        spec = self.spec

        if (self.spec_name is None or self.ns_name is None) and not spec:
            raise RuntimeError('Reference lookup without any spec context!')

        # an empty string is the root namespace
        ns = interfaces.INamespace(self.ns_name) if self.ns_name is not None else spec.namespace

        if self.spec_name is not None:
            spec = ns[self.spec_name]

        ref = spec[self.def_name]

        return ref

    def resolve(self, nested=True):
        target = self

        while nested and isinstance(target, Reference):
            target = target.target

        return target


@interface.implementer(interfaces.IReference)
@component.adapter(interfaces.IYamlScalarNode, interfaces.IYamlLoader)
def adapt_ref_scalar(node, loader):
    scalar = loader.construct_scalar(node)
    return interfaces.IReference(scalar)


@component.adapter(str)
def adapt_environ(name):
    envvar = os.environ.get(name, None)
    return envvar


@component.adapter(interfaces.IYamlScalarNode, interfaces.IYamlLoader)
def adapt_environ_scalar(node, loader):
    name = loader.construct_scalar(node)
    envvar = os.environ.get(name, None)

    return envvar


@interface.implementer(interfaces.IEvolvable)
@component.adapter(str)
class Eval(specs.Definition):
    def __init__(self, expression):
        self.expression = expression

    async def evolve(self, scope):
        result = scope.eval(self.expression)
        return result


def register_adapters():
    component.provideAdapter(EvolvableDefault)
    component.provideAdapter(EvolvableSequence)
    component.provideAdapter(EvolvableMapping)
    component.provideAdapter(Eval, provides=interfaces.IDefinition, name='!eval')
    component.provideAdapter(DebugMapping, provides=interfaces.IDefinition, name='!debug')
    component.provideAdapter(For.adapt_dict, provides=interfaces.IDefinition, name='!for')
    component.provideAdapter(adapt_ref_scalar, provides=interfaces.IYamlConstructor, name='!ref')
    component.provideAdapter(adapt_environ_scalar, provides=interfaces.IYamlConstructor, name='!env')
    component.provideAdapter(Reference, provides=interfaces.IReference)
