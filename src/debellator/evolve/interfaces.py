from zope import interface


class IYamlNode(interface.Interface):
    pass


class IYamlCollectionNode(IYamlNode):
    pass


class IYamlMappingNode(IYamlCollectionNode):
    pass


class IYamlSequenceNode(IYamlCollectionNode):
    pass


class IYamlScalarNode(IYamlNode):
    pass


class IYamlLoader(interface.Interface):

    """A marker for yaml laoder."""


class IYamlConstructor(interface.Interface):

    """A marker interface for a node adapter."""


class IDefinition(IYamlConstructor):

    """A Definition holds a reference to its spec."""

    spec = interface.Attribute('The spec this deinition belongs to.')


class IEvolvable(IDefinition):

    """Define the possibility to evolve."""

    async def evolve(self, scope):
        """Evolve remote by using scope."""


class IRemote(interface.Interface):

    """A remote."""


class IEvolve(interface.Interface):

    """A marker for utility registration."""


class INamespace(interface.Interface):

    """A unique namespace for spec lookup."""

    def listdir(resource_name):
        pass

    def isdir(resource_name):
        pass

    def open(resource_name):
        pass

    def __hash__():
        pass

    def __eq__(other):
        pass

    def iter_sources(resource_name=''):
        pass
