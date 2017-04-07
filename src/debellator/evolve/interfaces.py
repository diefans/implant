from zope.interface import Interface, Attribute


class IYamlNode(Interface):
    pass


class IYamlCollectionNode(IYamlNode):
    pass


class IYamlMappingNode(IYamlCollectionNode):
    pass


class IYamlSequenceNode(IYamlCollectionNode):
    pass


class IYamlScalarNode(IYamlNode):
    pass


class IYamlLoader(Interface):

    """A marker for yaml laoder."""


class IYamlConstructor(Interface):

    """A marker interface for a node adapter."""


class IDefinition(IYamlConstructor):

    """A Definition holds a reference to its spec."""

    spec = Attribute('The spec this deinition belongs to.')


class IEvolvable(IDefinition):

    """Define the possibility to evolve."""

    async def evolve(registry, scope):
        """Evolve remote by using scope."""


class IRemote(Interface):

    """A remote."""


class ISettings(Interface):

    """Hold all variable application settings."""


class IDependencies(Interface):

    """Just a marker to hold dependency tree."""


class IConfiguration(Interface):

    dependencies = Attribute("Actual working set dependencies")
    registry = Attribute("The zope component registry.""")


class IConfigurationEvent(Interface):

    configuration = Attribute("The application configuration.")


class IEvolve(IEvolvable):

    """A marker for utility registration."""


class INamespace(Interface):

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


class IIncommingQueue(Interface):
    pass


class IRequest(Interface):

    """A unique request to evolve."""

    time = Attribute('The time the request entered.')
    source = Attribute('The source of the request.')
    definition = Attribute('The definition, which should evolve.')
    scope = Attribute('The extra scope for this request')


class IReference(Interface):
    pass
