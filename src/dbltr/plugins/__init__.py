import pkg_resources
from dbltr import utils


PLUGINS_ENTRY_POINT_GROUP = 'dbltr.plugins'


class MetaPlugin(type):

    plugins = {}

    def __new__(mcs, name, bases, dct):
        print("new Plugin")

        cls = type.__new__(mcs, name, bases, dct)

        # scan for available plugins
        for entry_point in pkg_resources.iter_entry_points(group=PLUGINS_ENTRY_POINT_GROUP):
            plugin = cls(entry_point)
            mcs.plugins[plugin.name] = plugin

        return cls

    def __getitem__(cls, name):
        return cls.plugins[name]

    def __setitem__(cls, name, plugin):
        assert isinstance(name, cls)

        cls.plugins[name] = plugin

    def __delitem__(cls, name):
        del cls.plugins[name]


class Plugin(metaclass=MetaPlugin):

    """A plugin module introduced via entry point."""

    def __init__(self, entry_point):
        self.entry_point = entry_point

        self.name = '#'.join((self.entry_point.dist.project_name, self.entry_point.name))

    @utils.reify
    def module(self):
        """On demand module loading."""

        return self.entry_point.load()
