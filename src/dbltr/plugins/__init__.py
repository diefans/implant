import pkg_resources
from dbltr import utils


class MetaPlugin(type):

    plugins = {}

    def __new__(mcs, name, bases, dct):
        print("new Plugin")

        # scan for available plugins


        cls = type.__new__(mcs, name, bases, dct)

        return cls


class Plugin(metaclass=MetaPlugin):

    """A plugin module introduced via entry point."""

    def __init__(self, entry_point):
        self.entry_point = entry_point

        self.name = '#'.join((self.entry_point.dist.project_name, self.entry_point.name))

    @utils.reify
    def module(self):
        """On demand module loading."""

        return self.entry_point.load()
