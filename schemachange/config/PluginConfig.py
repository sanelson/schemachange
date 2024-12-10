import importlib
import pkgutil
from structlog import BoundLogger
import structlog

from schemachange.config.BaseConfig import BaseConfig


class PluginBaseConfig(BaseConfig):
    plugin_subcommand = None
    plugin_parent_arguments = {}
    plugin_subcommand_arguments = []

    @classmethod
    def get_parent_arguments(cls):
        return cls.plugin_parent_arguments

    @classmethod
    def get_subcommand_arguments(cls):
        return cls.plugin_subcommand_arguments


class PluginConfig:
    # TBD: What is the proper way to handle the logger without having to pass it around?
    def __init__(self):
        self.logger = structlog.get_logger()
        self.plugins = {}

    def import_plugin(self, name: str):
        try:
            plugin = importlib.import_module(name)
        except ImportError:
            self.logger.warning("Failed to import plugin {self.name}")
            return None
        self.logger.debug(f"Imported {name} plugin")
        return plugin

    def load_plugins(self):
        # Discover all Schemachange plugins
        self.logger.info("Discovering and importing plugins")
        discovered_plugins = []
        try:
            # for finder, name, ispkg in pkgutil.iter_modules(path=["schemachange"]):
            for finder, name, ispkg in pkgutil.iter_modules():
                if name.startswith("schemachange_"):
                    #                if name.startswith("schema"):
                    self.logger.debug("Found module", name=name)
                    discovered_plugins.append(name)
        except Exception as e:
            self.logger.error("Error discovering plugins", error=e)
            return None
        self.logger.debug("Discovered plugins", plugins=discovered_plugins)

        # Import all discovered plugins
        for plugin in discovered_plugins:
            # Initialize/import plugins
            self.logger.debug("Importing plugins")

            # If the plugin fails to import, skip it
            if not (plugin_module := self.import_plugin(name=plugin)):
                continue

            # Instatiate the plugin
            try:
                custom_plugin = plugin_module.SchemachangePlugin(name=plugin)
            except Exception as e:
                self.logger.error(f"Error instantiating plugin {plugin}", error=e)
                continue

            self.plugins[plugin] = custom_plugin

    def init_parsers(
        self, parent_parser, parser_subcommands, parser_deploy, parser_render
    ):
        for name, plugin in self.plugins.items():
            self.logger.debug(f"Initializing parsers for plugin {name}")
            plugin.init_parsers(
                parent_parser=parent_parser,
                parser_subcommands=parser_subcommands,
                parser_deploy=parser_deploy,
                parser_render=parser_render,
            )

    def get_subcommands(self):
        subcommands = []
        for name, plugin in self.plugins.items():
            self.logger.debug(f"Getting subcommands for plugin {name}")
            subcommands.extend(plugin.get_subcommands())

        # Remove duplicates
        subcommands = list(set(subcommands))
        self.logger.debug("Subcommands", subcommands=subcommands)
        return subcommands


class Plugin:
    name = None
    plugin_classes = {}  # Dict of subcommand: Class

    def __init__(self, name: str):
        self.name = name
        # self.logger = logger
        self.logger = structlog.get_logger()
        self.plugin_module = None

    def get_subcommands(self):
        return self.plugin_classes.keys()

    def get_subcommand_class(self, subcommand):
        return self.plugin_classes[subcommand]

    def init_parsers(
        self, parent_parser, parser_subcommands, parser_deploy, parser_render
    ):
        for subcommand in self.get_subcommands():
            plugin_class = self.get_subcommand_class(subcommand)

            # Handle Parent Parser args
            plugin_parent_arguments = plugin_class.get_parent_arguments()
            for name, options in plugin_parent_arguments.items():
                parent_parser.add_argument(name=name, **options)

            # Handle Subcommand args
            plugin_subcommand_arguments = plugin_class.get_subcommand_arguments()

            # Initialize parsers for existing subcommands
            parsers = {"render": parser_render, "deploy": parser_deploy}
            for options in plugin_subcommand_arguments:
                # Add/modify new custom subcommands
                if subcommand not in parsers:
                    parsers[subcommand] = parser_subcommands.add_parser(
                        subcommand, parents=[parent_parser]
                    )

                # Add the argument to the subcommand, existing or new
                name_or_flags = options.pop("name_or_flags")
                parsers[subcommand].add_argument(*name_or_flags, **options)

    def __str__(self):
        return f"Plugin {self.name}"

    def __repr__(self):
        return self.__str__()
