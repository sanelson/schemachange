# from __future__ import annotations
import importlib
import pkgutil
from structlog import BoundLogger
import structlog
import re
import structlog
import copy
import dataclasses

# from typing import Literal, TypeVar

from schemachange.config.BaseConfig import BaseConfig

logger = structlog.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class PluginBaseConfig(BaseConfig):
    plugin_subcommand: str | None = None
    plugin_parent_arguments: list = dataclasses.field(default_factory=list)
    plugin_subcommand_arguments: list = dataclasses.field(default_factory=list)
    #    plugin_subcommand = None
    #    plugin_parent_arguments = []
    #    plugin_subcommand_arguments = []

    #    def __post_init__(self):
    #        super().__init__()
    #
    #        self.analyze_sql = True

    @classmethod
    def get_subcommand(cls):
        return cls.plugin_subcommand

    @classmethod
    def get_parent_arguments(cls):
        return cls.plugin_parent_arguments

    @classmethod
    def get_subcommand_arguments(cls):
        return cls.plugin_subcommand_arguments

    #    def get_parser_kwargs(self):
    #        kwargs = [self.plugin_parent_arguments, self.plugin_subcommand_arguments]

    @classmethod
    def get_all_kwargs(cls):
        kwargs = []
        class_arguments = cls.get_parent_arguments() + cls.get_subcommand_arguments()
        logger.debug("Class arguments", class_arguments=class_arguments)
        # for option in cls.plugin_parent_arguments, cls.plugin_subcommand_arguments:
        for option in class_arguments:
            kwargs.extend(option.get("name_or_flags", []))

        return kwargs

    @classmethod
    def get_clean_kwargs(cls):
        kwargs = cls.get_all_kwargs()
        logger.debug("All dirty plugin kwargs", kwargs=kwargs)
        regex = re.compile(r"^-{1,2}")
        clean_kwargs = [
            re.sub(regex, "", substr).replace("-", "_") for substr in kwargs
        ]
        # substitute inner dashes to underscores without regex
        #        clean_kwargs = [substr.replace("-", "_") for substr in clean_kwargs]
        return clean_kwargs

    @classmethod
    def match_class_kwargs(cls, cli_kwargs):
        """
        Used to compare the current command invocation to the subcommand class kwargs
        """
        logger.debug("Incoming CLI kwargs", cli_kwargs=cli_kwargs)
        clean_kwargs = cls.get_clean_kwargs()
        logger.debug("Clean kwargs", clean_kwargs=clean_kwargs)
        for kwarg in clean_kwargs:
            if kwarg in cli_kwargs:
                return True
        return False

    def plugin_run(self):
        print(f"Running {self.get_subcommand()} plugin")
        return


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
        self.logger.debug("Plugin Subcommands", subcommands=subcommands)
        return subcommands

    def get_plugin_class_by_kwargs(self, cli_kwargs):
        subcommand = cli_kwargs["subcommand"]
        self.logger.debug("Plugins:", plugins=self.plugins)
        for name, plugin in self.plugins.items():
            self.logger.debug("Matching plugin", name=name)
            self.logger.debug("Matching plugin type", type=type(plugin))
            self.logger.debug(f"Matching kwargs for plugin {name}")
            #            plugin_class = plugin.get_subcommand_class_from_kwargs(
            #                subcommand=subcommand, cli_kwargs=cli_kwargs
            #            )
            #            #            self.logger.debug(f"Matched plugin {name}")
            #            self.logger.debug(f"Plugin matched {plugin_class.__str__()}")
            #            return plugin_class

            if plugin_class := plugin.get_subcommand_class_from_kwargs(
                subcommand=subcommand, cli_kwargs=cli_kwargs
            ):
                self.logger.debug(f"Matched plugin {name}")
                return plugin_class
        return None


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
        return self.plugin_classes.get(subcommand, None)

    def get_subcommand_class_from_kwargs(self, subcommand, cli_kwargs):
        self.logger.debug(f"Matching subcommand {subcommand} to plugin {self.name}")
        if plugin_class := self.get_subcommand_class(subcommand=subcommand):
            self.logger.debug(f"Matched plugin {plugin_class}")
            if plugin_class.match_class_kwargs(cli_kwargs=cli_kwargs):
                return plugin_class
        return None

    def init_parsers(
        self, parent_parser, parser_subcommands, parser_deploy, parser_render
    ):
        for subcommand in self.get_subcommands():
            plugin_class = self.get_subcommand_class(subcommand)

            # Handle Parent Parser args
            plugin_parent_arguments = plugin_class.get_parent_arguments()
            for options in plugin_parent_arguments:
                options_copy = copy.deepcopy(options)
                name_or_flags = options.pop("name_or_flags")
                parent_parser.add_argument(*name_or_flags, **options_copy)

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
                # Also, only remove the name_or_flags key from a copy of the options dict
                # NOTE: Is there a better way to do this? Seems wasteful...
                options_copy = copy.deepcopy(options)
                name_or_flags = options_copy.pop("name_or_flags")
                parsers[subcommand].add_argument(*name_or_flags, **options_copy)

    def __str__(self):
        return f"Plugin {self.name}"

    def __repr__(self):
        return self.__str__()
