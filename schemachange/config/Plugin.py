# from __future__ import annotations
import importlib
import pkgutil
import re
import structlog
import copy
import dataclasses

from schemachange.config.BaseConfig import BaseConfig
from schemachange.config.JobConfig import JobConfig

logger = structlog.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class PluginBaseConfig(BaseConfig):
    plugin_name: str | None = None
    plugin_enabled: bool = False
    plugin_description: str | None = None
    plugin_subcommand: str | None = None
    plugin_parent_arguments: list = dataclasses.field(default_factory=list)
    plugin_subcommand_arguments: list = dataclasses.field(default_factory=list)

    @classmethod
    def get_subcommand(cls):
        return cls.plugin_subcommand

    @classmethod
    def get_parent_arguments(cls):
        return cls.plugin_parent_arguments

    @classmethod
    def get_subcommand_arguments(cls):
        return cls.plugin_subcommand_arguments

    @classmethod
    def get_all_kwargs(cls):
        kwargs = []
        class_arguments = cls.get_parent_arguments() + cls.get_subcommand_arguments()
        logger.debug("Plugin class arguments", plugin_class_arguments=class_arguments)
        for option in class_arguments:
            kwargs.extend(option.get("name_or_flags", []))

        return kwargs

    @classmethod
    def get_clean_kwargs(cls):
        raw_kwargs = cls.get_all_kwargs()
        logger.debug("All raw plugin kwargs", raw_kwargs=raw_kwargs)
        regex = re.compile(r"^-{1,2}")
        clean_kwargs = [
            re.sub(regex, "", substr).replace("-", "_") for substr in raw_kwargs
        ]
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

    def __post_init__(self):
        if not self.plugin_name:
            self.plugin_name = type(self)

        if not self.plugin_description:
            self.plugin_description = f"Plugin: {self.plugin_name}"

    # Default plugin run method, override in subclass
    def plugin_run(self):
        logger.info("Running plugin", plugin=self.get_subcommand())
        return


@dataclasses.dataclass(frozen=True)
class PluginJobConfig(PluginBaseConfig, JobConfig):
    # Common arguments used by all *Job* type plugins
    # This is a *hack* until the JobConfig/DeployConfig classes are refactored
    plugin_parent_arguments = [
        {
            "name_or_flags": [
                "--connections-file-path",
            ],
            "type": str,
            "help": "Override the default connections.toml file path at snowflake.connector.constants.CONNECTIONS_FILE (OS specific)",
            "required": False,
        },
        {
            "name_or_flags": [
                "--connection-name",
            ],
            "type": str,
            "help": "Override the default connections.toml connection name. Other connection-related values will override these connection values.",
            "required": False,
        },
        {
            "name_or_flags": [
                "--change-history-table",
            ],
            "type": str,
            "help": "Used to override the default name of the change history table (the default is METADATA.SCHEMACHANGE.CHANGE_HISTORY)",
            "required": False,
        },
        {
            "name_or_flags": [
                "--create-change-history-table",
            ],
            "action": "store_const",
            "const": True,
            "default": None,
            "help": "Create the change history schema and table, if they do not exist (the default is False)",
            "required": False,
        },
        {
            "name_or_flags": [
                "-ac",
                "--autocommit",
            ],
            "action": "store_const",
            "const": True,
            "default": None,
            "help": "Enable autocommit feature for DML commands (the default is False)",
            "required": False,
        },
        {
            "name_or_flags": [
                "--dry-run",
            ],
            "action": "store_const",
            "const": True,
            "default": None,
            "help": "Run schemachange in dry run mode (the default is False)",
            "required": False,
        },
        {
            "name_or_flags": [
                "--query-tag",
            ],
            "type": str,
            "help": "The string to add to the Snowflake QUERY_TAG session value for each query executed",
            "required": False,
        },
    ]


class PluginCollection:
    def __init__(self):
        self.plugins = {}

    def __str__(self):
        return f"PluginCollection {self.plugins}"

    def __repr__(self):
        return self.__str__()

    def exist(self):
        if self.plugins:
            return True
        return False

    def enabled(self):
        enabled_plugins = []
        for name, plugin in self.plugins.items():
            if plugin.plugin_enabled:
                enabled_plugins.append(plugin)
        return enabled_plugins

    def import_plugin(self, name: str):
        try:
            plugin = importlib.import_module(name)
        except ImportError as e:
            logger.warning(f"Failed to import plugin {name}")
            logger.debug("Error importing plugin: %s", e)
            return None
        logger.debug(f"Imported {name} plugin")
        return plugin

    def load_plugins(self):
        # Discover all Schemachange plugins
        logger.info("Discovering and importing plugins")
        discovered_plugins = []
        try:
            for finder, name, ispkg in pkgutil.iter_modules():
                if name.startswith("schemachange_"):
                    logger.debug("Found module", finder=finder, name=name, ispkg=ispkg)
                    discovered_plugins.append(name)
        except Exception as e:
            logger.error("Error discovering plugins", error=e)
            return None
        logger.debug("Discovered plugins", plugins=discovered_plugins)

        # Import all discovered plugins
        for plugin in discovered_plugins:
            # Initialize/import plugins
            logger.debug("Importing plugins")

            # If the plugin fails to import, skip it
            if not (plugin_module := self.import_plugin(name=plugin)):
                continue

            # Instatiate the plugin
            try:
                custom_plugin = plugin_module.SchemachangePlugin(name=plugin)
            except Exception as e:
                logger.error(f"Error instantiating plugin {plugin}", error=e)
                continue

            self.plugins[plugin] = custom_plugin

    def init_parsers(
        self, parent_parser, parser_subcommands, parser_deploy, parser_render
    ):
        for name, plugin in self.plugins.items():
            logger.debug("Initializing parsers for plugin", name=name)
            plugin.init_parsers(
                parent_parser=parent_parser,
                parser_subcommands=parser_subcommands,
                parser_deploy=parser_deploy,
                parser_render=parser_render,
            )

    def get_subcommands(self):
        subcommands = []
        for name, plugin in self.plugins.items():
            logger.debug("Getting subcommands for plugin", name=name)
            subcommands.extend(plugin.get_subcommands())

        # Remove duplicates
        subcommands = list(set(subcommands))
        logger.debug("Supported plugin subcommands", subcommands=subcommands)
        return subcommands

    def get_plugin_class_by_kwargs(self, cli_kwargs):
        subcommand = cli_kwargs["subcommand"]
        for name, plugin in self.plugins.items():
            if plugin_class := plugin.get_subcommand_class_from_kwargs(
                subcommand=subcommand, cli_kwargs=cli_kwargs
            ):
                logger.debug("Matched plugin", name=name, plugin_class=plugin_class)
                # Mark plugin as enabled
                plugin_class.plugin_enabled = True
                return plugin_class
        return None


class Plugin:
    name = None
    plugin_classes = {}  # Dict of subcommand: Class

    def __init__(self, name: str):
        self.name = name

    def get_subcommands(self):
        return self.plugin_classes.keys()

    def get_subcommand_class(self, subcommand):
        return self.plugin_classes.get(subcommand, None)

    def get_subcommand_class_from_kwargs(self, subcommand, cli_kwargs):
        logger.debug("Matching subcommand to plugin", subcommand=subcommand)
        if plugin_class := self.get_subcommand_class(subcommand=subcommand):
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

            if not plugin_class.plugin_name:
                plugin_class.plugin_name = plugin_class.__name__

            parent_group_or_parser = parent_parser
            # Deal with JobConfig/DeployConfig parent arguments *hack*
            if (
                plugin_parent_arguments
                and PluginJobConfig not in plugin_class.__bases__
            ):
                parent_group_or_parser = parent_parser.add_argument_group(
                    plugin_class.plugin_name, plugin_class.plugin_description
                )
            for options in plugin_parent_arguments:
                options_copy = copy.deepcopy(options)
                name_or_flags = options_copy.pop("name_or_flags")
                parent_group_or_parser.add_argument(*name_or_flags, **options_copy)

            # Handle Subcommand args
            plugin_subcommand_arguments = plugin_class.get_subcommand_arguments()

            # Initialize parsers for existing subcommands
            parsers = {"render": parser_render, "deploy": parser_deploy}
            argument_groups = {}

            # Initialize parser plugin group for existing subcommands
            if plugin_subcommand_arguments and subcommand in parsers:
                logger.debug(
                    "Adding argument group to existing subcommand",
                    subcommand=subcommand,
                )
                argument_groups[subcommand] = parsers[subcommand].add_argument_group(
                    plugin_class.plugin_name, plugin_class.plugin_description
                )
            for options in plugin_subcommand_arguments:
                # Add/modify new custom subcommands
                if subcommand not in parsers:
                    logger.debug(
                        "Adding new subcommand and argument group",
                        subcommand=subcommand,
                    )
                    parsers[subcommand] = parser_subcommands.add_parser(
                        subcommand, parents=[parent_parser]
                    )
                    argument_groups[subcommand] = parsers[
                        subcommand
                    ].add_argument_group(
                        plugin_class.plugin_name, plugin_class.plugin_description
                    )

                # Add the argument to the subcommand, existing or new
                # Also, only remove the name_or_flags key from a copy of the options dict
                # NOTE: Is there a better way to do this? Seems wasteful...
                options_copy = copy.deepcopy(options)
                name_or_flags = options_copy.pop("name_or_flags")
                argument_groups[subcommand].add_argument(*name_or_flags, **options_copy)

    def __str__(self):
        return f"Plugin {self.name}"

    def __repr__(self):
        return self.__str__()
