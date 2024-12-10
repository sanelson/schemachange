import structlog
from schemachange.config.DeployConfig import DeployConfig
from schemachange.config.PluginConfig import Plugin, PluginBaseConfig

logger = structlog.getLogger(__name__)

# Plugin specific imports


class SchemachangePlugin(Plugin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.plugin_name = "sqlutil"
        self.plugin_type = "custom"
        #        self.plugin_path = "schemachange.plugins.sqlutil"
        self.plugin_description = "A custom plugin for SQL utilities"
        self.plugin_version = "0.1.0"
        self.plugin_author = "Sam Nelson"
        self.plugin_author_email = "sanelson@siliconfuture.net"

        # Plugin subcommands and their classes
        self.plugin_classes = {
            "deploy": DeployPluginConfig,
            "sqlutil": SQLUtilPluginConfig,
        }


class SQLUtilPluginConfig(PluginBaseConfig):
    plugin_subcommand = "sqlutil"
    plugin_parent_arguments = {}
    plugin_subcommand_arguments = [
        {
            "name_or_flags": [
                "--analyze-sql",
            ],
            "action": "store_const",
            "const": True,
            "default": None,
            "help": "Analyze SQL and re-run all dependent R__ scripts of the changed/new R__ and new V__ scripts (the default is False)",
            "required": False,
        },
    ]


class DeployPluginConfig(PluginBaseConfig, DeployConfig):
    plugin_subcommand = "deploy"
    plugin_parent_arguments = {}
    plugin_subcommand_arguments = [
        {
            "name_or_flags": [
                "--run-deps",
            ],
            "action": "store_const",
            "const": True,
            "default": None,
            "help": "Analyze SQL and re-run all dependent R__ scripts of the changed/new R__ and new V__ scripts (the default is False)",
            "required": False,
        },
        {
            "name_or_flags": [
                "--rerun-repeatable",
            ],
            "action": "store_const",
            "const": True,
            "default": None,
            "help": "Rerun ALL repeatable sc,ripts (the default is False)",
            "required": False,
        },
    ]

    def pre_command_tasks(self):
        print("Pre-command tasks")
        return

    def post_command_tasks(self):
        print("Post-command tasks")
        return
