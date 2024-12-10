import hashlib
from pathlib import Path

import structlog
from structlog import BoundLogger
import logging
import sys

from schemachange.JinjaTemplateProcessor import JinjaTemplateProcessor
from schemachange.config.RenderConfig import RenderConfig
from schemachange.config.Plugin import PluginCollection
from schemachange.config.get_merged_config import get_merged_config

from schemachange.deploy import Deployment
from schemachange.redact_config_secrets import redact_config_secrets
from schemachange.session.SnowflakeSession import SnowflakeSession

# region Global Variables
# metadata
SCHEMACHANGE_VERSION = "4.0.0"
SNOWFLAKE_APPLICATION_NAME = "schemachange"
module_logger = structlog.getLogger(__name__)


def render(config: RenderConfig, script_path: Path, logger: BoundLogger) -> None:
    """
    Renders the provided script.

    Note: does not apply secrets filtering.
    """
    # Always process with jinja engine
    jinja_processor = JinjaTemplateProcessor(
        project_root=config.root_folder, modules_folder=config.modules_folder
    )
    content = jinja_processor.render(
        jinja_processor.relpath(script_path), config.config_vars
    )

    checksum = hashlib.sha224(content.encode("utf-8")).hexdigest()
    logger.info("Success", checksum=checksum, content=content)


def set_log_level() -> None:
    log_level_str = "INFO"

    if "--verbose" in sys.argv or "-v" in sys.argv:
        log_level_str = "DEBUG"
    elif "--loglevel" in sys.argv:
        log_level_str = sys.argv[sys.argv.index("--loglevel") + 1].upper()

    try:
        # Python 3.11 and later
        if sys.version_info.major > 3 and sys.version_info.minor >= 11:
            log_level = logging.getLevelNamesMapping()[log_level_str]
        else:
            # Use the older deprecated method
            log_level = logging.getLevelName(log_level_str)
    except KeyError:
        module_logger.error(f"Invalid log level provided: {log_level_str}")

        # Fall back to INFO
        log_level = logging.INFO

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
    )


def main():
    # Configure structlog log level early in initialization
    set_log_level()

    module_logger.info(
        "schemachange version: %(schemachange_version)s"
        % {"schemachange_version": SCHEMACHANGE_VERSION}
    )

    # Load SchemaChange plugins
    plugins = PluginCollection()
    plugins.load_plugins()

    # TBD: Add plugin to load list to config
    config = get_merged_config(logger=module_logger, plugins=plugins)
    redact_config_secrets(config_secrets=config.secrets)

    logger = structlog.getLogger()
    logger = logger.bind(schemachange_version=SCHEMACHANGE_VERSION)

    config.log_details()

    # Finally, execute the command
    if config.subcommand == "render":
        render(
            config=config,
            script_path=config.script_path,
            logger=logger,
        )
    elif config.subcommand == "deploy":
        session = SnowflakeSession(
            schemachange_version=SCHEMACHANGE_VERSION,
            application=SNOWFLAKE_APPLICATION_NAME,
            logger=logger,
            **config.get_session_kwargs(),
        )
        deploy = Deployment(config=config, session=session)
        deploy.run()
    else:
        module_logger.info(
            "Custom plugin subcommand chosen", subcommand=config.subcommand
        )
        config.plugin_run()


if __name__ == "__main__":
    main()
