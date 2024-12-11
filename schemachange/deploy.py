from __future__ import annotations

import hashlib
import re

import structlog

from schemachange.JinjaTemplateProcessor import JinjaTemplateProcessor
from schemachange.config.DeployConfig import DeployConfig
from schemachange.session.Script import get_all_scripts_recursively
from schemachange.session.SnowflakeSession import SnowflakeSession

logger = structlog.getLogger(__name__)


def alphanum_convert(text: str):
    if text.isdigit():
        return int(text)
    return text.lower()


# This function will return a list containing the parts of the key (split by number parts)
# Each number is converted to and integer and string parts are left as strings
# This will enable correct sorting in python when the lists are compared
# e.g. get_alphanum_key('1.2.2') results in ['', 1, '.', 2, '.', 2, '']
def get_alphanum_key(key: str | int | None) -> list:
    if key == "" or key is None:
        return []
    alphanum_key = [alphanum_convert(c) for c in re.split("([0-9]+)", key)]
    return alphanum_key


def sorted_alphanumeric(data):
    return sorted(data, key=get_alphanum_key)


class Deployment:
    def __init__(self, config: DeployConfig, session: SnowflakeSession):
        self.config = config
        self.session = session

        # Script metadata from the filesystem
        self.all_scripts = {}
        self.all_script_names = []
        self.all_script_names_sorted = []

        # Script deployment statistics
        self.scripts_skipped = 0
        self.scripts_applied = 0

        # Script metadata from the change history table
        self.versioned_scripts = {}
        self.r_scripts_checksum = {}
        self.max_published_version = None

    def get_script_history(self):
        (
            self.versioned_scripts,
            self.r_scripts_checksum,
            self.max_published_version,
        ) = self.session.get_script_metadata(
            create_change_history_table=self.config.create_change_history_table,
            dry_run=self.config.dry_run,
        )

        self.max_published_version = get_alphanum_key(self.max_published_version)

    def find_scripts(self):
        # Find all scripts in the root folder (recursively) and sort them correctly
        self.all_scripts = get_all_scripts_recursively(
            root_directory=self.config.root_folder,
        )
        self.all_script_names = list(self.all_scripts.keys())
        # Sort scripts such that versioned scripts get applied first and then the repeatable ones.
        self.all_script_names_sorted = (
            sorted_alphanumeric(
                [script for script in self.all_script_names if script[0] == "v"]
            )
            + sorted_alphanumeric(
                [script for script in self.all_script_names if script[0] == "r"]
            )
            + sorted_alphanumeric(
                [script for script in self.all_script_names if script[0] == "a"]
            )
        )

    def render_script_content(self, script) -> str:
        # Always process with jinja engine
        jinja_processor = JinjaTemplateProcessor(
            project_root=self.config.root_folder,
            modules_folder=self.config.modules_folder,
        )
        content = jinja_processor.render(
            jinja_processor.relpath(script.file_path),
            self.config.config_vars,
        )

        return content

    def script_checksum(self, content) -> str:
        return hashlib.sha224(content.encode("utf-8")).hexdigest()

    def deploy_scripts(self):
        # Loop through each script in order and apply any required changes
        for script_name in self.all_script_names_sorted:
            script = self.all_scripts[script_name]
            script_log = logger.bind(
                # The logging keys will be sorted alphabetically.
                # Appending 'a' is a lazy way to get the script name to appear at the start of the log
                a_script_name=script.name,
                script_version=getattr(script, "version", "N/A"),
            )

            # Get the content of the script and its checksum, after parsing it with the Jinja engine
            content = self.render_script_content(script)
            checksum_current = self.script_checksum(content)

            # Apply a versioned-change script only if the version is newer than the most recent change in the database
            # Apply any other scripts, i.e. repeatable scripts, irrespective of the most recent change in the database
            if script.type == "V":
                script_metadata = self.versioned_scripts.get(script.name)

                if (
                    self.max_published_version is not None
                    and get_alphanum_key(script.version) <= self.max_published_version
                ):
                    if script_metadata is None:
                        script_log.debug(
                            "Skipping versioned script because it's older than the most recently applied change",
                            max_published_version=self.max_published_version,
                        )
                        self.scripts_skipped += 1
                        continue
                    else:
                        script_log.debug(
                            "Script has already been applied",
                            max_published_version=self.max_published_version,
                        )
                        if script_metadata["checksum"] != checksum_current:
                            script_log.info(
                                "Script checksum has drifted since application"
                            )

                        self.scripts_skipped += 1
                        continue

            # Apply only R scripts where the checksum changed compared to the last execution of snowchange
            if script.type == "R":
                # check if R file was already executed
                if (
                    self.r_scripts_checksum is not None
                ) and script.name in self.r_scripts_checksum:
                    checksum_last = self.r_scripts_checksum[script.name][0]
                else:
                    checksum_last = ""

                # check if there is a change of the checksum in the script
                if checksum_current == checksum_last:
                    script_log.debug(
                        "Skipping change script because there is no change since the last execution"
                    )
                    self.scripts_skipped += 1
                    continue

            self.session.apply_change_script(
                script=script,
                script_content=content,
                dry_run=self.config.dry_run,
                logger=script_log,
            )

            self.scripts_applied += 1

        logger.info(
            "Completed successfully",
            scripts_applied=self.scripts_applied,
            scripts_skipped=self.scripts_skipped,
        )

    def run(self):
        # Support plugin provided pre/post tasks
        if hasattr(self.config, "pre_command_tasks") and callable(
            self.config.pre_command_tasks
        ):
            self.config.pre_command_tasks()

        self.deploy()

        if hasattr(self.config, "post_command_tasks") and callable(
            self.config.post_command_tasks
        ):
            self.config.post_command_tasks()

    def deploy(self):
        logger.info(
            "starting deploy",
            dry_run=self.config.dry_run,
            snowflake_account=self.session.account,
            default_role=self.session.role,
            default_warehouse=self.session.warehouse,
            default_database=self.session.database,
            default_schema=self.session.schema,
            change_history_table=self.session.change_history_table.fully_qualified,
        )

        # Pull the current script metadata and history from the database
        self.get_script_history()

        # Find and sort all scripts
        self.find_scripts()

        # Process and deploy script changes
        self.deploy_scripts()
