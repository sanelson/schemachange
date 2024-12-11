from __future__ import annotations

import hashlib
import re

import structlog

from schemachange.JinjaTemplateProcessor import JinjaTemplateProcessor
from schemachange.config.JobConfig import JobConfig
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


class Job:
    def __init__(self, config: JobConfig, session: SnowflakeSession):
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
