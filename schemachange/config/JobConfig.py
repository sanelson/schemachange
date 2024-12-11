from __future__ import annotations

import dataclasses
from pathlib import Path

from schemachange.config.BaseConfig import BaseConfig
from schemachange.config.ChangeHistoryTable import ChangeHistoryTable
from schemachange.config.utils import get_snowflake_identifier_string


@dataclasses.dataclass(frozen=True)
class JobConfig(BaseConfig):
    subcommand: str | None = None
    connections_file_path: Path | None = None
    connection_name: str | None = None
    # TODO: Turn change_history_table into three arguments. There's no need to parse it from a string
    change_history_table: ChangeHistoryTable | None = dataclasses.field(
        default_factory=ChangeHistoryTable
    )
    create_change_history_table: bool = False
    autocommit: bool = False
    dry_run: bool = False
    query_tag: str | None = None

    @classmethod
    def factory(
        cls,
        subcommand: str,
        config_file_path: Path,
        change_history_table: str | None = None,
        **kwargs,
    ):
        if "subcommand" in kwargs:
            kwargs.pop("subcommand")

        change_history_table = ChangeHistoryTable.from_str(
            table_str=change_history_table
        )

        return super().factory(
            subcommand=subcommand,
            config_file_path=config_file_path,
            change_history_table=change_history_table,
            **kwargs,
        )

    def get_session_kwargs(self) -> dict:
        session_kwargs = {
            "connections_file_path": self.connections_file_path,
            "connection_name": self.connection_name,
            "change_history_table": self.change_history_table,
            "autocommit": self.autocommit,
            "query_tag": self.query_tag,
        }

        return {k: v for k, v in session_kwargs.items() if v is not None}
