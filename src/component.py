import csv
import logging
from typing import Any

from keboola.component.base import CommonInterface, ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException

from configuration import ColumnMappingItem, Configuration
from mailkit_client import MailkitClient
from sync_action_templates import SyncActionTemplates


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

        self.params = Configuration(**self.configuration.parameters)
        self.mkc = MailkitClient(self.params.client_id, self.params.client_md5)

    def run(self):
        ci = CommonInterface()
        tables = ci.get_input_tables_definitions()

        for table in tables:
            col_mapping_dict = self._parse_column_mapping(self.params.column_mapping)
            recipients = self._create_recipients_list(table, col_mapping_dict)

            result = self.mkc.mailinglist_import(self.params.list_id, recipients)

            logging.info(f"Mailing list import result: {result}")

    @sync_action("verifyCredentials")
    def list_mailing_lists(self):
        if self.mkc.mailinglist_list():
            return SyncActionTemplates.success("Verification successful")
        return SyncActionTemplates.error("Failed to verify credentials")

    def _create_recipients_list(
        self,
        table: TableDefinition,
        column_mapping: dict[str, str],
    ):
        recipients = []
        with open(table.full_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                recipients.append(self._get_renamed_columns(row, column_mapping))
        logging.info(f"Recipients count: {len(recipients)}")
        return recipients

    def _get_renamed_columns(
        self,
        row: dict[str, Any],
        column_mapping: dict[str, str],
    ) -> dict[str, str]:
        if not column_mapping:
            return row

        renamed_row = {}
        for k, v in row.items():
            renamed_row[column_mapping.get(k) or k] = v

        return renamed_row

    def _parse_column_mapping(self, column_mapping: list[ColumnMappingItem]) -> dict[str, str]:
        return {item.src_col: item.dest_col for item in column_mapping}


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
