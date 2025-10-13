import csv
import logging
from collections.abc import Iterator
from typing import Any

from keboola.component import sync_actions
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException

from configuration import ColumnMappingItem, Configuration
from mailkit_client import MailkitClient


BATCH_SIZE = 5_000


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

        self.params = Configuration(**self.configuration.parameters)
        self.mkc = MailkitClient(self.params.client_id, self.params.client_md5)

    def run(self):
        tables = self.get_input_tables_definitions()

        for table in tables:
            col_mapping_dict = self._parse_column_mapping(self.params.column_mapping)
            for recipients in self._create_recipients_list_in_batches(table, col_mapping_dict):
                self.mkc.mailinglist_import(self.params.list_id, recipients)

    @sync_action("verifyCredentials")
    def verify_credentials(self):
        if self.mkc.mailinglist_list():
            return sync_actions.ValidationResult("Verification successful", sync_actions.MessageType.SUCCESS)
        return sync_actions.ValidationResult("Failed to verify credentials", sync_actions.MessageType.ERROR)

    def _create_recipients_list_in_batches(
        self,
        table: TableDefinition,
        column_mapping: dict[str, str],
    ) -> Iterator[list[dict]]:
        total = 0
        recipients = []
        with open(table.full_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                renamed_row = self._lowercase_columns(self._rename_columns(row, column_mapping))
                recipients.append(renamed_row)
                total += 1
                if total % BATCH_SIZE == 0:
                    logging.info(f"Importing {BATCH_SIZE} recipients (total: {total})...")
                    yield recipients
                    recipients = []

        if recipients:
            logging.info(f"Importing {len(recipients)} recipients (total: {total})...")
            yield recipients

    def _lowercase_columns(self, row: dict[str, Any]) -> dict[str, Any]:
        return {k.lower(): v for k, v in row.items()}

    def _rename_columns(
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
