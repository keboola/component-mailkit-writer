import logging
from dataclasses import dataclass

import requests
from keboola.component.exceptions import UserException

ENDPOINT = "https://api.mailkit.eu/json.fcgi"


@dataclass
class MailkitClient:
    client_id: str
    client_md5: str

    def mailinglist_list(self):
        payload = {
            "function": "mailkit.mailinglist.list",
            "id": self.client_id,
            "md5": self.client_md5,
        }

        try:
            resp = requests.post(ENDPOINT, json=payload)
            logging.info("Mailkit API response: HTTP %i", resp.status_code)
            logging.debug("Response body: %s", resp.text)
            if resp.status_code != 200:
                raise UserException(f"Failed to get list of mailing lists: {resp.text}")
            return True
        except Exception as e:
            logging.exception("Error getting list of mailing lists: %s", e)

        return False

    def mailinglist_import(self, mailing_list_id: int, recipients: list[dict]):
        payload = {
            "function": "mailkit.mailinglist.import",
            "id": self.client_id,
            "md5": self.client_md5,
            "parameters": {
                "ID_user_list": mailing_list_id,
                "recipients": recipients,
            },
        }

        try:
            resp = requests.post(ENDPOINT, json=payload)
            logging.info("Mailkit API response: HTTP %i", resp.status_code)
            logging.debug("Response body: %s", resp.text)
            if resp.status_code != 200:
                raise UserException(f"Failed to import mailing list: {resp.text}")
        except Exception as e:
            raise Exception(f"Error during mailing list import: {e!r}")
