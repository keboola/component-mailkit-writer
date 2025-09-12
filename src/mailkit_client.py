import logging
from dataclasses import dataclass

import backoff
import requests
from keboola.component.exceptions import UserException

ENDPOINT = "https://api.mailkit.eu/json.fcgi"


@dataclass
class MailkitClient:
    client_id: str
    client_md5: str

    @backoff.on_exception(backoff.expo, (requests.Timeout, requests.HTTPError), max_tries=3)
    def _post_with_retry(self, payload) -> requests.Response:
        resp = requests.post(ENDPOINT, json=payload)
        logging.debug("Mailkit API response: HTTP %i %s", resp.status_code, resp.reason)
        logging.debug("Response body: %s", resp.text)
        resp.raise_for_status()
        return resp

    def mailinglist_list(self) -> dict | None:
        payload = {
            "function": "mailkit.mailinglist.list",
            "id": self.client_id,
            "md5": self.client_md5,
        }

        try:
            resp = self._post_with_retry(payload)
            if resp.status_code != 200:
                raise UserException(resp.text)

            result = resp.json()
            if result:
                logging.info("Getting list of mailing lists: OK")
            return result
        except Exception as e:
            logging.exception("Failed to get list of mailing lists: %s", e)

        return None

    def mailinglist_import(self, mailing_list_id: int, recipients: list[dict]) -> None:
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
            resp = self._post_with_retry(payload)
            if resp.status_code != 200:
                raise UserException(resp.text)

            result = resp.json()
            if result:
                # mailkit api currently returns 0 counts in every category (ok, skipped etc.)
                # even in case of successful import, so we just log success when the import returned
                # valid JSON response with HTTP 200
                logging.info("Importing recipients: OK")
        except Exception as e:
            raise Exception(f"Failed to import recipients: {e!r}")
