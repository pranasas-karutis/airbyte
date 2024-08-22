#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from airbyte_cdk.sources.streams.core import IncrementalMixin

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class KonnektivecrmStream(HttpStream, ABC):

    url_base = "https://api.konnektive.com/"
    resultsPerPage: int = 200
    timeDaysInterval: int = 10

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.config = config
        self.loginId = config["loginId"]
        self.password = config["password"]
        self.startDate = config["startDate"]
        if "endDate" in config:
            self.endDate = config["endDate"]
        else:
            self.endDate = datetime.strftime(datetime.today(), '%m/%d/%y')
        self.page = 1
        self._cursor_value = None  # Initialize cursor value
        self._adjust_start_date_if_needed()  # Adjust start date if necessary

    # We sync in batches, adjusted by timeDaysInterval constant. If the interval is too
    # short from the initial date, API returns error. The below logic handles it and adjusts
    # the startDate as required.
    def _adjust_start_date_if_needed(self):
        while True:
            response = self._check_date_range(self.startDate, self.endDate)
            if response.get("result") == "SUCCESS":
                break
            if "matching those parameters could be found" in response.get("message", ""):
                new_start_date_obj = datetime.strptime(self.startDate, '%m/%d/%y') + timedelta(days=self.timeDaysInterval)
                self.startDate = new_start_date_obj.strftime('%m/%d/%y')
                self.logger.error(f"Adjusting startDate: {self.startDate}")
            else:
                break  # Handle unexpected errors or stop condition

    def _check_date_range(self, start_date: str, end_date: str) -> Mapping[str, Any]:
        params = {
            "loginId": self.loginId,
            "password": self.password,
            "startDate": start_date,
            "endDate": end_date,
            "resultsPerPage": 1,
            "page": 1
        }
        response = requests.get(self.url_base + self.path(), params=params)
        return response.json()

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        total_results = int(response.json()["message"]["totalResults"])
        page_size = int(response.json()["message"]["resultsPerPage"])
        current_page = int(response.json()["message"]["page"])

        if page_size * current_page < total_results:
            self.page = current_page + 1
            return self.page
        else:
            # Update endDate to the next set-day interval
            start_date_obj = datetime.strptime(self.effective_start_date, '%m/%d/%y')
            new_start_date_obj = start_date_obj + timedelta(days=self.timeDaysInterval+1)
            new_end_date_obj = new_start_date_obj + timedelta(days=self.timeDaysInterval+1)
            today_date_obj = datetime.today()

            # Ensure the end date does not exceed the configured endDate or today's date
            if "endDate" in self.config:
                user_end_date_obj = datetime.strptime(self.config["endDate"], '%m/%d/%y')
                new_end_date_obj = min(new_end_date_obj, user_end_date_obj, today_date_obj)
            else:
                new_end_date_obj = min(new_end_date_obj, today_date_obj)

            if new_start_date_obj > new_end_date_obj or new_start_date_obj > today_date_obj:
                return None  # No more data to fetch

            self.startDate = new_start_date_obj.strftime('%m/%d/%y')
            self.endDate = new_end_date_obj.strftime('%m/%d/%y')

            # Reset the page to 1
            self.page = 1

            return self.page

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        try:
            json_response = response.json()
            message = json_response.get("message")

            if not message or "data" not in message:
                self.logger.error(f"Got bad response from API. Response: {response.text}")
                return

            for result in message["data"]:
                yield result
        except ValueError:
            self.logger.error(f"Failed to parse JSON from response: {response.text}")
            raise
        except KeyError:
            self.logger.error(f"Expected key not found in response JSON: {response.text}")
            raise

    @staticmethod
    def _format_date(date_str: str, current_format: str, desired_format: str) -> str:
        date = datetime.strptime(date_str, current_format)
        return date.strftime(desired_format)

class IncrementalKonnektivecrmStream(KonnektivecrmStream, IncrementalMixin, ABC):
    state_checkpoint_interval = 50000

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config, **kwargs)
        start_date_obj = datetime.strptime(self.startDate, '%m/%d/%y')
        new_end_date_obj = start_date_obj + timedelta(days=self.timeDaysInterval)
        today_date_obj = datetime.today()

        if "endDate" in config:
            user_end_date_obj = datetime.strptime(config["endDate"], '%m/%d/%y')
            self.endDate = min(new_end_date_obj, user_end_date_obj, today_date_obj).strftime('%m/%d/%y')
        else:
            self.endDate = min(new_end_date_obj, today_date_obj).strftime('%m/%d/%y')

        self.effective_start_date = self.startDate

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "dateUpdated"

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field)

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        if stream_state and not (self.page > 1):
            if stream_state[self.cursor_field]:
                start_date = stream_state[self.cursor_field]
                self.startDate = self._format_date(start_date, '%Y-%m-%d %H:%M:%S', '%m/%d/%y')
                self.effective_start_date = self._format_date(start_date, '%Y-%m-%d %H:%M:%S', '%m/%d/%y')

                start_date_obj = datetime.strptime(self.startDate, '%m/%d/%y')
                new_end_date_obj = start_date_obj + timedelta(days=self.timeDaysInterval)
                today_date_obj = datetime.today()
                self.endDate = min(new_end_date_obj, today_date_obj).strftime('%m/%d/%y')
            else:
                self.effective_start_date = self.startDate
        elif not stream_state:
            self.effective_start_date = self.startDate

        params = {"page": self.page, "resultsPerPage": self.resultsPerPage, "startDate": self.effective_start_date, "endDate": self.endDate,
                  "loginId": self.loginId, "password": self.password, 'dateRangeType': 'dateUpdated'}

        self.logger.info(f"The params that were formed: {params}")
        return params

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(*args, **kwargs)
        for record in records:
            yield record
            latest_record = record.get(self.cursor_field)
            # self.logger.info(f"value of latest_record: {latest_record}")
            latest_record_date = datetime.strptime(latest_record, '%Y-%m-%d %H:%M:%S')
            if self._cursor_value:
                current_state_date = datetime.strptime(self._cursor_value, '%Y-%m-%d %H:%M:%S')
                self._cursor_value = max(current_state_date, latest_record_date).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self._cursor_value = latest_record_date.strftime('%Y-%m-%d %H:%M:%S')
            # self.logger.info(f"Trying to update state: {self._cursor_value}")
            # self.logger.info(f"Full current state is: {self.state}")


class Customers(IncrementalKonnektivecrmStream):
    primary_key = "customerId"

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "dateUpdated"

    def path(
            self, next_page_token: Mapping[str, Any] = None, **kwargs) -> str:
        return "customer/query/"

class Transactions(IncrementalKonnektivecrmStream):
    primary_key = "transactionId"

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "dateUpdated"

    def path(
            self, next_page_token: Mapping[str, Any] = None, **kwargs) -> str:
        return "transactions/query/"

class Order(IncrementalKonnektivecrmStream):
    primary_key = "orderId"

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "dateUpdated"

    def path(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> str:
        return "order/query/"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token
        )
        params["includeCustomFields"] = 1
        return params

class Purchases(IncrementalKonnektivecrmStream):
    primary_key = "purchaseId"

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "dateUpdated"

    def path(
        self, next_page_token: Mapping[str, Any] = None, **kwargs) -> str:
        return "purchase/query/"

class Summary(IncrementalKonnektivecrmStream):
    primary_key = "date"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # Update endDate to the next set-day interval
        start_date_obj = datetime.strptime(self.effective_start_date, '%m/%d/%y')
        new_start_date_obj = start_date_obj + timedelta(days=self.timeDaysInterval + 1)
        new_end_date_obj = new_start_date_obj + timedelta(days=self.timeDaysInterval + 1)
        today_date_obj = datetime.today()

        # Ensure the end date does not exceed the configured endDate or today's date
        if "endDate" in self.config:
            user_end_date_obj = datetime.strptime(self.config["endDate"], '%m/%d/%y')
            new_end_date_obj = min(new_end_date_obj, user_end_date_obj, today_date_obj)
        else:
            new_end_date_obj = min(new_end_date_obj, today_date_obj)

        if new_start_date_obj > new_end_date_obj or new_start_date_obj > today_date_obj:
            return None  # No more data to fetch

        self.startDate = new_start_date_obj.strftime('%m/%d/%y')
        self.endDate = new_end_date_obj.strftime('%m/%d/%y')

        return 1 #return any value, for Summary does not use pagination

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field)

    def path(
        self, next_page_token: Mapping[str, Any] = None, **kwargs) -> str:
        return "transactions/summary/"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        if stream_state and self.cursor_field in stream_state and stream_state[self.cursor_field]:
            self.startDate = datetime.strptime(stream_state[self.cursor_field], '%Y-%m-%d').strftime('%m/%d/%y')

        params = {
            "startDate": self.startDate,
            "endDate": self.endDate,
            "loginId": self.loginId,
            "password": self.password,
            'dateRangeType': 'txnDate',
            'reportType': 'date'
        }

        self.logger.info(f"The params that were formed: {params}")
        return params

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        records = KonnektivecrmStream.read_records(self, *args, **kwargs)
        for record in records:
            yield record
            latest_record = record.get(self.cursor_field)
            self.logger.info(f"value of latest_record: {latest_record}")
            latest_record_date = datetime.strptime(latest_record, '%Y-%m-%d')
            if self._cursor_value:
                current_state_date = datetime.strptime(self._cursor_value, '%Y-%m-%d')
                self._cursor_value = max(current_state_date, latest_record_date).strftime('%Y-%m-%d')
            else:
                self._cursor_value = latest_record_date.strftime('%Y-%m-%d')
            self.logger.info(f"Trying to update state: {self._cursor_value}")
            self.logger.info(f"Full current state is: {self.state}")

    def parse_response(
            self,
            response: requests.Response,
            next_page_token: Mapping[str, Any] = None,

            **kwargs
    ) -> Iterable[Mapping]:
        try:
            json_response = response.json()
            self.logger.error(f"The received response is: {response.text}")
            for result in json_response["message"]:
                yield result
        except ValueError:
            self.logger.error(f"Failed to parse JSON from response: {response.text}")
            raise
        except KeyError:
            self.logger.error(f"Expected key not found in response JSON: {response.text}")
            raise

# Source
class SourceKonnektivecrm(AbstractSource):
    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        endDate = datetime.strftime(datetime.today(), '%m/%d/%y')
        params = {'loginId': config["loginId"], 'password': config["password"],
                  'startDate': config["startDate"], 'endDate': endDate, 'dateRangeType': 'dateUpdated'}
        r = requests.get(url=Customers.url_base+"customer/query/", params=params)

        result = (r.json()["result"])
        message = (r.json()["message"])

        if result == "SUCCESS":
            return True, None
        else:
            return False, message

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        incremental_streams = [Customers(config=config), Order(config=config), Transactions(config=config), Purchases(config=config), Summary(config=config)]
        return incremental_streams
