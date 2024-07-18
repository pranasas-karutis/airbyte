#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from datetime import datetime
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

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.loginId = config["loginId"]
        self.password = config["password"]
        self.startDate = config["startDate"]
        if "endDate" in config:
            self.endDate = config["endDate"]
        else:
            self.endDate = datetime.strftime(datetime.today(), '%m/%d/%y')
        self.page = 1
        self._cursor_value = None  # Initialize cursor value

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        total_results = int(response.json()["message"]["totalResults"])
        page_size = int(response.json()["message"]["resultsPerPage"])
        current_page = int(response.json()["message"]["page"])

        if page_size * current_page < total_results:
            self.page = current_page + 1
            return self.page
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        try:
            json_response = response.json()
            for result in json_response["message"]["data"]:
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
    state_checkpoint_interval = None

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
            else:
                self.effective_start_date = self.startDate
        elif not stream_state:
            self.effective_start_date = self.startDate
        else:
            self.logger.info(f"value of page: {self.page}")
            self.logger.info(f"value of state: {stream_state}")

        params = {"page": self.page, "resultsPerPage": self.resultsPerPage, "startDate": self.effective_start_date, "endDate": self.endDate,
                  "loginId": self.loginId, "password": self.password, 'dateRangeType': 'dateUpdated'}

        self.logger.info(f"The params that were formed: {params}")
        return params

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(*args, **kwargs)
        for record in records:
            yield record
            latest_record = record.get(self.cursor_field)
            self.logger.info(f"value of latest_record: {latest_record}")
            latest_record_date = datetime.strptime(latest_record, '%Y-%m-%d %H:%M:%S')
            if self._cursor_value:
                current_state_date = datetime.strptime(self._cursor_value, '%Y-%m-%d %H:%M:%S')
                self._cursor_value = max(current_state_date, latest_record_date).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self._cursor_value = latest_record_date.strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"Trying to update state: {self._cursor_value}")
            self.logger.info(f"Full current state is: {self.state}")

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
        return None

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
