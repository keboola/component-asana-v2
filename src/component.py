import asyncio
import logging
import os
import datetime
import pytz
import dateparser
import pandas as pd
from typing import Dict
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from src.asana_client.client import AsanaClient, AsanaClientException

# configuration variables
KEY_DEBUG = 'debug'
KEY_TOKEN = '#token'
KEY_INCREMENTAL_LOAD = 'incremental_load'
KEY_ENDPOINTS = 'endpoints'
KEY_PROJECT_ID = 'project_id'

KEY_LOAD_OPTIONS = "load_options"
KEY_DATE_FROM = "date_from"
KEY_SKIP_UNAUTHORIZED = "skip_unauthorized"
KEY_MAX_REQUESTS_PER_SECOND = "max_requests_per_second"
KEY_TASK_MEMBERSHIP_TIMESTAMP = "task_membership_timestamp"

REQUIRED_PARAMETERS = [
    KEY_ENDPOINTS,
    KEY_INCREMENTAL_LOAD,
    KEY_TOKEN
]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.client = None
        self.params = self.configuration.parameters
        self.date_from = self.get_date_from()
        self.now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.skip = self.params.get(KEY_SKIP_UNAUTHORIZED, False)
        self.incremental = self.params.get(KEY_INCREMENTAL_LOAD)
        self.token = self.params.get(KEY_TOKEN)

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        # Initialize the client
        self.client = AsanaClient(destination=self.tables_out_path, api_token=self.token, incremental=self.incremental,
                                  debug=self.params.get(KEY_DEBUG), skip_unauthorized=self.skip,
                                  max_requests_per_second=self.params.get('max_requests_per_second'),
                                  membership_timestamp=self.params.get(KEY_TASK_MEMBERSHIP_TIMESTAMP, False)
                                  )

        # Validate user inputs
        self.validate_user_inputs(self.params)

        # User input parameters
        endpoints_raw = self.params.get(KEY_ENDPOINTS)

        endpoints = [k for k, v in endpoints_raw.items() if v]

        if self.incremental:
            logging.info(f"Timestamp used for incremental fetching: {self.date_from}")

        try:
            asyncio.run(self.client.fetch(endpoints, completed_since=self.date_from))
        except AsanaClientException as e:
            raise UserException(f"Failed to fetch data, exception: {e}")

        # Always storing the last extraction date
        # if self.incremental:
        state = {'last_run': self.now}
        self.write_state_file(state)

        logging.info("Extraction finished")
        logging.debug(f"Requests count: {self.client.counter}")

    def get_date_from(self):
        params = self.configuration.parameters
        load_options = params.get(KEY_LOAD_OPTIONS, {})
        state = self.get_state_file()
        if date_from_raw := load_options.get(KEY_DATE_FROM):
            return self.parse_date(state, date_from_raw)
        return state.get('last_run')

    def validate_user_inputs(self, params):
        """
        Validating user inputs
        """

        # Validate if configuration is empty
        if not params:
            raise UserException('Your configurations are missing.')

        # Validate if nthe API token is missing
        if params[KEY_TOKEN] == '':
            raise UserException('Your API token is missing.')

        # Validate if any endpoints is selected
        endpoint_selected = sum(bool(params[KEY_ENDPOINTS][i]) for i in params[KEY_ENDPOINTS])

        if endpoint_selected == 0:
            raise UserException('Please select at least one endpoint to extract.')

        # Validating if project_ids are defined when
        # endpoint [user_defined_projects] is defined
        if params[KEY_ENDPOINTS]['user_defined_projects']:
            if params[KEY_PROJECT_ID] == '':
                raise UserException(
                    'Parameters are required when [Projects - User Defined] is selected. Please '
                    'define your project IDs.')

            self.client.requested_endpoints.append('projects')
            self.client.delimit_string(params[KEY_PROJECT_ID], 'projects')

    def _output(self, df_json, filename):
        output_filename = f'{self.tables_out_path}/{filename}.csv'
        data_output = pd.DataFrame(df_json, dtype=str)
        if not os.path.isfile(output_filename):
            with open(output_filename, 'a') as b:
                data_output.to_csv(b, index=False)
        else:
            with open(output_filename, 'a') as b:
                data_output.to_csv(b, index=False, header=False)

        b.close()

    @staticmethod
    def parse_date(state: Dict, date_str: str) -> str:
        if date_str.lower() in {"last", "lastrun", "last run"}:
            return state.get('last_run')
        try:
            date_obj = dateparser.parse(date_str, settings={'TIMEZONE': 'UTC'})
            if date_obj is None:
                raise ValueError("Invalid date string")
            date_obj = date_obj.replace(tzinfo=pytz.UTC)
            date_str = date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
            return date_str
        except ValueError as e:
            raise UserException(f"Parameters Error : Could not parse to date : {date_str}") from e


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
