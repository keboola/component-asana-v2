import logging
import os
import datetime
import pytz
import requests
import dateparser
import pandas as pd
import json

from retry import retry
from typing import Dict
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from mapping_parser import MappingParser

# configuration variables
KEY_DEBUG = 'debug'
KEY_TOKEN = '#token'
KEY_INCREMENTAL_LOAD = 'incremental_load'
KEY_ENDPOINTS = 'endpoints'
KEY_PROJECT_ID = 'project_id'

KEY_LOAD_OPTIONS = "load_options"
KEY_DATE_FROM = "date_from"

REQUIRED_PARAMETERS = [
    KEY_ENDPOINTS,
    KEY_INCREMENTAL_LOAD,
    KEY_TOKEN
]
REQUIRED_IMAGE_PARS = []

BASE_URL = 'https://app.asana.com/api/1.0/'
REQUEST_MAP = {
    'workspaces': {
        'endpoint': 'workspaces',
        'mapping': 'workspaces'},
    'users': {
        'endpoint': 'workspaces/{workspaces_id}/users',
        'required': 'workspaces', 'mapping': 'users'},
    'users_details': {
        'endpoint': 'users/{users_id}',
        'required': 'users', 'mapping': 'users_details'},
    'user_defined_projects': {
        'endpoint': 'projects/{projects_id}',
        'required': 'projects', 'mapping': 'projects_details'},
    'projects': {
        'endpoint': 'workspaces/{workspaces_id}/projects',
        'required': 'workspaces', 'mapping': 'projects'},
    'projects_details': {
        'endpoint': 'projects/{projects_id}',
        'required': 'projects', 'mapping': 'projects_details'},
    'projects_sections': {
        'endpoint': 'projects/{projects_id}/sections',
        'required': 'projects', 'mapping': 'sections'},
    "projects_sections_tasks": {
        'endpoint': 'sections/{projects_sections_id}/tasks',
        'required': 'projects_sections', 'mapping': 'section_tasks'},
    'projects_tasks': {
        'endpoint': 'projects/{projects_id}/tasks',
        'required': 'projects', 'mapping': 'tasks'},
    'projects_tasks_details': {
        'endpoint': 'tasks/{projects_tasks_id}',
        'required': 'projects_tasks', 'mapping': 'task_details'},
    'projects_tasks_subtasks': {
        'endpoint': 'tasks/{projects_tasks_id}/subtasks',
        'required': 'projects_tasks', 'mapping': 'task_subtasks'},
    'projects_tasks_stories': {
        'endpoint': 'tasks/{projects_tasks_id}/stories',
        'required': 'projects_tasks', 'mapping': 'task_stories'}
}

ROOT_ENDPOINTS = {
    'workspaces': [],
    'users': [],
    'projects': [],
    'projects_sections': [],
    'projects_tasks': [],
    'tasks': []
}

REQUESTED_ENDPOINTS = []
REQUEST_ORDER = [
    'workspaces',
    'users',
    'users_details',
    'user_defined_projects',
    'projects',
    'archived_projects',
    'projects_sections',
    'projects_sections_tasks',
    'projects_tasks',
    'projects_tasks_details',
    'projects_tasks_subtasks',
    'projects_tasks_stories'
]

try:
    with open('src/endpoint_mappings.json', 'r') as m:
        MAPPINGS = json.load(m)
except FileNotFoundError:
    with open('../src/endpoint_mappings.json', 'r') as m:
        MAPPINGS = json.load(m)


class RetryableError(Exception):
    pass


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        params = self.configuration.parameters
        self.incremental = params.get(KEY_INCREMENTAL_LOAD)
        self.token = params.get(KEY_TOKEN)

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        params = self.configuration.parameters

        # Validate user inputs
        # & prep parameters for user_defined_projects
        self.validate_user_inputs(params)

        date_from = self.get_date_from()

        # User input parameters
        endpoints = params.get(KEY_ENDPOINTS)

        if self.incremental:
            logging.info(f"Timestamp used for incremental fetching: {date_from}")

        for r in REQUEST_ORDER:
            if r == 'workspaces' or endpoints[r]:
                self.fetch(endpoint=r, incremental=self.incremental, modified_since=date_from)

        # Always storing the last extraction date
        # if self.incremental:
        state = {'last_run': now}
        self.write_state_file(state)

        logging.info("Extraction finished")

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
            # Priortizing user_defined_projects endpoint
            REQUEST_ORDER.remove('projects')
            REQUEST_ORDER.remove('archived_projects')

            REQUESTED_ENDPOINTS.append('projects')
            self._delimit_string(params[KEY_PROJECT_ID], 'projects')

    @retry(RetryableError, tries=5, delay=1, backoff=2)
    def get_request(self, endpoint, params=None):
        """
        Generic Get request
        """

        request_url = BASE_URL + endpoint
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }

        # Pagination parameters
        if not params:
            params = {}
        params['limit'] = 100
        request_loop = True
        pagination_offset = ''

        data_out = []
        while request_loop:
            # If pagination parameter exist
            if pagination_offset:
                params['offset'] = pagination_offset

            logging.debug(f'{endpoint} Parameters: {params}')
            r = requests.get(url=request_url, headers=headers, params=params)

            if r.status_code in [401]:
                logging.error(
                    'Authorization failed. Please validate your credentials.')
                raise RetryableError(f"Retrying on exception with code {r.status_code}")
            elif r.status_code in [400, 402, 451]:
                raise UserException(f"Failed request on exception with code {r.status_code} : {r.json()}")
            elif r.status_code in [403, 404, 429, 500, 503]:
                logging.error(f'Request issue:{r.status_code} {r.json()}')
                raise RetryableError(f"Retrying on exception with code {r.status_code}")

            elif r.status_code not in [200, 201]:
                logging.error(f'Request Failed: code - {r.status_code} :{r.json()}')

                if 'errors' in r.json():
                    for err in r.json()['errors']:
                        logging.error(err['message']) if 'message' in err else ''
                raise RetryableError(f"Retrying on exception with code {r.status_code}")

            requested_data = [r.json()['data']] if type(
                r.json()['data']) == dict else r.json()['data']
            data_out = data_out + requested_data

            # Loop
            if r.json().get('next_page'):
                pagination_offset = r.json()['next_page']['offset']

            else:
                request_loop = False

        return data_out

    def fetch(self, endpoint, incremental, modified_since=None):
        """
        Processing/Fetching data
        """

        logging.info(f'Requesting {endpoint}...')

        # Prep-ing request parameters
        request_params = {}
        if endpoint == 'archived_projects':
            endpoint = 'projects'
            request_params['archived'] = True
        elif endpoint == 'projects':
            request_params['archived'] = False

        # Incremental load
        """
        if self.incremental and modified_since:
            request_params['modified_since'] = modified_since
        """

        if endpoint == "projects_tasks":
            if self.incremental and modified_since:
                request_params['completed_since'] = modified_since

        # Inputs required for the parser and requests
        required_endpoint = REQUEST_MAP[endpoint].get('required')
        endpoint_mapping = MAPPINGS[REQUEST_MAP[endpoint]['mapping']]

        # Checking if parent endpoint is required
        if required_endpoint:
            self.fetch(
                required_endpoint, incremental=self.incremental, modified_since=modified_since)\
                if required_endpoint not in REQUESTED_ENDPOINTS else ''

        # For endpoints required data from parent endpoint
        if required_endpoint:
            for i in ROOT_ENDPOINTS[required_endpoint]:
                i_id = i['gid']
                endpoint_url = REQUEST_MAP[endpoint]['endpoint']
                endpoint_url = endpoint_url.replace(
                    '{' + f'{required_endpoint}' + '_id}', i_id)

                data = self.get_request(
                    endpoint=endpoint_url, params=request_params)
                # self._output(df_json=data, filename=endpoint)
                MappingParser(
                    destination=f'{self.tables_out_path}/',
                    # endpoint=endpoint,
                    endpoint=REQUEST_MAP[endpoint]['mapping'],
                    endpoint_data=data,
                    mapping=endpoint_mapping,
                    parent_key=i_id,
                    incremental=incremental
                )

                # Saving endpoints that are parent
                if endpoint in ROOT_ENDPOINTS:
                    ROOT_ENDPOINTS[endpoint] = ROOT_ENDPOINTS[endpoint] + data

        else:
            endpoint_url = REQUEST_MAP[endpoint]['endpoint']
            data = self.get_request(endpoint=endpoint_url)

            MappingParser(
                destination=f'{self.tables_out_path}/',
                # endpoint=endpoint,
                endpoint=REQUEST_MAP[endpoint]['mapping'],
                endpoint_data=data,
                mapping=endpoint_mapping,
                incremental=incremental
            )

            # Saving endpoints that are parent
            if endpoint in ROOT_ENDPOINTS:
                ROOT_ENDPOINTS[endpoint] = ROOT_ENDPOINTS[endpoint] + data

        REQUESTED_ENDPOINTS.append(endpoint)

    @staticmethod
    def _delimit_string(id_str, endpoint):
        """
        Delimiting the list of ids and add them into the respective
        endpoint to bypass original request order
        """

        id_str = id_str.replace(' ', '')
        id_list = id_str.split(',')

        for i in id_list:
            tmp = {'gid': i}
            ROOT_ENDPOINTS[endpoint].append(tmp)

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
