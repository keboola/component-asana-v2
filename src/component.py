'''
Template Component main class.

'''

import logging
import sys
import os  # noqa
import datetime  # noqa
import requests
import pandas as pd
import json

from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef  # noqa
from kbc.result import ResultWriter  # noqa
from mapping_parser import MappingParser


# configuration variables
KEY_DEBUG = 'debug'
KEY_TOKEN = '#token'
KEY_INCREMENTAL_LOAD = 'incremental_load'
KEY_ENDPOINTS = 'endpoints'

MANDATORY_PARS = [
    KEY_ENDPOINTS,
    KEY_INCREMENTAL_LOAD,
    KEY_TOKEN
]
MANDATORY_IMAGE_PARS = []

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
    'projects': {
        'endpoint': 'workspaces/{workspaces_id}/projects',
        'required': 'workspaces', 'mapping': 'projects'},
    'projects_details': {
        'endpoint': 'projects/{projects_id}',
        'required': 'projects', 'mapping': 'projects_details'},
    'projects_sections': {
        'endpoint': 'projects/{projects_id}/sections',
        'required': 'projects', 'mapping': 'sections'},
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
    'projects_tasks': []
}

REQUESTED_ENDPOINTS = []
REQUEST_ORDER = [
    'workspaces',
    'users',
    'users_details',
    'projects',
    'archived_projects',
    'projects_sections',
    'projects_tasks',
    'projects_tasks_details',
    'projects_tasks_subtasks',
    'projects_tasks_stories'
]

with open('src/endpoint_mappings.json', 'r') as m:
    MAPPINGS = json.load(m)

APP_VERSION = '0.0.3'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        # Disabling list of libraries you want to output in the logger
        disable_libraries = []
        for library in disable_libraries:
            logging.getLogger(library).disabled = True

        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True

        log_level = logging.DEBUG if debug else logging.INFO
        # setup GELF if available
        if os.getenv('KBC_LOGGER_ADDR', None):
            self.set_gelf_logger(log_level)
        else:
            self.set_default_logger(log_level)

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

    def run(self):
        '''
        Main execution code
        '''

        params = self.cfg_params  # noqa
        self.token = params.get(KEY_TOKEN)
        self.incremental = params.get(KEY_INCREMENTAL_LOAD)
        state = self.get_state_file()
        if self.incremental and not state:
            state = {}
        # Last run date
        try:
            self.last_run = state['component']['last_run']
        except Exception:
            self.last_run = None

        # Validate user inputs
        self.validate_user_inputs(params)

        # User input parameters
        endpoints = params.get(KEY_ENDPOINTS)
        now = datetime.datetime.now().strftime('%Y-%m-%d')

        for r in REQUEST_ORDER:
            if r == 'workspaces' or endpoints[r]:
                self.fetch(endpoint=r)

        if self.incremental:
            state['component'] = {}
            state['component']['last_run'] = now
            self.write_state_file(state)

        logging.info("Extraction finished")

    def validate_user_inputs(self, params):
        '''
        Validating user inputs
        '''

        # Validate if configuration is empty
        if not params:
            logging.error('Your configurations are missing.')
            sys.exit(1)

        # Validate if nthe API token is missing
        if params[KEY_TOKEN] == '':
            logging.error('Your API token is missing.')
            sys.exit(1)

        # Validate if any endpoints is selected
        endpoint_selected = 0
        for i in params[KEY_ENDPOINTS]:
            if params[KEY_ENDPOINTS][i]:
                endpoint_selected += 1

        if endpoint_selected == 0:
            logging.error('Please select at least one endpoint to extract.')
            sys.exit(1)

    def get_request(self, endpoint, params=None):
        '''
        Generic Get request
        '''

        request_url = BASE_URL+endpoint
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
                sys.exit(1)
            elif r.status_code in [429]:
                logging.error(f'Request issue: {r.json()}')
                logging.error('Please contact support')
                sys.exit(1)
            elif r.status_code not in [200, 201]:
                logging.error(f'Request Failed: {r.json()}')
                sys.exit(1)

            requested_data = [r.json()['data']] if type(
                r.json()['data']) == dict else r.json()['data']
            data_out = data_out + requested_data

            # Loop
            if 'next_page' in r.json():
                if r.json()['next_page']:
                    pagination_offset = r.json()['next_page']['offset']
                else:
                    request_loop = False
            else:
                request_loop = False

        return data_out

    def fetch(self, endpoint):
        '''
        Processing/Fetching data
        '''

        logging.info(f'Requesting [{endpoint}]...')

        request_params = {}
        if endpoint == 'archived_projects':
            endpoint = 'projects'
            request_params['archived'] = True
        elif endpoint == 'projects':
            request_params['archived'] = False

        # Incremental load
        if self.incremental and self.last_run:
            request_params['modified_since'] = self.last_run

        required_endpoint = REQUEST_MAP[endpoint].get('required')
        endpoint_mapping = MAPPINGS[REQUEST_MAP[endpoint]['mapping']]

        # Checking if parent endpoint is required
        if required_endpoint:
            self.fetch(
                required_endpoint) if required_endpoint not in REQUESTED_ENDPOINTS else ''

        # For endpoints required data from parent endpoint
        if required_endpoint:
            for i in ROOT_ENDPOINTS[required_endpoint]:
                i_id = i['gid']
                endpoint_url = REQUEST_MAP[endpoint]['endpoint']
                endpoint_url = endpoint_url.replace(
                    '{'+f'{required_endpoint}'+'_id}', i_id)

                data = self.get_request(
                    endpoint=endpoint_url, params=request_params)
                # self._output(df_json=data, filename=endpoint)
                MappingParser(
                    destination=f'{self.tables_out_path}/',
                    # endpoint=endpoint,
                    endpoint=REQUEST_MAP[endpoint]['mapping'],
                    endpoint_data=data,
                    mapping=endpoint_mapping,
                    parent_key=i_id
                )

                # Saving endpoints that are parent
                if endpoint in ROOT_ENDPOINTS:
                    ROOT_ENDPOINTS[endpoint] = ROOT_ENDPOINTS[endpoint] + data
        else:
            endpoint_url = REQUEST_MAP[endpoint]['endpoint']
            data = self.get_request(endpoint=endpoint_url)
            # self._output(df_json=data, filename=endpoint)
            MappingParser(
                destination=f'{self.tables_out_path}/',
                # endpoint=endpoint,
                endpoint=REQUEST_MAP[endpoint]['mapping'],
                endpoint_data=data,
                mapping=endpoint_mapping
            )

            # Saving endpoints that are parent
            if endpoint in ROOT_ENDPOINTS:
                ROOT_ENDPOINTS[endpoint] = ROOT_ENDPOINTS[endpoint] + data
        REQUESTED_ENDPOINTS.append(endpoint)

    def _output(self, df_json, filename):
        output_filename = f'{self.tables_out_path}/{filename}.csv'
        data_output = pd.DataFrame(df_json, dtype=str)
        if not os.path.isfile(output_filename):
            with open(output_filename, 'a') as b:
                data_output.to_csv(b, index=False)
            b.close()
        else:
            with open(output_filename, 'a') as b:
                data_output.to_csv(b, index=False, header=False)
            b.close()


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = True
    comp = Component(debug)
    comp.run()
