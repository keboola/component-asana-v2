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
KEY_ENDPOINTS = 'endpoints'

MANDATORY_PARS = [
    KEY_ENDPOINTS,
    KEY_TOKEN
]
MANDATORY_IMAGE_PARS = []

# Logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)-8s : [line:%(lineno)3s] %(message)s',
#     datefmt="%Y-%m-%d %H:%M:%S")

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
    'projects_sections',
    'projects_tasks',
    'projects_tasks_details',
    'projects_tasks_subtasks',
    'projects_tasks_stories'
]

with open('src/endpoint_mappings.json', 'r') as m:
    MAPPINGS = json.load(m)

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        # Disabling list of libraries you want to output in the logger
        disable_libraries = []
        for library in disable_libraries:
            logging.getLogger(library).disabled = True

        if self.cfg_params.get(KEY_DEBUG, False) is True:
            logger = logging.getLogger()
            logger.setLevel(level='DEBUG')

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
        endpoints = params.get(KEY_ENDPOINTS)

        for r in REQUEST_ORDER:
            if r == 'workspaces' or endpoints[r]:
                self.fetch(endpoint=r)

        logging.info("Extraction finished")

    def get_request(self, endpoint, params=None):
        '''
        Generic Get request
        '''

        request_url = BASE_URL+endpoint
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }

        r = requests.get(url=request_url, headers=headers, params=params)

        if r.status_code not in [200, 201]:
            logging.error(f'Request issue: {r.json()}')
            sys.exit(1)

        return r.json()['data']

    def fetch(self, endpoint):
        '''
        Processing/Fetching data
        '''

        required_endpoint = REQUEST_MAP[endpoint].get('required')
        endpoint_mapping = MAPPINGS[REQUEST_MAP[endpoint]['mapping']]
        # Checking if parent endpoint is required
        if required_endpoint:
            self.fetch(
                required_endpoint) if required_endpoint not in REQUESTED_ENDPOINTS else ''

        logging.info(f'Requesting [{endpoint}]...')
        # For endpoints required data from parent endpoint
        if required_endpoint:
            for i in ROOT_ENDPOINTS[required_endpoint]:
                i_id = i['gid']
                endpoint_url = REQUEST_MAP[endpoint]['endpoint']
                endpoint_url = endpoint_url.replace(
                    '{'+f'{required_endpoint}'+'_id}', i_id)
                data = self.get_request(endpoint=endpoint_url)
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
