import json
import logging

from httpx import HTTPStatusError
from keboola.http_client.async_client import AsyncHttpClient

from src.asana_client.mapping_parser import MappingParser

BASE_URL = 'https://app.asana.com/api/1.0/'

REQUEST_MAP = {
    'workspaces': {
        'endpoint': 'workspaces',
        'mapping': 'workspaces'},
    'users': {
        'endpoint': 'users?workspace={workspaces_id}',
        'required': 'workspaces',
        'mapping': 'users'},
    'users_details': {
        'endpoint': 'users/{users_id}',
        'required': 'users',
        'mapping': 'users_details'},
    'user_defined_projects': {
        'endpoint': 'projects/{projects_id}',
        'required': 'projects',
        'mapping': 'projects_details'},
    'projects': {
        'endpoint': 'workspaces/{workspaces_id}/projects',
        'required': 'workspaces',
        'mapping': 'projects'},
    'projects_details': {
        'endpoint': 'projects/{projects_id}',
        'required': 'projects',
        'mapping': 'projects_details'},
    'projects_sections': {
        'endpoint': 'projects/{projects_id}/sections',
        'required': 'projects',
        'mapping': 'sections'},
    "projects_sections_tasks": {
        'endpoint': 'sections/{projects_sections_id}/tasks',
        'required': 'projects_sections',
        'mapping': 'section_tasks'},
    'projects_tasks': {
        'endpoint': 'projects/{projects_id}/tasks',
        'required': 'projects',
        'mapping': 'tasks'},
    'projects_tasks_details': {
        'endpoint': 'tasks/{projects_tasks_id}',
        'required': 'projects_tasks',
        'mapping': 'task_details'},
    'projects_tasks_subtasks': {
        'endpoint': 'tasks/{projects_tasks_id}/subtasks',
        'required': 'projects_tasks',
        'mapping': 'task_subtasks'},
    'projects_tasks_stories': {
        'endpoint': 'tasks/{projects_tasks_id}/stories',
        'required': 'projects_tasks',
        'mapping': 'task_stories'}
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

# TODO add option to change it by user licence
MAX_REQUESTS_PER_SECOND = 4

# The number of objects to return per page. The value must be between 1 and 100.
API_PAGE_LIMIT = 100


class AsanaClientException(Exception):
    pass


class AsanaClient(AsyncHttpClient):
    def __init__(self, destination, api_token, incremental=False, debug: bool = False):
        self.tables_out_path = destination
        self.incremental = incremental
        self.request_order = REQUEST_ORDER
        self.requested_endpoints = REQUESTED_ENDPOINTS
        self.root_endpoints = ROOT_ENDPOINTS
        self.request_map = REQUEST_MAP
        super().__init__(base_url=BASE_URL,
                         auth=(api_token, ''),
                         retries=5,
                         retry_status_codes=[402, 429, 500, 502, 503, 504],
                         max_requests_per_second=MAX_REQUESTS_PER_SECOND,
                         timeout=10,
                         debug=debug)
        with open('./asana_client/endpoint_mappings.json', 'r') as m:
            self.mappings = json.load(m)

    async def fetch(self, endpoints, completed_since=None):
        for r in self.request_order:
            if r == 'workspaces' or endpoints[r]:
                await self._fetch(endpoint=r, completed_since=completed_since)

    async def _fetch(self, endpoint, completed_since=None):
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
        Used for endpoint https://developers.asana.com/reference/gettasksforproject
        """
        if endpoint == "projects_tasks":
            if self.incremental and completed_since:
                request_params['completed_since'] = completed_since

        # Inputs required for the parser and requests
        required_endpoint = self.request_map[endpoint].get('required')
        endpoint_mapping = self.mappings[self.request_map[endpoint]['mapping']]

        # Checking if parent endpoint is required
        if required_endpoint:
            await self._fetch(
                required_endpoint, completed_since=completed_since) \
                if required_endpoint not in self.requested_endpoints else ''

        # For endpoints required data from parent endpoint
        if required_endpoint:

            if endpoint == "projects_tasks_details":
                logging.debug(f"Fetching {len(self.root_endpoints['projects_tasks'])} tasks.")

            for i in self.root_endpoints[required_endpoint]:
                i_id = i['gid']
                endpoint_url = self.request_map[endpoint]['endpoint']
                endpoint_url = endpoint_url.replace(
                    '{' + f'{required_endpoint}' + '_id}', i_id)

                data = await self._get_request(endpoint=endpoint_url, params=request_params)

                if data:
                    MappingParser(
                        destination=f'{self.tables_out_path}/',
                        # endpoint=endpoint,
                        endpoint=self.request_map[endpoint]['mapping'],
                        endpoint_data=data,
                        mapping=endpoint_mapping,
                        parent_key=i_id,
                        incremental=self.incremental
                    )

                    # Saving endpoints that are parent
                    if endpoint in self.root_endpoints:
                        self.root_endpoints[endpoint] = self.root_endpoints[endpoint] + data

        else:
            endpoint_url = self.request_map[endpoint]['endpoint']
            data = await self._get_request(endpoint=endpoint_url)

            MappingParser(
                destination=f'{self.tables_out_path}/',
                # endpoint=endpoint,
                endpoint=self.request_map[endpoint]['mapping'],
                endpoint_data=data,
                mapping=endpoint_mapping,
                incremental=self.incremental
            )

            # Saving endpoints that are parent
            if endpoint in self.root_endpoints:
                self.root_endpoints[endpoint] = self.root_endpoints[endpoint] + data

        self.requested_endpoints.append(endpoint)

    def delimit_string(self, id_str, endpoint):
        """
        Delimiting the list of ids and add them into the respective
        endpoint to bypass original request order
        """

        id_str = id_str.replace(' ', '')
        id_list = id_str.split(',')

        for i in id_list:
            tmp = {'gid': i}
            self.root_endpoints[endpoint].append(tmp)

    async def _get_request(self, endpoint, params=None):
        """
        Generic Get request
        """

        # Pagination parameters
        if not params:
            params = {}
        params['limit'] = API_PAGE_LIMIT
        pagination_offset = None

        data_out = []
        while True:
            # If pagination parameter exist
            if pagination_offset:
                params['offset'] = pagination_offset

            logging.debug(f'{endpoint} Parameters: {params}')
            r = await self._get(endpoint=endpoint, params=params)
            data = r.get('data', {})
            data_out.extend(data)

            # Loop
            if r.get('next_page'):
                pagination_offset = r['next_page']['offset']
            else:
                params.pop("offset", None)
                break

        return data_out

    async def _get(self, endpoint: str, params=None) -> dict:
        if params is None:
            params = {}

        r = await self.get_raw(endpoint, params=params)

        try:
            r.raise_for_status()
        except HTTPStatusError:
            raise AsanaClientException(f"Cannot fetch resource: {endpoint}")

        try:
            return r.json()
        except json.decoder.JSONDecodeError as e:
            raise AsanaClientException(f"Cannot parse response for {endpoint}, exception: {e}") from e
