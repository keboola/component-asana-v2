import asyncio
import itertools
import json
import logging
import os

from httpx import HTTPStatusError
from keboola.http_client.async_client import AsyncHttpClient

from .mapping_parser import MappingParser

MAPPINGS_JSON = 'endpoint_mappings.json'

UNAUTHORIZED = 'unauthorized'

BASE_URL = 'https://app.asana.com/api/1.0/'

REQUEST_MAP = {
    'workspaces': {
        'level': 0,
        'endpoint': 'workspaces',
        'mapping': 'workspaces'},
    'users': {
        'level': 1,
        'endpoint': 'users?workspace={workspaces_id}',
        'required': 'workspaces',
        'mapping': 'users'},
    'users_details': {
        'level': 2,
        'endpoint': 'users/{users_id}',
        'required': 'users',
        'mapping': 'users_details'},
    'projects': {
        'level': 1,
        'endpoint': 'workspaces/{workspaces_id}/projects',
        'required': 'workspaces',
        'mapping': 'projects'},
    'projects_details': {
        'level': 2,
        'endpoint': 'projects/{projects_id}',
        'required': 'projects',
        'mapping': 'projects_details'},
    'user_defined_projects': {
        'level': 2,
        'endpoint': 'projects/{projects_id}',
        'required': 'projects',
        'mapping': 'projects_details'},
    'projects_sections': {
        'level': 3,
        'endpoint': 'projects/{projects_details_id}/sections',
        'required': 'projects_details',
        'mapping': 'sections'},
    "projects_sections_tasks": {
        'level': 4,
        'endpoint': 'sections/{projects_sections_id}/tasks',
        'required': 'projects_sections',
        'mapping': 'section_tasks'},
    'projects_tasks': {
        'level': 3,
        'endpoint': 'projects/{projects_details_id}/tasks',
        'required': 'projects_details',
        'mapping': 'tasks'},
    'projects_tasks_details': {
        'level': 4,
        'endpoint': 'tasks/{projects_tasks_id}',
        'required': 'projects_tasks',
        'mapping': 'task_details'},
    'projects_tasks_subtasks': {
        'level': 4,
        'endpoint': 'tasks/{projects_tasks_id}/subtasks',
        'required': 'projects_tasks',
        'mapping': 'task_subtasks'},
    'projects_tasks_stories': {
        'level': 4,
        'endpoint': 'tasks/{projects_tasks_id}/stories',
        'required': 'projects_tasks',
        'mapping': 'task_stories'}
}

ROOT_ENDPOINTS = {
    'workspaces': [],
    'users': [],
    'projects': [],
    'projects_details': [],
    'projects_sections': [],
    'projects_tasks': [],
    'tasks': []
}

REQUESTED_ENDPOINTS = []

DEFAULT_MAX_REQUESTS_PER_SECOND = 2.5

# The number of objects to return per page. The value must be between 1 and 100.
API_PAGE_LIMIT = 100


class AsanaClientException(Exception):
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code

    pass


class AsanaClient(AsyncHttpClient):
    def __init__(self, destination, api_token, incremental=False, debug: bool = False, skip_unauthorized: bool = False,
                 max_requests_per_second: int = DEFAULT_MAX_REQUESTS_PER_SECOND, membership_timestamp: bool = False):
        self.request_map_levels = None
        self.tables_out_path = destination
        self.incremental = incremental
        self.requested_endpoints = REQUESTED_ENDPOINTS
        self.root_endpoints = ROOT_ENDPOINTS
        self.request_map = REQUEST_MAP
        self.counter = 0
        self.skip_unauthorized = skip_unauthorized
        self.membership_timestamp = membership_timestamp
        self.endpoints_needed = set()
        self.completed_since = None
        # self.requested_endpoints = []
        super().__init__(base_url=BASE_URL,
                         auth=(api_token, ''),
                         retries=3,
                         retry_status_codes=[400, 402, 429, 500, 502, 503, 504],
                         max_requests_per_second=max_requests_per_second,
                         timeout=10,
                         debug=debug)

        json_path = os.path.join(os.path.dirname(__file__), MAPPINGS_JSON)
        with open(json_path, 'r') as m:
            self.mappings = json.load(m)

    async def fetch(self, endpoints, completed_since=None):

        self.endpoints_needed = self.get_endpoints_needed(endpoints)
        self.request_map_levels = self.get_request_map_by_level()
        self.requested_endpoints = endpoints
        self.completed_since = completed_since

        for level in self.request_map_levels:
            tasks = []
            logging.debug(f"Fetching level: {level}")
            level_endpoints = self.request_map_levels[level]
            for endpoint in level_endpoints:
                if endpoint in self.endpoints_needed:
                    tasks.append(self._fetch(endpoint, completed_since=self.completed_since,
                                             requested_endpoints=self.requested_endpoints))

            await asyncio.gather(*tasks)

    async def _fetch(self, endpoint, completed_since=None, requested_endpoints: list = None):
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
        elif endpoint == 'user_defined_projects':
            endpoint = 'projects_details'

        # Incremental load
        """
        Used for endpoint https://developers.asana.com/reference/gettasksforproject
        """
        if endpoint == "projects_tasks":
            if self.incremental and completed_since:
                request_params['completed_since'] = completed_since

        # Inputs required for the parser and requests
        required_endpoint = self.request_map[endpoint].get('required')
        # endpoint_mapping = self.mappings[self.request_map[endpoint]['mapping']]

        # For endpoints required data from parent endpoint
        if required_endpoint:

            if (endpoint == "projects_tasks_details"
                    or endpoint == "projects_tasks_subtasks"
                    or endpoint == "projects_tasks_stories"):
                logging.debug(f"Fetching {len(self.root_endpoints['projects_tasks'])} tasks.")

            tasks = []
            for i in self.root_endpoints[required_endpoint]:
                i_id = i['gid']
                endpoint_url = self.request_map[endpoint]['endpoint']
                endpoint_url = endpoint_url.replace(
                    '{' + f'{required_endpoint}' + '_id}', i_id)

                tasks.append(self._get_request(endpoint=endpoint, endpoint_url=endpoint_url, params=request_params,
                                               requested_endpoints=requested_endpoints, i_id=i_id))

            data_r = await asyncio.gather(*tasks)
            data = list(itertools.chain.from_iterable(data_r))

            if data:
                await self.save_to_requested_endpoints(data, endpoint, requested_endpoints, i_id)

                # Saving endpoints that are parent
                await self.save_data_of_parent_endpoint(data, endpoint)

        else:
            endpoint_url = self.request_map[endpoint]['endpoint']
            data = await self._get_request(endpoint=endpoint,
                                           endpoint_url=endpoint_url,
                                           requested_endpoints=requested_endpoints)

            await self.save_to_requested_endpoints(data, endpoint, requested_endpoints)

            # Saving endpoints that are parent
            await self.save_data_of_parent_endpoint(data, endpoint)

        self.requested_endpoints.append(endpoint)

    async def save_data_of_parent_endpoint(self, data, endpoint):
        if endpoint in self.root_endpoints:
            self.root_endpoints[endpoint] = self.root_endpoints[endpoint] + data

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

    def get_endpoints_needed(self, endpoints):
        endpoints_needed = set()
        for endpoint in endpoints:
            self.find_dependencies(endpoint, endpoints_needed)
        if 'user_defined_projects' in endpoints:
            endpoints_needed.remove('projects')
            endpoints_needed.remove('projects_details')
        return endpoints_needed

    def find_dependencies(self, endpoint, endpoints_needed):
        if endpoint not in endpoints_needed:
            endpoints_needed.add(endpoint)
            required = self.request_map.get(endpoint, {}).get('required')
            if required:
                self.find_dependencies(required, endpoints_needed)

    def get_request_map_by_level(self):
        levels = {}
        for endpoint, details in self.request_map.items():
            level = details.get('level', 0)
            level_key = f'level_{level}'
            levels.setdefault(level_key, []).append(endpoint)
        for level in levels:
            levels[level].sort()
        return levels

    async def _get_request(self, endpoint, endpoint_url, params=None, requested_endpoints=None, i_id=None):
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
            is_unauthorized = False
            # If pagination parameter exist
            if pagination_offset:
                params['offset'] = pagination_offset

            logging.debug(f'{endpoint_url} Parameters: {params}')

            try:
                r = await self._get(endpoint=endpoint_url, params=params)
            except AsanaClientException as e:
                if e.status_code == 403:
                    if self.skip_unauthorized:
                        logging.warning(f"Skipping unauthorized request: {e}")
                        is_unauthorized = True
                else:
                    raise AsanaClientException(e)

            try:
                if is_unauthorized:
                    await self.save_data_of_parent_endpoint([{UNAUTHORIZED: is_unauthorized}], endpoint)
                    break

                requested_data = [r['data']] if isinstance(r['data'], dict) else r['data']
                data_out = data_out + requested_data

                if len(data_out) > 1000:
                    await self.save_to_requested_endpoints(data_out, endpoint, requested_endpoints, i_id)

                    # Saving endpoints that are parent
                    await self.save_data_of_parent_endpoint(data_out, endpoint)
                    data_out = []

            except KeyError:
                logging.warning(f"Failed to parse data from response: {r.json()}")

            # Loop
            if r.get('next_page'):
                pagination_offset = r['next_page']['offset']
            else:
                params.pop("offset", None)
                break

        return data_out

    async def save_to_requested_endpoints(self, data_out, endpoint, requested_endpoints, i_id=None):
        if endpoint in requested_endpoints:
            MappingParser(
                destination=f'{self.tables_out_path}/',
                endpoint=self.request_map[endpoint]['mapping'],
                endpoint_data=data_out,
                mapping=self.mappings[self.request_map[endpoint]['mapping']],
                parent_key=i_id,
                incremental=self.incremental,
                add_timestamp=self.membership_timestamp
            )

    async def _get(self, endpoint: str, params=None) -> dict:
        self.counter += 1

        if params is None:
            params = {}

        try:
            r = await self.get_raw(endpoint, params=params)
            r.raise_for_status()
        except HTTPStatusError as e:
            raise AsanaClientException(f"Cannot fetch resource: {endpoint}, exception: {e}",
                                       status_code=e.response.status_code) from e

        try:
            return r.json()
        except json.decoder.JSONDecodeError as e:
            raise AsanaClientException(f"Cannot parse response for {endpoint}, exception: {e}") from e
