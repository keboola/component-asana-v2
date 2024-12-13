import asyncio
import json
import logging
import os
import time
import random

from httpx import HTTPStatusError
from keboola.http_client.async_client import AsyncHttpClient

from .mapping_parser import MappingParser

MAPPINGS_JSON = 'endpoint_mappings.json'

BASE_URL = 'https://app.asana.com/api/1.0/'

REQUEST_MAP = {
    'workspaces': {
        'level': 0,
        'endpoint': 'workspaces',
        'mapping': 'workspaces'},
    'users': {
        'level': 1,
        'endpoint_batch': '/workspaces/{workspaces_id}/users',
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

DEFAULT_MAX_REQUESTS_PER_SECOND = 2
DEFAULT_BATCH_SIZE = 100

# The number of objects to return per page. The value must be between 1 and 100.
API_PAGE_LIMIT = 100

KEY_FORBIDDEN_ENDPOINTS = 'forbidden_endpoints'
KEY_GID = 'gid'
KEY_GEN_ID = 'gen_id'
TMP_FOLDER_PATH = '/tmp'


class AsanaClientException(Exception):
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code

    pass


class AsanaClient(AsyncHttpClient):
    def __init__(self, destination, api_token, incremental=False, debug: bool = False, skip_unauthorized: bool = False,
                 max_requests_per_second: int = DEFAULT_MAX_REQUESTS_PER_SECOND, membership_timestamp: bool = False,
                 batch_size: int = DEFAULT_BATCH_SIZE):
        self.request_map_levels = None
        self.tables_out_path = destination
        self.incremental = incremental
        self.requested_endpoints = []
        self.root_endpoints_data = {rm_endpoint: [] for rm_endpoint in REQUEST_MAP}
        self.request_map = REQUEST_MAP
        self.counter = 0
        self.skip_unauthorized = skip_unauthorized
        self.membership_timestamp = membership_timestamp
        self.endpoints_needed = set()
        self.completed_since = None
        self.batch_size = batch_size
        super().__init__(base_url=BASE_URL,
                         auth=(api_token, ''),
                         retries=3,
                         retry_status_codes=[400, 402, 429, 500, 502, 503, 504],
                         max_requests_per_second=max_requests_per_second,
                         timeout=10,
                         debug=debug)

        self._init_mappings()
        self._init_tmp_folders()

    def _init_mappings(self):
        json_path = os.path.join(os.path.dirname(__file__), MAPPINGS_JSON)
        with open(json_path, 'r') as m:
            self.mappings = json.load(m)

    async def fetch(self, endpoints, completed_since=None):

        self.endpoints_needed = self.get_endpoints_needed(endpoints)
        self.request_map_levels = self.construct_request_map_with_levels()
        self.requested_endpoints = endpoints
        self.completed_since = completed_since

        for level in self.request_map_levels:
            tasks = []
            logging.debug(f"Fetching level: {level}")
            level_endpoints = self.request_map_levels[level]
            for level_endpoint in level_endpoints:
                if level_endpoint in self.endpoints_needed:
                    tasks.append(self._fetch(level_endpoint, completed_since=self.completed_since))

            await asyncio.gather(*tasks)

    async def _fetch(self, fetched_endpoint, completed_since=None):
        """
        Processing/Fetching data
        """

        logging.info(f'Requesting {fetched_endpoint}...')

        # Prep-ing request parameters
        request_params = {}
        if fetched_endpoint == 'archived_projects':
            fetched_endpoint = 'projects'
            request_params['archived'] = True
        elif fetched_endpoint == 'projects':
            request_params['archived'] = False
        elif fetched_endpoint == 'user_defined_projects':
            fetched_endpoint = 'projects_details'

        # Incremental load
        # Used for endpoint https://developers.asana.com/reference/gettasksforproject
        if fetched_endpoint == "projects_tasks":
            if self.incremental and completed_since:
                request_params['completed_since'] = completed_since

        # Inputs required for the parser and requests
        required_endpoint_data = self.request_map[fetched_endpoint].get('required')

        # For endpoints required data from parent endpoint
        if required_endpoint_data:
            await self._get_multiple_batched(fetched_endpoint, request_params, required_endpoint_data)

        else:
            endpoint_url = self.request_map[fetched_endpoint]['endpoint']
            await self._get_request(endpoint_url=endpoint_url, endpoint_id=await self._generate_root_id(),
                                    endpoint=fetched_endpoint)
        await self._parse_endpoint_data_from_tmp(fetched_endpoint)

    def _generate_batch(self, data):
        for i in range(0, len(data), self.batch_size):
            yield data[i:i + self.batch_size]

    async def _get_multiple_batched(self, fetched_endpoint, request_params, required_endpoint_data):

        # Some endpoint can be forbidden for some parent endpoints type, name etc.
        without_forbidden_endpoints = [endpoint for endpoint in self.root_endpoints_data[required_endpoint_data] if
                                       fetched_endpoint not in endpoint.get(KEY_FORBIDDEN_ENDPOINTS, [])]

        for batch_parent_endpoint_data in self._generate_batch(without_forbidden_endpoints):
            tasks = []
            for parent_endpoint_data in batch_parent_endpoint_data:
                parent_id = parent_endpoint_data[KEY_GID]
                endpoint_url = self.request_map[fetched_endpoint]['endpoint']
                endpoint_url = endpoint_url.replace('{' + f'{required_endpoint_data}' + '_id}', parent_id)

                tasks.append(self._get_request(endpoint_url=endpoint_url, params=request_params, endpoint_id=parent_id,
                                               endpoint=fetched_endpoint))
            await asyncio.gather(*tasks)

    @staticmethod
    async def _generate_root_id():
        gen_index = f"{KEY_GEN_ID}_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        return gen_index

    def _init_tmp_folders(self):
        # create file if not exist
        for endpoint in self.root_endpoints_data:
            file_path = self._construct_tmp_folder_name(endpoint)
            if not os.path.exists(file_path):
                os.makedirs(file_path, exist_ok=True)
            else:
                for file in os.listdir(file_path):
                    os.remove(os.path.join(file_path, file))

    @staticmethod
    def _construct_tmp_folder_name(endpoint):
        file_path = f'{TMP_FOLDER_PATH}/{endpoint}'
        return file_path

    def _write_endpoint_data_to_tmp(self, data, endpoint, file_index=None):
        file_path = self._construct_tmp_folder_name(endpoint)
        with open(f'{file_path}/{file_index}.json', 'w') as f:
            json.dump(data, f)

    async def _parse_endpoint_data_from_tmp(self, endpoint):
        # read every file in the endpoint folder
        file_counter = 0
        data_counter = 0
        for file in os.listdir(self._construct_tmp_folder_name(endpoint)):
            file_counter += 1
            with open(f'{self._construct_tmp_folder_name(endpoint)}/{file}', 'r+') as f:
                file_data = json.load(f)
                self._save_parent_endpoint_data(file_data, endpoint)
                file_name = file.split('.')[0]
                data_counter += len(file_data)
                await self._mapping_endpoint_data_to_output(file_data, endpoint, i_id=file_name)

        logging.debug(f"Parsed data count: {data_counter} from tmp files({file_counter}), endpoint: {endpoint}")

    async def _mapping_endpoint_data_to_output(self, data_out, endpoint, i_id=None):
        MappingParser(
            destination=f'{self.tables_out_path}',
            endpoint=self.request_map[endpoint]['mapping'],
            endpoint_data=data_out,
            mapping=self.mappings[self.request_map[endpoint]['mapping']],
            parent_key=i_id,
            incremental=self.incremental,
            add_timestamp=self.membership_timestamp
        )

    def _save_parent_endpoint_data(self, data, endpoint):
        for i in data:
            data_to_save = self._check_endpoint_rules(endpoint, i)
            self.root_endpoints_data[endpoint].append(data_to_save)

    @staticmethod
    def _check_endpoint_rules(endpoint, data):
        data_to_save = {KEY_GID: data[KEY_GID]}

        if endpoint == 'workspaces':
            if data['name'] == 'Personal Projects':
                logging.info(f"Skipping endpoint users for personal workspaces is not allowed: {data['gid']}")
                data_to_save = {KEY_GID: data[KEY_GID], KEY_FORBIDDEN_ENDPOINTS: ['users']}

        return data_to_save

    def add_parent_endpoint_manually(self, id_str, endpoint):
        """
        Delimiting the list of ids and add them into the respective
        endpoint to bypass original request order
        """
        id_str = id_str.replace(' ', '')
        id_list = id_str.split(',')

        for i in id_list:
            tmp = {KEY_GID: i}
            self.root_endpoints_data[endpoint].append(tmp)

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

    def construct_request_map_with_levels(self):
        levels = {}
        for endpoint, details in self.request_map.items():
            level = details.get('level', 0)
            level_key = f'level_{level}'
            levels.setdefault(level_key, []).append(endpoint)
        for level in levels:
            levels[level].sort()
        return levels

    async def _get_request(self, endpoint_url, endpoint, endpoint_id, params=None):
        """
        Generic Get request
        """
        # Pagination parameters
        if not params:
            params = {}
        params['limit'] = API_PAGE_LIMIT
        pagination_offset = None

        data = []
        while True:
            # If pagination parameter exist
            if pagination_offset:
                params['offset'] = pagination_offset

            r: dict = {}
            try:
                r = await self._get(endpoint=endpoint_url, params=params)
            except AsanaClientException as e:
                if e.status_code == 403:
                    if self.skip_unauthorized:
                        logging.warning(f"Skipping unauthorized request: {e}")
                        break
                raise AsanaClientException(e)

            try:
                data.extend([r['data']] if isinstance(r['data'], dict) else r['data'])
            except KeyError:
                logging.warning(f"Failed to parse data from response: {r}")

            # Loop
            if r.get('next_page'):
                pagination_offset = r['next_page']['offset']
            else:
                params.pop("offset", None)
                break

        self._write_endpoint_data_to_tmp(data, endpoint, endpoint_id)

    async def _get(self, endpoint: str, params=None) -> dict:
        self.counter += 1

        if params is None:
            params = {}

        try:
            logging.debug(f'{endpoint} Parameters: {params}')
            r = await self.get_raw(endpoint, params=params)
            r.raise_for_status()
        except HTTPStatusError as e:
            raise AsanaClientException(f"Cannot fetch resource: {endpoint}, exception: {e}",
                                       status_code=e.response.status_code) from e

        try:
            return r.json()
        except json.decoder.JSONDecodeError as e:
            raise AsanaClientException(f"Cannot parse response for {endpoint}, exception: {e}") from e
