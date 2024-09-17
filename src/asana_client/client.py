import asyncio
import itertools
import json
import logging
import os

from httpx import HTTPStatusError
from keboola.http_client.async_client import AsyncHttpClient
import tracemalloc


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

DEFAULT_MAX_REQUESTS_PER_SECOND = 2.5

# The number of objects to return per page. The value must be between 1 and 100.
API_PAGE_LIMIT = 10


class AsanaClientException(Exception):
    pass


class AsanaClient(AsyncHttpClient):
    def __init__(self, temp_dir, api_token, incremental=False, debug: bool = False, skip_unauthorized: bool = False,
                 max_requests_per_second: int = DEFAULT_MAX_REQUESTS_PER_SECOND, membership_timestamp: bool = False,
                 ):
        self.temp_dir = temp_dir
        self.incremental = incremental
        self.requested_endpoints = REQUESTED_ENDPOINTS
        self.root_endpoints = ROOT_ENDPOINTS
        self.request_map = REQUEST_MAP
        self.counter = 0
        self.skip_unauthorized = skip_unauthorized
        self.membership_timestamp = membership_timestamp
        self.tracemalloc = tracemalloc.start()
        super().__init__(base_url=BASE_URL,
                         auth=(api_token, ''),
                         retries=5,
                         retry_status_codes=[402, 429, 500, 502, 503, 504],
                         max_requests_per_second=max_requests_per_second,
                         timeout=10,
                         debug=debug)
        with open('./src/asana_client/endpoint_mappings.json', 'r') as m:
            self.mappings = json.load(m)

        return

    async def fetch(self, endpoints, completed_since=None):

        endpoints_needed = self.get_endpoints_needed(endpoints)

        await self._fetch(endpoint="workspaces", completed_since=completed_since, requested_endpoints=endpoints)

        tasks = []
        for r in ['users', 'projects']:
            if r in endpoints_needed:
                tasks.append(self._fetch(r, completed_since=completed_since, requested_endpoints=endpoints))
        await asyncio.gather(*tasks)

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        out = ""
        for stat in top_stats[:10]:
            out += f"{stat}\n"
        logging.debug(f"1st stage: \n {out}")

        tasks = []
        for r in ['users_details', 'user_defined_projects', 'archived_projects', 'projects_sections', 'projects_tasks']:
            if r in endpoints_needed:
                tasks.append(self._fetch(r, completed_since=completed_since, requested_endpoints=endpoints))
        await asyncio.gather(*tasks)

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        out = ""
        for stat in top_stats[:10]:
            out += f"{stat}\n"
        logging.debug(f"2nd stage: \n {out}")

        tasks = []
        for r in ['projects_sections_tasks', 'projects_tasks_details',
                  'projects_tasks_subtasks', 'projects_tasks_stories']:
            if r in endpoints_needed:
                tasks.append(self._fetch(r, completed_since=completed_since, requested_endpoints=endpoints))
        await asyncio.gather(*tasks)

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        out = ""
        for stat in top_stats[:10]:
            out += f"{stat}\n"
        logging.debug(f"3rd stage: \n {out}")

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

        # Incremental load
        """
        Used for endpoint https://developers.asana.com/reference/gettasksforproject
        """
        if endpoint == "projects_tasks":
            if self.incremental and completed_since:
                request_params['completed_since'] = completed_since

        # Inputs required for the parser and requests
        required_endpoint = self.request_map[endpoint].get('required')

        # For endpoints required data from parent endpoint
        if required_endpoint:

            if endpoint == "projects_tasks_details":
                logging.debug(f"Fetching {len(self.root_endpoints['projects_tasks'])} tasks.")

            tasks = []
            for i in self.root_endpoints[required_endpoint]:
                i_id = i['gid']
                endpoint_url = self.request_map[endpoint]['endpoint']
                endpoint_url = endpoint_url.replace(
                    '{' + f'{required_endpoint}' + '_id}', i_id)

                tasks.append(self._get_request(endpoint=endpoint_url, params=request_params))

            data_r = await asyncio.gather(*tasks)
            data = list(itertools.chain.from_iterable(data_r))

            if data:
                if endpoint in requested_endpoints:
                    self.save_to_temp_file(endpoint, data, i_id)

                # Saving endpoints that are parent
                if endpoint in self.root_endpoints:
                    self.root_endpoints[endpoint] = self.root_endpoints[endpoint] + data

        else:
            endpoint_url = self.request_map[endpoint]['endpoint']
            data = await self._get_request(endpoint=endpoint_url)

            if endpoint in requested_endpoints:
                self.save_to_temp_file(endpoint, data)

            # Saving endpoints that are parent
            if endpoint in self.root_endpoints:
                self.root_endpoints[endpoint] = self.root_endpoints[endpoint] + data

        self.requested_endpoints.append(endpoint)
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        out = ""
        for stat in top_stats[:10]:
            out += f"{stat}\n"
        logging.debug(f"Top stats for {endpoint}: \n {out}")

    def save_to_temp_file(self, endpoint, data, parent_key=None):
        path = os.path.join(self.temp_dir, endpoint)
        if not os.path.exists(path):
            os.makedirs(path)
        file_path = os.path.join(path, f"---{parent_key}---{self.counter}.json")
        with open(file_path, 'w') as temp_file:
            json.dump(data, temp_file)
        logging.info(f"Saved data for {endpoint} to {file_path}")

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
        return endpoints_needed

    def find_dependencies(self, endpoint, endpoints_needed):
        if endpoint not in endpoints_needed:
            endpoints_needed.add(endpoint)
            required = self.request_map.get(endpoint, {}).get('required')
            if required:
                self.find_dependencies(required, endpoints_needed)

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

            try:
                r = await self._get(endpoint=endpoint, params=params)
            except AsanaClientException as e:
                if self.skip_unauthorized:
                    logging.warning(f"Skipping unauthorized request: {e}")
                    break
                else:
                    raise AsanaClientException(e)

            try:
                requested_data = [r['data']] if isinstance(r['data'], dict) else r['data']
                data_out = data_out + requested_data

                if len(data_out) > 10:
                    self.save_to_temp_file(endpoint.split('/')[-1].split("?")[0], data_out)
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

    async def _get(self, endpoint: str, params=None) -> dict:
        self.counter += 1

        if params is None:
            params = {}

        try:
            r = await self.get_raw(endpoint, params=params)
            r.raise_for_status()
        except HTTPStatusError as e:
            raise AsanaClientException(f"Cannot fetch resource: {endpoint}, exception: {e}")

        try:
            return r.json()
        except json.decoder.JSONDecodeError as e:
            raise AsanaClientException(f"Cannot parse response for {endpoint}, exception: {e}") from e
