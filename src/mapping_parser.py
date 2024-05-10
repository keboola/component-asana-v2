import os
import json
import logging  # noqa
import sys  # noqa
import pandas as pd
import time


class MappingParser():
    def __init__(self, destination, endpoint, endpoint_data, mapping, parent_key=None, incremental=False,
                 add_timestamp=False):

        self.destination = destination
        self.endpoint = endpoint
        self.endpoint_data = endpoint_data
        self.mapping = mapping
        self.parent_key = parent_key
        self.output = []
        self.primary_key = []
        self.incremental = incremental
        self.add_timestamp = add_timestamp

        # Countermeasures for response coming in as DICT
        if isinstance(self.endpoint_data, dict):
            self.endpoint_data = []
            self.endpoint_data.append(endpoint_data)

        # Parsing
        self.parse()
        if self.output:
            if self.add_timestamp:
                self.output = self._add_timestamp(df_json=self.output)
            self._output(df_json=self.output, filename=self.endpoint)
            self._produce_manifest(
                filename=self.endpoint, incremental=self.incremental, primary_key=self.primary_key)

    def parse(self):
        for row in self.endpoint_data:
            row_json = {}

            for m in self.mapping:
                col_type = self.mapping[m].get('type')

                if col_type == 'column' or not col_type:
                    key = self.mapping[m]['mapping']['destination']
                    # value = row[m]
                    value = self._fetch_value(row=row, key=m)
                    row_json[key] = value

                    # Primary key for incremental load
                    if "primaryKey" in self.mapping[m]['mapping'] and key not in self.primary_key:
                        self.primary_key.append(key)

                elif col_type == 'user':
                    key = self.mapping[m]['mapping']['destination']
                    value = self.parent_key
                    row_json[key] = value

                    # Primary key for incremental load
                    self.primary_key.append(
                        key) if key not in self.primary_key else ''

                elif col_type == 'table':
                    endpoint = self.mapping[m]['destination']
                    mapping = self.mapping[m]['tableMapping']
                    parent_key = row['gid']
                    data = self._fetch_value(row=row, key=m)
                    
                    if endpoint == 'task_details-memberships':
                        add_timestamp = True
                    else:
                        add_timestamp = False

                    MappingParser(
                        destination=self.destination,
                        endpoint=endpoint,
                        endpoint_data=data,
                        mapping=mapping,
                        parent_key=parent_key,
                        incremental=self.incremental,
                        add_timestamp=add_timestamp
                    )

            self.output.append(row_json)

    def _fetch_value(self, row, key):
        '''
        Fetching value from a nested object
        '''
        key_list = key.split('.')
        value = row

        try:
            for k in key_list:
                value = value[k]

        except Exception:
            value = ''

        return value

    def _output(self, df_json, filename):
        output_filename = f'{self.destination}/{filename}.csv'
        if df_json:
            data_output = pd.DataFrame(df_json, dtype=str)
            if not os.path.isfile(output_filename):
                with open(output_filename, 'a') as b:
                    data_output.to_csv(b, index=False)
                b.close()
            else:
                with open(output_filename, 'a') as b:
                    data_output.to_csv(b, index=False, header=False)
                b.close()

    def _produce_manifest(self, filename, incremental, primary_key):
        manifest_filename = f'{self.destination}/{filename}.csv.manifest'
        manifest = {
            'incremental': incremental,
            'primary_key': primary_key,
        }

        with open(manifest_filename, 'w') as file_out:
            json.dump(manifest, file_out)

    def _add_timestamp(self, df_json):
        current_timestamp = time.time()
        for item in df_json:
            item['timestamp'] = current_timestamp

        return df_json
