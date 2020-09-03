import os
import json
import logging  # noqa
import sys  # noqa
import pandas as pd


class MappingParser():
    def __init__(self, destination, endpoint, endpoint_data, mapping, parent_key=None, incremental=False):
        self.destination = destination
        self.endpoint = endpoint
        self.endpoint_data = endpoint_data
        self.mapping = mapping
        self.parent_key = parent_key
        self.output = []
        self.primary_key = []

        # Countermeasures for response coming in as DICT
        if type(self.endpoint_data) == dict:
            self.endpoint_data = []
            self.endpoint_data.append(endpoint_data)

        # Parsing
        self.parse()
        if self.output:
            self._output(df_json=self.output, filename=self.endpoint)
            self._product_manifest(
                filename=self.endpoint, incremental=incremental, primary_key=self.primary_key)

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
                    if "primaryKey" in self.mapping[m]['mapping']:
                        self.primary_key.append(key)

                    # if self.endpoint == 'task_details-memberships':
                    #     print('ROW: {}'.format(row))
                    #     print('m: {}'.format(m))
                    #     print('key: {}'.format(key))
                    #     print('value:{}'.format(value))
                    #     print(row['project']['gid'])
                    #     sys.exit(0)

                elif col_type == 'user':
                    key = self.mapping[m]['mapping']['destination']
                    value = self.parent_key
                    row_json[key] = value

                    # Primary key for incremental load
                    self.primary_key.append(key)

                elif col_type == 'table':
                    endpoint = self.mapping[m]['destination']
                    mapping = self.mapping[m]['tableMapping']
                    parent_key = row['gid']
                    data = self._fetch_value(row=row, key=m)
                    # if m == 'memberships':
                    #     print(row[m])
                    #     print(parent_key)
                    #     print(mapping)
                    #     print(data)
                    #     # sys.exit(0)

                    MappingParser(
                        destination=self.destination,
                        endpoint=endpoint,
                        endpoint_data=data,
                        mapping=mapping,
                        parent_key=parent_key
                    )

            self.output.append(row_json)

    def _fetch_value(self, row, key):
        '''
        Fetching value from a nested object
        '''
        key_list = key.split('.')
        # print(key_list)
        # print(row)
        value = row
        try:
            for k in key_list:
                value = row[k]
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

    def _product_manifest(self, filename, incremental, primary_key):
        manifest_filename = f'{self.destination}/{filename}.csv.manifest'
        manifest = {
            'incremental': incremental,
            'primary_key': primary_key,
        }

        with open(manifest_filename, 'w') as file_out:
            json.dump(manifest, file_out)
