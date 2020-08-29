import os
import logging  # noqa
import sys  # noqa
import pandas as pd


class MappingParser():
    def __init__(self, destination, endpoint, endpoint_data, mapping, parent_key=None):
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
        self._output(df_json=self.output, filename=self.endpoint)

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

                elif col_type == 'user':
                    key = self.mapping[m]['mapping']['destination']
                    value = self.parent_key
                    row_json[key] = value

                elif col_type == 'table':
                    endpoint = self.mapping[m]['destination']
                    mapping = self.mapping[m]['tableMapping']
                    parent_key = row['gid']
                    data = self._fetch_value(row=row, key=m)

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
        value = row
        for k in key_list:
            value = row[k]

        return value

    def _output(self, df_json, filename):
        output_filename = f'{self.destination}/{filename}.csv'
        data_output = pd.DataFrame(df_json, dtype=str)
        if not os.path.isfile(output_filename):
            with open(output_filename, 'a') as b:
                data_output.to_csv(b, index=False)
            b.close()
        else:
            with open(output_filename, 'a') as b:
                data_output.to_csv(b, index=False, header=False)
            b.close()
