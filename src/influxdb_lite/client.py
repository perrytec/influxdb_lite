from influxdb_client import InfluxDBClient
from influxdb_lite.measurement import Measurement


class Client(InfluxDBClient):
    def __init__(self, url: str, token: str, org: str, **kwargs):
        super().__init__(url=url, token=token, org=org, **kwargs)
        self.url = url
        self.token = token
        self.org = org
        self.query_str = ''
        self.measurement = None
        self.select_list = ['_time']

    def query(self, measurement: Measurement):
        self.measurement = measurement
        self.select_list += measurement.tags + measurement.fields
        self.query_str = '\n'.join([f'from(bucket: "{measurement.bucket}")',
                                   f'|> filter(fn: (r) => r._measurement == "{measurement.name}")'])
        return self

    def select(self, _list: list):
        """ Receives a list of fields to show in the query. If it's not called all the columns will be selected by
         default"""
        self.select_list = _list
        query_list = self.query_str.split('\n')
        range_idxs = [i for i in range(len(query_list)) if 'range' in query_list[i]]
        range_idx = 1 if not range_idxs else range_idxs[0]+1
        query_list.insert(
            range_idx, f'|> filter(fn: (r) => contains(value: r._field, set:{self._parse_list_into_str(_list)}))')
        return self

    def range(self, interval: int):
        self._validate_selection(['_time'])
        query_list = self.query_str.split('\n')
        query_list.insert(1, f'|> range(start: -{interval}d)')
        self.query_str = '\n'.join(query_list)
        return self

    def filter(self, attr_dict: dict):
        """ Adds filter statement to query. Receives an attribute dictionary in the format:
        {'tag_1':[value_tag_1, operation], 'field_1':[value_field_1, operation], ...} for filtering based on
        thresholds, where operation is a string in the list [==, >, <, >=, <=] """
        query_list = self.query_str.split('\n')
        for attr in attr_dict:
            if not isinstance(attr_dict[attr], list):
                raise TypeError(f"Unrecognized format {attr_dict[attr]}")
            if attr in self.measurement.tags:
                query_list.append(f'|> filter(fn: (r) => r["{attr}"] {attr_dict[attr][1]} "{attr_dict[attr][0]}")')
            elif attr in self.measurement.fields:
                query_list.append(f'|> filter(fn: (r) => r["_field"] {attr_dict[attr][1]} "{attr}" and r["_value"] == {attr_dict[attr][0]})')
            else:
                ValueError(f"Unrecognized attribute {attr} given in dictionary.")
        self.query_str = '\n'.join(query_list)
        return self

    def group_by(self, _list: list):
        self._validate_selection(_list)
        query_list = self.query_str.split('\n')
        query_list.append(f'|> group(columns: {self._parse_list_into_str(_list)})')
        self.query_str = '\n'.join(query_list)
        return self

    def order_by(self, _list: list):
        self._validate_selection(_list)
        query_list = self.query_str.split('\n')
        query_list.append(f'|> sort(columns: {self._parse_list_into_str(_list)})')
        self.query_str = '\n'.join(query_list)
        return self

    def limit(self, lmt: int):
        query_list = self.query_str.split('\n')
        query_list.append(f'|> limit(n:{lmt})')
        self.query_str = '\n'.join(query_list)
        return self

    def all(self):
        return self.query_api().query(query=self.query_str, org=self.org)

    @staticmethod
    def _parse_list_into_str(_list):
        _str = "["
        for _int in _list[:-1]:
            _str += f"\"{str(_int)}\","
        return _str + f"\"{str(_list[-1])}\"]"

    def _validate_selection(self, _list):
        for column in _list:
            if column not in self.select_list:
                raise TypeError(f"Please include {column} in select list.")
