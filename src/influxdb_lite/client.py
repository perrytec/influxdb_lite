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

    def query(self, measurement: Measurement):
        self.measurement = measurement
        self.query_str = '\n'.join([f'from(bucket: "{measurement.bucket}")',
                                   f'|> filter(fn: (r) => r._measurement == "{measurement.name}")'])
        return self

    def range(self, interval: int):
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
        query_list = self.query_str.split('\n')
        query_list.append(f'|> group(columns: {self._parse_list_into_str(_list)})')
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
