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
        query_list = self.query_str.split('\n')
        for attr in attr_dict:
            if attr in self.measurement.tags:
                query_list.append(f'|> filter(fn: (r) => r["{attr}"] == "{attr_dict[attr]}")')
            elif attr in self.measurement.fields:
                query_list.append(f'|> filter(fn: (r) => r["_field"] == "{attr}" and r["_value"] == {attr_dict[attr]})')
            else:
                ValueError(f"Unrecognized attribute {attr} given in dictionary.")
        self.query_str = '\n'.join(query_list)
        return self

    def all(self):
        return self.query_api().query(query=self.query_str, org=self.org)
