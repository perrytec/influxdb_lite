from influxdb_client import InfluxDBClient


class Client(InfluxDBClient):
    def __init__(self, url: str, token: str, org: str, **kwargs):
        super().__init__(url=url, token=token, org=org, **kwargs)
        self.url = url
        self.token = token
        self.org = org
        self.query_str = ''

    def query(self, measurement: Measurement):
        self.query_str = '\n'.join([f'from(bucket: "{measurement.bucket}")',
                                   f'|> filter(fn: (r) => r._measurement == "{measurement.name}")'])
        return self

    def range(self, interval: int):
        query_list = self.query_str.split('\n')
        query_list.append(f'|> range(start: -{interval}d)')
        self.query_str = '\n'.join(query_list)
        return self

