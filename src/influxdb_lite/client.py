from influxdb_client import InfluxDBClient


class Client(InfluxDBClient):
    def __init__(self, url: str, token: str, org: str, **kwargs):
        super().__init__(url=url, token=token, org=org, **kwargs)
        self.url = url
        self.token = token
        self.org = org
