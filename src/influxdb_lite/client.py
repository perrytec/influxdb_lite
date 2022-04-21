from influxdb_client import InfluxDBClient
from influxdb_lite.measurement import Measurement
import datetime as dt


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
        """Defines the base query from the bucket and the name of the measurement selected. All the following
        methods need a base query to work. """
        self.measurement = measurement
        self.select_list += measurement.tags + measurement.fields
        self.query_str = '\n'.join([f'from(bucket: "{measurement.bucket}")',
                                   f'|> filter(fn: (r) => r._measurement == "{measurement.name}")'])
        return self

    def select(self, _list: list):
        """ Receives a list of fields to show in resulting table of the query. If it's not called, all the columns
        will be selected by default. """
        self.select_list = _list
        query_list = self.query_str.split('\n')
        range_idxs = [i for i in range(len(query_list)) if 'range' in query_list[i]]
        range_idx = 1 if not range_idxs else range_idxs[0]+1
        query_list.insert(
            range_idx, f'|> filter(fn: (r) => contains(value: r._field, set:{self._parse_list_into_str(_list)}))')
        return self

    def range(self, interval: dict):
        """ Modifies the base query adding a specified range. The format of the interval dictionary depends on the type
        of range given, this can be either relative or absolute. If the datatype of interval['start'] is a string, the
        type will relative and if it is a datetime object, it will be considered absolute. A combination of
        interval['start'], interval['stop'] with different datatype will fail.

        If the range is relative, interval accepts a dictionary in the format {'start': rr_start, 'stop': rr_stop} where
        rr_start and rr_stop are relative range strings like -15d. If _type is 'absolute', interval accepts a dictionary
        in the format {'start': dt_start, 'stop': datetime} where dt_start and dt_stop are datetime.datetime timezone
        unaware objects. If 'stop' key is not present or its value is None, now() will be considered as default. If
        'start' key is not present method will raise an error. """
        self._validate_selection(['_time'])
        query_list = self.query_str.split('\n')
        valid_interval = self._validate_range(interval)
        query_list.insert(1, f"|> range(start: {valid_interval['start']}, stop: {valid_interval['stop']})")
        self.query_str = '\n'.join(query_list)
        return self

    def _validate_range(self, interval: dict):
        if interval.get('start', None) is None:
            raise ValueError(f"Invalid start value. ")
        elif isinstance(interval['start'], str):
            pass
        elif isinstance(interval['start'], dt.datetime):
            interval['start'] = self._dt_to_RFC3339(interval['start'])
            interval['stop'] = self._dt_to_RFC3339(interval.get('stop', None))
        else:
            raise ValueError(f"_type {type(interval['start'])} not recognized. ")

        if interval.get('stop', None) is None:
            interval['stop'] = 'now()'
        return interval

    def filter(self, attr_dict: dict):
        """ Adds filter statement to query. Receives an attribute dictionary in the format:
        {'tag_1':[value_tag_1, operation], 'field_1':[value_field_1, operation], ...} for filtering based on
        thresholds, where operation is a string in the list [==, >, <, >=, <=]. """
        query_list = self.query_str.split('\n')
        for attr in attr_dict:
            if not isinstance(attr_dict[attr], list):
                raise TypeError(f"Unrecognized format {attr_dict[attr]}")
            op = attr_dict[attr][1]
            value = attr_dict[attr][0]
            if attr in self.measurement.tags:
                query_list.append(f'|> filter(fn: (r) => r["{attr}"] {op} "{value}")')
            elif attr in self.measurement.fields:
                query_list.append(f'|> filter(fn: (r) => r["_field"] {op} "{attr}" and r["_value"] == {value})')
            else:
                ValueError(f"Unrecognized attribute {attr} given in dictionary.")
        self.query_str = '\n'.join(query_list)
        return self

    def group_by(self, _list: list):
        """Group by the influxdb tables based on influxdb columns. """
        self._validate_selection(_list)
        query_list = self.query_str.split('\n')
        query_list.append(f'|> group(columns: {self._parse_list_into_str(_list)})')
        self.query_str = '\n'.join(query_list)
        return self

    def order_by(self, _list: list):
        """Sorts influxdb columns in descending order. """
        self._validate_selection(_list)
        query_list = self.query_str.split('\n')
        query_list.append(f'|> sort(columns: {self._parse_list_into_str(_list)})')
        self.query_str = '\n'.join(query_list)
        return self

    def pivot(self, row_keys: list = None, column_keys: list = None, value_column: str = '_value'):
        """Pivots a table based on row_keys, column_keys and a value_column. The default call pivots field sets into
        a sql-like table. """
        row_keys = ['_time'] if row_keys is None else row_keys
        column_keys = ['_field'] if column_keys is None else column_keys
        query_list = self.query_str.split('\n')
        query_list.append(f'|> pivot(rowKey:{self._parse_list_into_str(row_keys)}, columnKey: {self._parse_list_into_str(column_keys)}, valueColumn: "{value_column}")')
        self.query_str = '\n'.join(query_list)
        return self

    def limit(self, lmt: int):
        """Limits the amount of results to {lmt}. """
        query_list = self.query_str.split('\n')
        query_list.append(f'|> limit(n:{lmt})')
        self.query_str = '\n'.join(query_list)
        return self

    def all(self):
        """Executes the resulting query. """
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
                raise TypeError(f"Please include {column} in the select list.")

    def _dt_to_RFC3339(self, datetime_obj: dt.datetime = dt.datetime.now(), format: str = 'short'):
        """Transform datetime object into string RFC3339 format (either in date, short or long format). Ignores
         timezone aware datetime objects. """
        if datetime_obj is not None:
            base = datetime_obj.isoformat()
            res = self._get_resolution(base)
            base = base.split('+')[0] if '+' in base else base
            if format == 'date':
                return base.split('T')[0]
            elif format == 'short':
                return base[:-res-1] + 'Z' if res == 6 else base + 'Z'
            elif format == 'long':
                return base[:-res//2] + 'Z' if res == 6 else base + '.000Z'
            else:
                raise ValueError("Enter a format from 1 to 3")
        else:
            return None

    @staticmethod
    def _get_resolution(isoformat):
        if len(isoformat.split('.')) == 1:
            return -1
        else:
            return len(isoformat.split('.')[1])
