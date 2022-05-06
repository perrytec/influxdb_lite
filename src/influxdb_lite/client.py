from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import ASYNCHRONOUS
from influxdb_client import WritePrecision
import datetime as dt
import time


class Client(InfluxDBClient):
    def __init__(self, url: str, token: str, org: str, **kwargs):
        super().__init__(url=url, token=token, org=org, **kwargs)
        self.url = url
        self.token = token
        self.org = org
        self.query_str = ''
        self.measurement = None
        self.select_list = ['_time']

    def query(self, measurement):
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
        self.select_list = _list if '_time' in _list else _list + ['_time']
        query_list = self.query_str.split('\n')
        range_idxs = [i for i in range(len(query_list)) if 'range' in query_list[i]]
        range_idx = 1 if not range_idxs else range_idxs[0]+1
        query_list.insert(
            range_idx, f'|> filter(fn: (r) => contains(value: r._field, set:{self._parse_list_into_str(_list)}))')
        self.query_str = '\n'.join(query_list)
        return self

    def range(self, start: (int, str, dt.datetime), stop: (int, str, dt.datetime) = None):
        """ Modifies the base query adding a specified range. This range can be either relative or absolute, this will
        depend on the start argument datatype. If start is a string (for example: '-15d'), the type will be considered
        relative and if is datetime or int, it will be considered absolute. A combination of 'start', 'stop' with
        different datatypes will fail.
        If 'stop' key is not present or its value is None, now() will be considered as default. If 'start' key is not
        present method will raise an error. """
        self._validate_selection(['_time'])
        query_list = self.query_str.split('\n')
        v_start, v_stop = self._validate_range(start, stop)
        query_list.insert(1, f"|> range(start: {v_start}, stop: {v_stop})")
        self.query_str = '\n'.join(query_list)
        return self

    def _validate_range(self,  start: (int, str, dt.datetime), stop: (int, str, dt.datetime)):
        if start is None:
            raise ValueError(f"Invalid start value. ")
        elif isinstance(start, str) or isinstance(start, int):
            pass
        elif isinstance(start, dt.datetime):
            start = self._dt_to_RFC3339(start)
            stop = self._dt_to_RFC3339(stop)
        else:
            raise ValueError(f"_type {type(start)} not recognized. ")
        if stop is None:
            stop = 'now()'
        return start, stop

    def filter(self, *args):
        """ Adds filter statement to query. Receives filter statements in the form Measurement.Tag == a, ...
        where the available operations are ==, >, <, >=, <= and the in_ function.
        * The 'in_' operation for fields must be used in conjunction with the select method and only one field at a time
         to work properly. """
        query_list = self.query_str.split('\n')
        for (attr, comparator, value) in args:
            if attr in self.measurement.tags:
                if comparator != 'in':
                    query_list.append(f'|> filter(fn: (r) => r["{attr}"] {comparator} "{value}")')
                else:
                    query_list.append(f'|> filter(fn: (r) => contains(value: r["{attr}"], set: {self._parse_list_into_str(value)}))')
            elif attr in self.measurement.fields:
                if comparator != 'in':
                    query_list.append(f'|> filter(fn: (r) => r["_field"] == "{attr}" and r["_value"] {comparator} {value})')
                else:
                    query_list.append(f'|> filter(fn: (r) => contains(value: r["_value"], set: {str(value)}))')
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

    def order_by(self, _list: list, desc: bool):
        """Sorts influxdb columns in descending or ascending order. """
        self._validate_selection(_list)
        query_list = self.query_str.split('\n')
        query_list.append(f'|> sort(columns: {self._parse_list_into_str(_list)}, desc: {str(desc).lower()})')
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

    def last(self, column: str = '_value'):
        """Returns the last non-null records from selected columns. """
        query_list = self.query_str.split('\n')
        query_list.append(f'|> last(column:"{column}")')
        self.query_str = '\n'.join(query_list)
        return self.all()

    def all(self):
        """Executes the resulting query. """
        return self.drop(['_start', '_stop']).query_api().query(query=self.query_str, org=self.org)

    def drop(self, _list: list):
        query_list = self.query_str.split('\n')
        query_list.append(f'|> drop(columns:{self._parse_list_into_str(_list)})')
        self.query_str = '\n'.join(query_list)
        return self

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

    def _dt_to_RFC3339(self, datetime_obj: dt.datetime = dt.datetime.now(), _format: str = 'long'):
        """Transform datetime object into string RFC3339 format (either in date, short or long format). Ignores
         timezone aware datetime objects. """
        if datetime_obj is not None:
            base = datetime_obj.isoformat()
            res = self._get_resolution(base)
            base = base.split('+')[0] if '+' in base else base
            if _format == 'date':
                return base.split('T')[0]
            elif _format == 'short':
                return base[:-res-1] + 'Z' if res == 6 else base + 'Z'
            elif _format == 'long':
                return base[:-res//2] + 'Z' if res == 6 else base + '.000Z'
            else:
                raise ValueError("Enter a format from 1 to 3")

    @staticmethod
    def _dt_to_unix(datetime_obj: dt.datetime = dt.datetime.now()):
        """Transform datetime object into string RFC3339 format (either in date, short or long format). Ignores
         timezone aware datetime objects. """
        if datetime_obj is not None:
            return int(time.mktime(datetime_obj.timetuple()))

    @staticmethod
    def _get_resolution(isoformat):
        if len(isoformat.split('.')) == 1:
            return -1
        else:
            return len(isoformat.split('.')[1])

    def bulk_insert(self, measurements: list, precision: str = 'ns'):
        """ Receives a list of measurement objects and inserts them at the same time. At least one tag and one
        field per measure are needed. Empty-valued tags or fields will not be included.
        Sets the precision to either seconds (s), milliseconds (ms), microseconds(us) or nanoseconds (default).
        Precision is set for all the batch inserted. timestamp has to be an int equal to the number of s, ms, us or ns
        since epoch. For example use time.time_ns() for default precision and int(time.time()) for precision = 's'. """
        sequence = [''] * len(measurements)
        bucket = measurements[0].bucket
        for i in range(len(measurements)):
            values = measurements[i].get_values()
            tag_set = ','.join([f"{tag}={values[tag]}"
                                for tag in measurements[i].tags if values.get(tag, None) is not None])
            field_set = ','.join([f"{field}={values[field]}"
                                  for field in measurements[i].fields if values.get(field, None) is not None])
            if tag_set == '' or field_set == '':
                raise ValueError(f"Cannot insert zero fields nor tags in measurement number {i}. ")
            if values.get('_time', None) is not None:
                sequence[i] = f"{measurements[i].name},{tag_set} {field_set} {values['_time']}"
            else:
                sequence[i] = f"{measurements[i].name},{tag_set} {field_set}"
        write_api = self.write_api(write_options=ASYNCHRONOUS)
        write_api.write(bucket=bucket, org=self.org, record='\n'.join(sequence),
                        write_precision=getattr(WritePrecision, precision.upper()))
