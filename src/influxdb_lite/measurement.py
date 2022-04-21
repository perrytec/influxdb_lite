from influxdb_lite.attributes import Tag, Field


class MetaMeasurement(type):
    def __init__(cls, name, *args, **kwargs):
        super(MetaMeasurement, cls).__init__(name)
        cls.tags = [attr_name for attr_name in cls.__dict__ if isinstance(cls.__dict__[attr_name], Tag)]
        cls.fields = [attr_name for attr_name in cls.__dict__ if isinstance(cls.__dict__[attr_name], Field)]
        [cls.__dict__[elem].set_name(elem) for elem in cls.tags + cls.fields]


class Measurement(metaclass=MetaMeasurement):
    name = ''
    bucket = ''
    timestamp_label = ''

    def check_insert(self, insert_dict: dict):
        """Checks correct insertion of dictionaries in the format {tag_1: {field_1: value_1, field_2: value_2, ...}}
        or the format {tag_1: {field_1: value_1, field_2: value_2, ...}}}
        """
        if len(self.tags) == 1:
            for index in insert_dict:
                self._check_fields(insert_dict[index])
        elif len(self.tags) == 2:
            for first_index in insert_dict:
                for second_index in insert_dict[first_index]:
                    self._check_fields(insert_dict[first_index][second_index])
        else:
            raise NotImplementedError(f"Check insert not implement yet for {len(self.tags)} tag(s)")

    def _check_fields(self, sub_dict):
        for field_name in self.fields:
            if field_name not in sub_dict:
                raise ValueError(f"Field key {field_name} not present in data")
        if self.timestamp_label not in sub_dict:
            print("Timestamp label not found, inserting current time")
