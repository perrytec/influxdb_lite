from influxdb_lite.attributes import Tag, Field, Timestamp, Base


class MetaMeasurement(type):
    def __init__(cls, name, *args, **kwargs):
        super(MetaMeasurement, cls).__init__(name)
        cls.tags = [attr_name for attr_name in cls.__dict__ if isinstance(cls.__dict__[attr_name], Tag)]
        cls.fields = [attr_name for attr_name in cls.__dict__ if isinstance(cls.__dict__[attr_name], Field)]
        cls.columns = cls.tags + cls.fields + ['_time']
        [cls.__dict__[elem].set_name(elem) for elem in cls.tags + cls.fields]


class Measurement(metaclass=MetaMeasurement):
    name = ''
    bucket = ''
    _time = Timestamp(name='_time')

    def __init__(self, **kwargs):
        for attribute in kwargs:
            setattr(getattr(self, attribute), 'value', kwargs[attribute])
        self.dict = {column: getattr(getattr(self, column), 'value') for column in self.columns}

    def get_values(self):
        """Returns a dictionary in the format {column_1: value_1, column_2, value_2, ...} including all the tags,
        fields and timestamp columns. """
        return self.dict

