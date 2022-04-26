from influxdb_lite.attributes import Tag, Field, Timestamp, Base


class MetaMeasurement(type):
    def __init__(cls, name, *args, **kwargs):
        super(MetaMeasurement, cls).__init__(name)
        cls.tags = [attr_name for attr_name in cls.__dict__ if isinstance(cls.__dict__[attr_name], Tag)]
        cls.fields = [attr_name for attr_name in cls.__dict__ if isinstance(cls.__dict__[attr_name], Field)]
        cls.timestamps = [attr_name for attr_name in cls.__dict__ if isinstance(cls.__dict__[attr_name], Timestamp)]
        cls.columns = cls.tags + cls.fields + cls.timestamps
        [cls.__dict__[elem].set_name(elem) for elem in cls.tags + cls.fields + cls.timestamps]
        cls.dict = {k: v for k, v in cls.__dict__.items() if k in cls.columns}


class Measurement(metaclass=MetaMeasurement):
    name = ''
    bucket = ''
    columns = []
    dict = {}

    def __init__(self, **kwargs):
        for attribute in kwargs:
            setattr(getattr(self, attribute), 'value', kwargs[attribute])

    def get_values(self):
        """Returns a dictionary in the format {column_1: value_1, column_2, value_2, ...} including all the tags,
        fields and timestamp columns. """
        return {k: v.value for k, v in self.dict.items()}
