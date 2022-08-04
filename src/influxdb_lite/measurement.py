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
    accept_dynamic_fields = False  # Defines if to treat unkown columns as new fields or to raise an exception
    _time = Timestamp(name='_time')

    def __init__(self, **kwargs):
        for attribute in kwargs:
            if self.accept_dynamic_fields and not hasattr(self, attribute):
                cls = Field
                _type = None
                self.columns += [attribute]
                self.fields += [attribute]
            else:
                cls = type(getattr(self, attribute))
                _type = getattr(self, attribute)._type
            setattr(self, attribute, cls(name=attribute, value=kwargs[attribute], _type=_type))

    def get_values(self):
        return {column: getattr(getattr(self, column), 'value') for column in self.columns if hasattr(self, column)}
