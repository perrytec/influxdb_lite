
class Base:
    def __init__(self, **kwargs):
        self.name = kwargs.get('name', None)
        self.default = kwargs.get('default', None)
        self.is_nullable = kwargs.get('is_nullable', True)

    def validate(self, value):
        if value is None and self.default is None and not self.is_nullable:
            raise ValueError('This tag cannot be nullable')


class Tag(Base):
    pass


class Field(Base):
    pass


class Timestamp(Base):
    pass


class GeneralAttr:
    pass


class Intattr(GeneralAttr):
    pass


class Floatattr(GeneralAttr):
    pass


class Strattr(GeneralAttr):
    pass


class Boolattr(GeneralAttr):
    pass

