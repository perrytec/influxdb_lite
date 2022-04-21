
class Base:
    def __init__(self, **kwargs):
        self.name = kwargs.get('name', None)
        self.default = kwargs.get('default', None)
        self.is_nullable = kwargs.get('is_nullable', True)

    def validate(self, value):
        if value is None and self.default is None and not self.is_nullable:
            raise ValueError('This tag cannot be nullable')

    def set_name(self, name: str):
        if self.name is not None and self.name != name:
            raise AttributeError('Name duplicity: Two distinct names where given: %s and %s' % (self.name, name))
        self.name = name

    def __eq__(self, other):
        return self.name, '==', other

    def __gt__(self, other):
        return self.name, '>', other

    def __ge__(self, other):
        return self.name, '>=', other

    def __lt__(self, other):
        return self.name, '<', other

    def __le__(self, other):
        return self.name, '<=', other

    def in_(self, other):
        return self.name, 'in', other


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

