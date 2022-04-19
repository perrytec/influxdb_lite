

class Measurement:
    def __init__(self, **kwargs):
        self.name = kwargs.get('name', 'default')
        self.bucket = kwargs.get('bucket', 'default')
        self.tags = kwargs.get('tags', [])
        self.fields = kwargs.get('fields', [])
