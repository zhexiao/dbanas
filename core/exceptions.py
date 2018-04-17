class BasicException(Exception):
    def __init__(self, *args, **kwargs):
        pass


class ConfigError(BasicException):
    pass


class HdfsError(BaseException):
    pass
