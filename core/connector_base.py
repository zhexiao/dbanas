class BaseConnector(object):
    """
    数据库连接基类
    """

    def __init__(self):
        pass

    def connector(self):
        raise NotImplementedError('方法未实现')

    def show_tables(self):
        raise NotImplementedError('方法未实现')

    def export(self):
        raise NotImplementedError('方法未实现')
