import json
from hdfs import InsecureClient
from core.exceptions import HdfsError


def check_dir_path(func):
    """
    dir path 检查装饰器
    :param func:
    :return:
    """

    def check(*args, **kwargs):
        dir_path = kwargs.get('dir_path')

        if not dir_path:
            raise HdfsError('路径为None')

        if not dir_path.startswith('/'):
            raise HdfsError('路径以`/`开头，例如`/data`')

        return func(*args, **kwargs)

    return check


class HdfsDb(object):
    HOST = '192.168.71.156'
    PORT = 50070
    USER = 'hdgs'
    HOST_URI = 'http://{0}:{1}'.format(HOST, PORT)

    def __init__(self):
        self.client = InsecureClient(self.HOST_URI, user=self.USER)

    @check_dir_path
    def list_dir(self, dir_path=None):
        """
        列出根目录
        :return:
        """
        dir_data = self.client.list(dir_path)
        return dir_data

    @check_dir_path
    def mk_dir(self, dir_path=None):
        self.client.makedirs(dir_path)

    def write_file(self, filename, data, dir_path=None):
        """
        写入文件
        hd.write_file('test.json', {'name': 'zhexiao'}, dir_path='/data')
        :param filename:
        :param data:
        :param dir_path:
        :return:
        """
        file_path = '{0}/{1}'.format(dir_path, filename)
        self.client.write(file_path, str(data))

    @check_dir_path
    def read_file(self, filename, dir_path=None):
        """
        读取文件数据
        filedata = hd.read_file('README.txt', dir_path='/data')
        :param filename:
        :param dir_path:
        :return:
        """
        file_path = '{0}/{1}'.format(dir_path, filename)

        with self.client.read(file_path, encoding='utf-8') as reader:
            for line in reader:
                yield line

    @check_dir_path
    def delete(self, filename, dir_path=None):
        file_path = '{0}/{1}'.format(dir_path, filename)
        self.client.delete(file_path)
