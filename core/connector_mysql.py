import os
import shutil
import MySQLdb
from core.connector_base import BaseConnector


class MysqlConnector(BaseConnector):
    SQL_STRING = {
        'show_table': ('SHOW TABLES;', '查看数据库中的表'),
        'export_table': (
            'SELECT * FROM {table} INTO OUTFILE "{filename}"',
            '导出表数据到文件'
        )
    }

    def __init__(self, configure=None):
        super(BaseConnector, self).__init__()

        # 变量
        self.connection = None

        # 读取mysql的配置数据
        self.mysql_attr = getattr(configure, 'mysql', None)
        self.commands = getattr(configure, 'commands', None)

        # 连接数据库
        self.connector()

    def connector(self):
        host = self.mysql_attr.get('host', None)
        user = self.mysql_attr.get('user', None)
        passwd = self.mysql_attr.get('passwd', None)
        db = self.mysql_attr.get('db', None)
        port = self.mysql_attr.get('port', 3306)

        self.connection = MySQLdb.connect(
            host=host,
            user=user,
            passwd=passwd,
            db=db,
            port=port
        )

    def query_data(self, sql_string, dict_cursor=None):
        """
        查询
        :param sql_string:
        :param dict_cursor: 字典的类型查找
        :return:
        """
        if dict_cursor:
            dict_cursor = MySQLdb.cursors.DictCursor

        cursor = self.connection.cursor(dict_cursor)
        cursor.execute(sql_string)
        data = cursor.fetchall()
        cursor.close()
        return data

    def execute_data(self, sql_string):
        """
        update，delete或者insert调用
        :param sql_string:
        :return:
        """
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(sql_string)
        self.connection.commit()
        cursor.close()

    def show_tables(self):
        tables = self.query_data(self.SQL_STRING['show_table'][0])
        return tables

    def export(self):
        """
        导出数据
        :return:
        """
        tmp_folder = '/tmp'
        folder = getattr(self.commands, 'folder', '/tmp')
        tables = self.show_tables()
        for t in tables:
            table_name = t[0]
            tmp_path = '{0}/{1}.txt'.format(tmp_folder, table_name)
            path = '{0}/{1}.txt'.format(folder, table_name)

            print('export table:{0}'.format(table_name))
            self.execute_data(self.SQL_STRING['export_table'][0].format(
                table=table_name,
                filename=tmp_path
            ))

            # 迁移数据
            shutil.copy(tmp_path, path)

    def __del__(self):
        """
        资源释放的时候调用
        :return:
        """
        # 关闭数据库
        self.connection.close()
        print('关闭数据库连接')
