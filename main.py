from core.configure import Configure
from core.connector_mysql import MysqlConnector

if __name__ == '__main__':
    mc = MysqlConnector(configure=Configure())
    mc.export()
