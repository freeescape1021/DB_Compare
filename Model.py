import json
import pandas as pd
import dmPython
import configparser
class DatabaseManager:
    def __init__(self, config_file='database.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.table_mappings = self.load_table_mappings()

    def load_table_mappings(self):
        with open("table_mappings.json", "r") as file:
            return json.load(file)

    def get_db_config(self, section):
        db_config = {}
        db_config['host'] = self.config.get(section, 'host')
        db_config['port'] = self.config.getint(section, 'port')
        db_config['user'] = self.config.get(section, 'user')
        db_config['password'] = self.config.get(section, 'password')
        return db_config

    def execute_query(self, db_config, query):
        conn = dmPython.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        cursor.close()
        conn.close()
        return df

    def get_data_from_dm8(self, section, query):
        db_config = self.get_db_config(section)
        return self.execute_query(db_config, query)

# # 实例画示例
# if __name__ == '__main__':
#     db_manager = DatabaseManager()
#
#     # 测试获取数据实例--第一个数据库的数据
#     database1_query = "select * from USERS"  # SQL语句
#     df_database1 = db_manager.get_data_from_dm8('database1', database1_query)
#     print(df_database1)
#
#     # 测试获取数据实例--第二个数据库的数据
#     database2_query = "select * from user_profiles"  # SQL语句
#     df_database2 = db_manager.get_data_from_dm8('database2', database2_query)
#     print(df_database2)