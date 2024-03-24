import json
from   Demon01 import  Model
import pandas as pd
import dmPython
import configparser

class DataFrameComparer:
    def __init__(self, df1, df2, on_column,table_name, table_mappings):
        self.df1 = df1
        self.df2 = df2
        self.on_column = on_column
        self.merged_df = None
        self.final_df = None
        self.table_name=table_name
        self.table_mappings = table_mappings

    def merge_dataframes(self, how='outer', suffixes=('_db1', '_db2'), indicator=True):
        self.merged_df = self.df1.merge(self.df2, on=self.on_column, how=how, suffixes=suffixes, indicator=indicator)

    def create_detailed_difference_flag(self, table_name):
        if table_name not in self.table_mappings:
            raise ValueError(f"Table {table_name} not found in table_mappings")

        field_mappings = self.table_mappings[table_name]["fieldMappings"]
        db2_table_name = self.table_mappings[table_name].get("database2TableName", table_name.split('.')[1])

        columns_to_compare_db1 = [col for col in self.merged_df.columns if col.endswith('_db1') and
                                  col.replace('_db1', '') != self.on_column and col.replace('_db1',
                                                                                            '') in field_mappings]
        columns_to_compare_db2 = [field_mappings[col.replace('_db1', '')] + '_db2' for col in columns_to_compare_db1]
        columns_to_compare_db2 = [col for col in columns_to_compare_db2 if col in self.merged_df.columns]

        self.merged_df['difference_type'] = ''

        def set_difference_type(row):
            merge_status = row['_merge']
            if merge_status == 'left_only':
                return 'insert'
            elif merge_status == 'right_only' and any(
                    col.endswith(f'_{db2_table_name}') for col in self.merged_df.columns):
                return 'delete'
            elif merge_status == 'both':
                return 'modify' if any(row[col_db1] != row[col_db2] for col_db1, col_db2 in
                                       zip(columns_to_compare_db1, columns_to_compare_db2)) else ''

        self.merged_df['difference_type'] = self.merged_df.apply(set_difference_type, axis=1)

    def process_and_compare(self, final_columns=None):
        self.merge_dataframes()
        self.create_detailed_difference_flag(self.table_name)
        if final_columns is not None:
            self.final_df = self.merged_df[[col for col in final_columns if
                                            col in self.merged_df.columns and not col.endswith(('_db1', '_db2'))] + [
                                               'difference_type']]
        else:
            self.final_df = self.merged_df[
                [col for col in self.merged_df.columns if not col.endswith(('_db1', '_db2'))] + ['difference_type']]
# 实例画示例
if __name__ == '__main__':
    db_manager = Model.DatabaseManager()
    # 测试获取数据实例--第一个数据库的数据
    database1_query = "select * from USERS"  # SQL语句
    df_database1 = db_manager.get_data_from_dm8('database1', database1_query)
    print(df_database1)
    # 测试获取数据实例--第二个数据库的数据
    database2_query = "select * from user_profiles"  # SQL语句
    df_database2 = db_manager.get_data_from_dm8('database2', database2_query)
    print(df_database2)
    table_mappings=db_manager.load_table_mappings()
    print(table_mappings)
    comparer = DataFrameComparer(df_database1, df_database2, on_column='id',table_name='users',table_mappings=table_mappings)
    comparer.process_and_compare()

    # 输出比较结果到控制台
    print(comparer.final_df)
    df_ret = comparer.final_df.drop(columns=['_merge'])
    print(df_ret)
    df_ret.to_excel('output.xlsx', index=False, engine='openpyxl')

