import pandas as pd

from Demon01 import Model  # 假设Model类在your_module模块中
import openpyxl
class DataFrameComparer:
    def __init__(self, df1, df2, on_column, table_name, table_mappings):
        """
        初始化DataFrameComparer对象。

        参数:
        df1 (pd.DataFrame): 第一个数据框。
        df2 (pd.DataFrame): 第二个数据框。
        on_column (str): 用于合并的列名。
        table_name (str): 表名。
        table_mappings (dict): 表映射字典。
        """
        self.df1 = df1
        self.df2 = df2
        self.on_column = on_column
        self.final_df = None
        self.table_name = table_name
        self.table_mappings = table_mappings
        self.merged_df = self.df1.merge(self.df2, on=self.on_column, how='outer', suffixes=('_db1', '_db2'),
                                        indicator=True,)
    def load_map_info(self):
        # 查找表映射关系
        for source_table, mapping in self.table_mappings.items():
            if source_table.endswith(self.table_name):
                target_table_name = mapping['database2TableName']
                field_mappings = mapping['fieldMappings']
                break
        else:
            raise ValueError(f"No mapping found for table {self.table_name}")

    def set_difference_type(self):
        """
        为merged_df中的每一行设置差异类型（'insert', 'delete', 'both'（可能需要进一步检查是否为'modify'））。
        """
            # 定义一个空列表来存储差异类型
        difference_types = []
        # 遍历每一行来设置差异类型
        for index, row in self.merged_df.iterrows():
            merge_status = row['_merge']
            if merge_status == 'left_only':
                difference_types.append('insert')
            elif merge_status == 'right_only' :
                difference_types.append('delete')
            elif merge_status == 'both':
                # 这里需要进一步检查是否为'modify'
                difference_types.append('both')  # 暂时设置为'both'，后面可以进一步修改为'modify'或空字符串
            else:
                raise ValueError(f"Unexpected merge status: {merge_status}")

                # 将差异类型作为新列添加到merged_df中
        self.merged_df['diff_action'] = difference_types
        return self.merged_df
    def create_result_sql(self):
        """
        根据，set_difference_type,flag标记来判断生产insert、delete、update语句。
        """
        pass

if __name__ == '__main__':
    db_manager = Model.DatabaseManager()
    database1_query = "SELECT * FROM USERS"  # SQL语句
    df_database1 = db_manager.get_data_from_dm8('database1', database1_query)
    # print(df_database1)

    database2_query = "SELECT * FROM user_profiles"  # SQL语句
    df_database2 = db_manager.get_data_from_dm8('database2', database2_query)
    # print(df_database2)

    table_mappings = db_manager.load_table_mappings()


    comparer = DataFrameComparer(df_database1, df_database2, on_column='id', table_name='database1.users',
                                 table_mappings=table_mappings)
    final_df=comparer.set_difference_type()


    # 保存到Excel文件
    final_df.drop(columns=['_merge']).to_excel('output.xlsx', index=False, engine='openpyxl')