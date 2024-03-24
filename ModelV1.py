import json
import pandas as pd
from Demon01 import Model  # 假设Model类在your_module模块中


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
        self.merged_df = None
        self.final_df = None
        self.table_name = table_name
        self.table_mappings = table_mappings

    def merge_dataframes(self, how='outer', suffixes=('_db1', '_db2'), indicator=True):
        """
        合并两个数据框。
        """
        self.merged_df = self.df1.merge(self.df2, on=self.on_column, how=how, suffixes=suffixes, indicator=indicator)

    def create_detailed_difference_flag(self):
        """
        为合并后的数据框创建详细的差异标志。
        """
        # if self.table_name not in self.table_mappings:
        #     raise ValueError(f"Table {self.table_name} not found in table_mappings")
        #
        # 查找表映射关系
        for source_table, mapping in table_mappings.items():
            print("-----------------------")
            print(mapping)
            if source_table.endswith(self.table_name):
                target_table_name = mapping['database2TableName']
                field_mappings = mapping['fieldMappings']
                break
        else:
            raise ValueError(f"No mapping found for table {self.table_name}")


        field_mappings = self.table_mappings[self.table_name]["fieldMappings"]
        db2_table_name = self.table_mappings[self.table_name].get("database2TableName", self.table_name.split('.')[1])

        columns_to_compare_db1 = [
            col for col in self.merged_df.columns
            if col.endswith('_db1') and col.replace('_db1', '') != self.on_column and col.replace('_db1',
                                                                                                  '') in field_mappings
        ]
        columns_to_compare_db2 = [
            field_mappings[col.replace('_db1', '')] + '_db2' for col in columns_to_compare_db1
            if field_mappings[col.replace('_db1', '')] + '_db2' in self.merged_df.columns
        ]

        self.merged_df['difference_type'] = self.merged_df.apply(
            lambda row: self._set_difference_type(row, db2_table_name, columns_to_compare_db1, columns_to_compare_db2),
            axis=1
        )

    def _set_difference_type(self, row, db2_table_name, columns_to_compare_db1, columns_to_compare_db2):
        """
        设置差异类型的辅助函数。
        """
        merge_status = row['_merge']
        if merge_status == 'left_only':
            return 'insert'
        elif merge_status == 'right_only' and any(col.endswith(f'_{db2_table_name}') for col in self.merged_df.columns):
            return 'delete'
        elif merge_status == 'both':
            return 'modify' if any(row[col_db1] != row[col_db2] for col_db1, col_db2 in
                                   zip(columns_to_compare_db1, columns_to_compare_db2)) else ''

    def process_and_compare(self, final_columns=None):
        """
        处理并比较两个数据框。
        """
        self.merge_dataframes()
        self.create_detailed_difference_flag()
        self._create_final_df(final_columns)

    def _create_final_df(self, final_columns):
        """
        创建最终的数据框。
        """
        if final_columns is not None:
            self.final_df = self.merged_df[
                [col for col in final_columns if
                 col in self.merged_df.columns and not col.endswith(('_db1', '_db2'))] + ['difference_type']
                ]
        else:
            self.final_df = self.merged_df[
                [col for col in self.merged_df.columns if not col.endswith(('_db1', '_db2'))] + ['difference_type']
                ]

        # 主程序示例


if __name__ == '__main__':
    db_manager = Model.DatabaseManager()
    database1_query = "SELECT * FROM USERS"  # SQL语句
    df_database1 = db_manager.get_data_from_dm8('database1', database1_query)
    print(df_database1)

    database2_query = "SELECT * FROM user_profiles"  # SQL语句
    df_database2 = db_manager.get_data_from_dm8('database2', database2_query)
    print(df_database2)

    table_mappings = db_manager.load_table_mappings()
    print(table_mappings)

    comparer = DataFrameComparer(df_database1, df_database2, on_column='id', table_name='database1.users',
                                 table_mappings=table_mappings)
    comparer.process_and_compare()

    # 输出比较结果到控制台
    print(comparer.final_df)

    # 保存到Excel文件
    comparer.final_df.drop(columns=['_merge']).to_excel('output.xlsx', index=False, engine='openpyxl')