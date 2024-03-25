import pandas as pd
from Demon01 import Model  # 假设Model类在your_module模块中
import openpyxl

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)

table_mapping={
    "database1.orders":
    {
      "database2TableName": "order_details",
      "fieldMappings": {
        "orderId": "order_id",
        "productId": "product_ref_id",
        "quantity": "quantity_ordered"}
    },

    "database1.users":
    {
      "database2TableName": "user_profiles",
      "fieldMappings": {
        "id": "user_id",
        "username": "user_name",
        "email": "email_address",
        "sex":"sex"}
    }
}
class DataFrameComparer:
    def __init__(self, df1, df2, on_column, table_name, table_mappings):
        self.df1 = df1
        self.df2 = df2
        self.on_column = on_column
        self.final_df = None
        self.table_name = table_name
        self.table_mappings = table_mappings
        self.diff_columns = ()  # 使用集合存储差异列，以去除重复项
        # print(self.load_map_info())
        self.diff_columns = []  # 用于存储数据不一致的列名
        self.merged_df = self.load_map_info()

    def load_map_info(self):
        for source_table, mapping in self.table_mappings.items():
            if source_table.endswith(self.table_name):
                field_mappings = mapping.get('fieldMappings')
                reversed_dict = {value: key for key, value in field_mappings.items()}
                if field_mappings:  # 检查 field_mappings 是否存在且非空
                    df2_rename = self.df2.rename(columns=reversed_dict)
                    merged_df = self.df1.merge(df2_rename, on=self.on_column, how='outer', suffixes=('_db1', '_db2')
                                               , indicator=True)
                    return merged_df  # 返回重命名后的 DataFrame
                else:
                    raise ValueError("Field mappings are empty for the given table.")
        else:
            raise ValueError(f"No mapping found for table {self.table_name}")



    def set_difference_type(self):
        difference_types = []
        for index, row in self.merged_df.iterrows():
            if row['_merge'] == 'left_only':
                difference_types.append('insert')
            elif row['_merge'] == 'right_only':
                difference_types.append('delete')
            elif row['_merge'] == 'both':
                is_modified = False
                for col in self.df1.columns:
                    if col != self.on_column and row[col + '_db1'] != row[col + '_db2']:
                        self.diff_columns.append(col)
                        is_modified = True
                difference_types.append('modify' if is_modified else 'both_same')
            else:
                raise ValueError(f"Unexpected merge status: {row['_merge']}")

        self.merged_df['diff_action'] = difference_types
        print(self.merged_df)
        return self.merged_df

    def create_result_sql(self):
        sql_statements = []  # 用于存储生成的SQL语句
        for index, row in self.merged_df.iterrows():
            diff_action = row['diff_action']
            if diff_action == 'insert':
                # 生成INSERT语句（假设df1是源数据库，需要插入到df2中）
                columns = ', '.join(self.df1.columns)
                values = ', '.join([f"'{str(value)}'" if isinstance(value, str) else str(value) for value in
                                    row[self.df1.columns.tolist()]])  # 处理字符串类型的值，加上单引号
                sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({values});"
                sql_statements.append(sql)
            elif diff_action == 'delete':
                # 生成DELETE语句（假设df2是目标数据库，需要从中删除某些行）
                condition = f"{self.on_column} = {row[self.on_column + '_db2']}"  # 根据on_column列的值来删除行
                sql = f"DELETE FROM {self.table_name} WHERE {condition};"
                sql_statements.append(sql)
            elif diff_action == 'modify':
                # 生成UPDATE语句（假设df2是目标数据库，需要更新其中的某些行）
                set_clause = ', '.join([f"{col} = {row[col + '_db1']}" for col in self.diff_columns if
                                        pd.notna(row[col + '_db1'])])  # 只更新那些在df1中有值的列
                condition = f"{self.on_column} = {row[self.on_column + '_db2']}"  # 根据on_column列的值来更新行
                sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {condition};"
                sql_statements.append(sql)
        print(sql_statements)
        return sql_statements


if __name__ == '__main__':
    db_manager = Model.DatabaseManager()
    database1_query = "SELECT * FROM USERS"
    df_database1 = db_manager.get_data_from_dm8('database1', database1_query)
    database2_query = "SELECT * FROM user_profiles"
    df_database2 = db_manager.get_data_from_dm8('database2', database2_query)
    table_mapping = db_manager.load_table_mappings()
    runapp = DataFrameComparer(df_database1, df_database2, on_column='id', table_name='users',
                               table_mappings=table_mapping)
    final_df = runapp.set_difference_type()
    final_df.drop(columns=['_merge']).to_excel('output.xlsx', index=False, engine='openpyxl')

    # runapp.create_result_sql()
