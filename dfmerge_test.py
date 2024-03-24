import pandas as pd

# 创建两个示例 DataFrame
df1 = pd.DataFrame({
    'key': ['A', 'B', 'C', 'D'],
    'value1': [1, 2, 3, 4]
})

df2 = pd.DataFrame({
    'key': ['B', 'C', 'D', 'E'],
    'value2': [5, 6, 7, 8]
})

# 使用 merge 函数合并两个 DataFrame，并设置 indicator=True
merged_df = pd.merge(df1, df2, on='key', how='outer', indicator=True)

# 打印合并后的 DataFrame，包括 _merge 列
print(merged_df)

# 使用 '_merge' 列的值作为条件（注意这里的引号）
# 找出只出现在左 DataFrame（df1）中的行
left_only = merged_df[merged_df['_merge'] == 'left_only']
print("只出现在 df1 中的行:\n", left_only)

# 找出只出现在右 DataFrame（df2）中的行
right_only = merged_df[merged_df['_merge'] == 'right_only']
print("只出现在 df2 中的行:\n", right_only)

# 找出同时出现在两个 DataFrame 中的行
both = merged_df[merged_df['_merge'] == 'both']
print("同时出现在两个 DataFrame 中的行:\n", both)