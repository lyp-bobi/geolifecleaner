import pandas as pd

def read_csv_first_1000_rows(file_path):
    """
    读取CSV文件的前1000行

    参数:
    file_path (str): CSV文件的路径

    返回:
    DataFrame: 包含前1000行的数据
    """
    try:
        df = pd.read_csv(file_path, nrows=1000)
        return df
    except Exception as e:
        print(f"读取CSV文件时出错: {e}")
        return None

# 示例用法
file_path = '/c/Users/Chuang/Downloads/train.csv'  # 替换为你的CSV文件路径
data = read_csv_first_1000_rows(file_path)
if data is not None:
    print(data)
