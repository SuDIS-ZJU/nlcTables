import os
import csv
import json

def update_json_files(csv_file, json_folder, target_folder):
    """
    按行遍历.csv文件，记录qtable对应的qcol，形成键值对(qtable, qcol)。
    然后打开对应的{qtable}.json文件，加入“targetCol”属性值为qcol，并将更新后的文件保存到新的文件夹中。
    :param csv_file: CSV文件路径
    :param json_folder: 包含JSON文件的文件夹路径
    :param target_folder: 保存更新后JSON文件的目标文件夹路径
    """
    # 确保JSON文件夹存在
    if not os.path.exists(json_folder):
        raise FileNotFoundError(f"JSON文件夹 '{json_folder}' 不存在。")

    # 确保目标文件夹存在
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # 读取CSV文件，构建qtable到qcol的映射
    qtable_to_qcol = {}
    with open(csv_file, 'r', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            qtable = row[0]
            qcol = row[2]
            qtable_to_qcol[qtable] = qcol

    # 遍历JSON文件夹，更新JSON文件
    for filename in os.listdir(json_folder):
        if filename.endswith('.json'):
            if filename in qtable_to_qcol:
                json_path = os.path.join(json_folder, filename)
                with open(json_path, 'r', encoding='utf-8') as jsonfile:
                    data = json.load(jsonfile)
                
                # 添加 "targetCol" 属性
                data['targetCol'] = qtable_to_qcol[filename]
                
                # 保存更新后的JSON文件到目标文件夹
                target_json_path = os.path.join(target_folder, filename)
                with open(target_json_path, 'w', encoding='utf-8') as jsonfile:
                    json.dump(data, jsonfile, indent=4, ensure_ascii=False)
                print(f"更新了文件 {filename}，添加了 'targetCol': {qtable_to_qcol[filename]}")

    print("所有相关JSON文件已更新并保存到目标文件夹。")

# 示例用法
csv_file = "fuzzy-join/all-fuzzy1/deepjoin.csv"  # 替换为你的CSV文件路径
json_folder = 'fuzzy-join/all-fuzzy1/query-test'  # 替换为你的JSON文件夹路径
target_folder = 'fuzzy-join/fuzzy_test/oridata'  # 替换为你的目标文件夹路径

update_json_files(csv_file, json_folder, target_folder)