import os
import csv
import shutil
import numpy as np
import json
import random

import os

import os

def process_txt_file(source_file):
    """
    读取原始txt文件，删除重复的行，并删除所有rel为0的qid对应的行，
    处理后的内容覆盖原始文件，并按qid降序排列。
    :param source_file: 原始txt文件路径
    """
    # 读取原始文件内容
    seen_lines = set()
    qid_to_rels = {}
    qid_to_lines = {}
    with open(source_file, 'r', encoding='utf-8') as src_file:
        for line in src_file:
            line = line.strip()
            if line in seen_lines:
                continue  # 跳过重复行
            seen_lines.add(line)
            qid, _, dlname, rel = line.split('\t')
            if qid not in qid_to_rels:
                qid_to_rels[qid] = []
                qid_to_lines[qid] = []
            qid_to_rels[qid].append(int(rel))
            qid_to_lines[qid].append(line)

    # 筛选出需要保留的行
    filtered_lines = []
    for qid in qid_to_rels:
        if any(rel != 0 for rel in qid_to_rels[qid]):
            filtered_lines.extend(qid_to_lines[qid])

    # 按qid降序排列
    filtered_lines.sort(key=lambda x: int(x.split('\t')[0]), reverse=False)

    # 覆盖原始文件
    with open(source_file, 'w', encoding='utf-8') as new_txt:
        for line in filtered_lines:
            new_txt.write(line + '\n')

    print(f"处理完成，原始文件已更新为 {source_file}")





def process_qtrel_file(source_file, queries_test_file, target_folder, new_txt_file, new_csv_file):
    """
    读取原始txt文件和queries-test.txt文件，复制内容到新文件夹下的新txt文件，并写入新的csv文件。
    :param source_file: 原始txt文件路径
    :param queries_test_file: queries-test.txt文件路径
    :param target_folder: 新文件夹路径
    :param new_txt_file: 新txt文件名
    :param new_csv_file: 新csv文件名
    """
    # 确保目标文件夹存在
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # 定义新txt文件和csv文件的完整路径
    new_txt_path = os.path.join(target_folder, new_txt_file)
    new_csv_path = os.path.join(target_folder, new_csv_file)

    # 读取queries-test.txt文件，构建qid到string的映射
    query_map = {}
    with open(queries_test_file, 'r', encoding='utf-8') as queries_file:
        for line in queries_file:
            parts = line.strip().split('\t')
            if len(parts) == 3:
                qid, string, _ = parts
                query_map[qid] = string

    # 打开原始文件和新文件
    with open(source_file, 'r', encoding='utf-8') as src_file, \
         open(new_txt_path, 'w', encoding='utf-8') as new_txt, \
         open(new_csv_path, 'w', newline='', encoding='utf-8') as new_csv:
        
        # 创建CSV写入器
        csv_writer = csv.writer(new_csv)
        # 写入CSV文件的列名
        csv_writer.writerow(['query_id', 'query', 'table_id', 'rel'])

        # 逐行读取原始文件内容
        for line in src_file:
            parts = line.strip().split('\t')
            if len(parts) == 4:
                qid, _, dlname, rel = parts
                query = query_map.get(qid, "")
                # 写入新txt文件
                new_txt.write(f"{qid}\tQ0\t{dlname}\t0\t0\trow\n")
                # 写入新csv文件
                csv_writer.writerow([qid, query, dlname, rel])

    shutil.copy(source_file, os.path.join(target_folder, 'qrels.txt'))

    print(f"处理完成，新txt文件已保存到 {new_txt_path}")
    print(f"处理完成，新csv文件已保存到 {new_csv_path}")





def process_query_file(source_file, target_folder, new_txt_file):
    """
    读取原始txt文件，将内容写入新的txt文件。
    :param source_file: 原始txt文件路径
    :param target_folder: 新文件夹路径
    :param new_txt_file: 新txt文件名
    """
    # 确保目标文件夹存在
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # 定义新txt文件的完整路径
    new_txt_path = os.path.join(target_folder, new_txt_file)

    # 打开原始文件和新文件
    with open(source_file, 'r', encoding='utf-8') as src_file, \
         open(new_txt_path, 'w', encoding='utf-8') as new_txt:
        
        # 逐行读取原始文件内容
        for line in src_file:
            parts = line.strip().split('\t')
            if len(parts) == 3:
                qid, string, _ = parts
                # 写入新txt文件
                new_txt.write(f"{qid}\t{string}\n")

    print(f"处理完成，新txt文件已保存到 {new_txt_path}")



def wrap_json_files(source_folder, target_folder):
    """
    遍历文件夹下的所有json文件，将每个json的文件名去掉后缀作为键值加入json文件中，
    并将新的json文件存储到新的文件夹下。
    :param source_folder: 原始文件夹路径
    :param target_folder: 新文件夹路径
    """
    # 确保目标文件夹存在
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # 遍历源文件夹中的所有文件
    for filename in os.listdir(source_folder):
        if filename.endswith('.json'):
            # 构造原始文件的完整路径
            source_path = os.path.join(source_folder, filename)
            # 获取文件名（去掉后缀）
            base_name = os.path.splitext(filename)[0]
            # 构造目标文件的完整路径
            target_path = os.path.join(target_folder, filename)

            # 读取原始JSON文件
            with open(source_path, 'r', encoding='utf-8') as src_file:
                data = json.load(src_file)

            # 将文件名作为键值包装JSON数据
            wrapped_data = {base_name: data}

            # 写入新的JSON文件
            with open(target_path, 'w', encoding='utf-8') as tgt_file:
                json.dump(wrapped_data, tgt_file, ensure_ascii=False, indent=4)

            print(f"处理完成，新JSON文件已保存到 {target_path}")




def create_folds(source_file, target_folder, num_folds=2, test_ratio=0.3):
    """
    读取txt文件，获取行数n，构造数组[0, n-1]作为训练集Tr，
    随机抽取30%作为测试集T，生成num_folds组数据并保存到folds.npy文件中。
    :param source_file: 原始txt文件路径
    :param target_folder: 目标文件夹路径
    :param num_folds: 生成的组数，默认为2
    :param test_ratio: 测试集比例，默认为0.3
    """
    # 确保目标文件夹存在
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # 读取txt文件的行数
    with open(source_file, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    n = len(lines)

    # # 构造数组[0, n-1]
    # indices = np.arange(n)

    # # 生成num_folds组数据
    # folds = []
    # for _ in range(num_folds):
    #     # 随机打乱索引
    #     np.random.shuffle(indices)
    #     # 计算测试集大小
    #     test_size = int(n * test_ratio)
    #     # 分割训练集和测试集
    #     train_indices = indices[test_size:]
    #     test_indices = indices[:test_size]
    #     folds.append([train_indices, test_indices])
    
    # 将 folds 转换为 NumPy 数组
    # folds_array = [np.array(fold) for fold in folds]
    # print("folds_array",folds_array)

    # # 保存到folds.npy文件
    # np.save(os.path.join(target_folder, 'folds.npy'), folds_array)
    Tr = np.arange(n)  # 训练集 [0, n-1]

    folds = []
    T_size = int(0.3 * n)

    for _ in range(num_folds):
        # 随机抽取 30% 的数据作为测试集
        
        T_indices = random.sample(range(n), T_size)
        
        # 生成训练集
        T = np.array(T_indices)
        Tr_indices = np.setdiff1d(Tr, T)  # 训练集为剩余的部分
        folds.append([Tr_indices, T])
    
    print(folds)
    # 将 folds 转换为 NumPy 对象并保存
    folds_array = np.array(folds, dtype=object)  # 使用 dtype=object 以保持不规则形状
    print(folds_array)
    np.save(os.path.join(target_folder, 'folds.npy'), folds_array)

    print(f"处理完成，folds.npy文件已保存到 {os.path.join(target_folder, 'folds.npy')}")

# 示例用法
source_file = 'all/qtrels-test.txt'  # 替换为你的原始txt文件路径

process_txt_file(source_file)

# 示例用法
source_file = 'all/qtrels-test.txt'  # 替换为你的原始txt文件路径
target_folder = 'all-test'  # 替换为你的目标文件夹路径

create_folds(source_file, target_folder)

# 示例用法
source_folder = 'all/datalake-test'  # 替换为你的原始文件夹路径
target_folder = 'all-test/datalake-test'  # 替换为你的目标文件夹路径

wrap_json_files(source_folder, target_folder)

# 示例用法
source_file = 'all/queries-test.txt'  # 替换为你的原始txt文件路径
target_folder = 'all-test'  # 替换为你的目标文件夹路径
new_txt_file = 'queries_wiki.txt'  # 新txt文件名

process_query_file(source_file, target_folder, new_txt_file)


# 示例用法
source_file = 'all/qtrels-test.txt'  # 替换为你的原始txt文件路径
queries_test_file = 'all/queries-test.txt'  # 替换为你的queries-test.txt文件路径
target_folder = 'all-test'  # 替换为你的目标文件夹路径
new_txt_file = 'all.txt'  # 新txt文件名
new_csv_file = 'features2.csv'  # 新csv文件名

process_qtrel_file(source_file, queries_test_file, target_folder, new_txt_file, new_csv_file)