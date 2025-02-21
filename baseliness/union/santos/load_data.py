# -*- coding: utf-8 -*-
"""
@Author     : Fei Wang
@Contact    : fwang1412@gmail.com
@Time       : 2020/10/14 13:12
@Description: 
"""
import os
import json
import pickle
import pandas as pd


# def load_tables(data_dir):
#     with open(os.path.join(data_dir, 'tables.json')) as f:
#         tables = json.load(f)
#     return tables

def load_datalake_tables(data_dir, baseline):
    """
    读取指定文件夹中所有.json文件中的表格数据。
    
    :param datalake_path: 包含表格文件的文件夹路径
    :return: 包含所有表格数据的字典
    """
    table_data = {}

    datalake_path = os.path.join(data_dir, 'datalake-test')
    # 遍历文件夹中的所有文件
    for filename in os.listdir(datalake_path):
        if filename.endswith('.json'):  # 确保处理的是JSON文件
            file_path = os.path.join(datalake_path, filename)
            table_key = filename[:-5]
            
            # 打开并读取JSON文件
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    # print(file_path)

                    if len(data["data"])>500:
                        data["data"] = data["data"][:500]  # 取前500条数据
                        data["table_array"] = data["table_array"][:501]  # 取前501条数据
                        data["numDataRows"] = 500  # 取前500条数据

                    if baseline == "starmie" or "santos":
                        starmie_data = {}
                        csv_data = data['data']
                        csv_columns = data['title']
                        starmie_data["csv"] = pd.DataFrame(csv_data, columns=csv_columns)
                        starmie_data["json"] = data
                        data = starmie_data

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from {filename}: {e}")
                    continue
                
                # 假设每个JSON文件只包含一个表格
                # for table_key, table_info in data.items():
                #     table_data[table_key] = table_info
                table_data[table_key] = data
    
    return table_data


def load_query_tables(data_dir, baseline):
    """
    读取指定文件夹中所有.json文件中的表格数据。
    
    :param datalake_path: 包含表格文件的文件夹路径
    :return: 包含所有表格数据的字典
    """
    table_data = {}
    
    qts_path = os.path.join(data_dir, 'query-test')
    # 遍历文件夹中的所有文件
    for filename in os.listdir(qts_path):
        if filename.endswith('.json'):  # 确保处理的是JSON文件
            file_path = os.path.join(qts_path, filename)
            table_key = filename[:-5]
            
            # 打开并读取JSON文件
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)

                    if len(data["data"])>500:
                        data["data"] = data["data"][:500]  # 取前500条数据
                        data["table_array"] = data["table_array"][:501]  # 取前501条数据
                        data["numDataRows"] = 500  # 取前500条数据

                    if baseline == "starmie" or "santos":
                        starmie_data = {}
                        csv_data = data['data']
                        csv_columns = data['title']
                        starmie_data["csv"] = pd.DataFrame(csv_data, columns=csv_columns)
                        starmie_data["json"] = data
                        data = starmie_data

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from {filename}: {e}")
                    continue
                
                # 假设每个JSON文件只包含一个表格
                # for table_key, table_info in data.items():
                #     table_data[table_key] = table_info
                # for table_name, table_info in data.items():
                table_data[table_key] = data
    
    return table_data

def load_queries(data_dir, file_name='queries-test.txt'):
    queries = {}
    with open(os.path.join(data_dir, file_name)) as f:
        for line in f.readlines():
            query = line.strip().split('\t')
            queries[query[0]] = query[1:]
    return queries

# def load_groundtruth(data_dir, file_name='Groundtruth.pickle'):
#     Groundtruth_dict = {}
#     with open(os.path.join(data_dir, file_name), "rb") as Groundtruth_file:
#         Groundtruth_dict = pickle.load(Groundtruth_file)
#     return Groundtruth_dict

def load_groundtruth(data_dir):
    qtrels = {}
    with open(os.path.join(data_dir, 'qtrels-test.txt')) as f:
        for line in f.readlines():
            rel = line.strip().split()
            rel[0] = rel[0]
            rel[3] = int(rel[3])
            if rel[0] not in qtrels:
                qtrels[rel[0]] = {}
            qtrels[rel[0]][rel[2]] = rel[3]
    return qtrels
