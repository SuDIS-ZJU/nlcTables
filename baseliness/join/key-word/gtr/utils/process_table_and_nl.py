# -*- coding: utf-8 -*-
"""
@Author     : Fei Wang
@Contact    : fwang1412@gmail.com
@Time       : 2020/12/28 19:46
@Description: 
"""
from tqdm import tqdm
import numpy as np
import torch

from src.table_encoder.gtr.tabular_graph import TabularGraph
from pytorch_transformers import BertTokenizer, BertModel

# import tensorflow as tf
# from transformers import BertTokenizer, TFBertModel

# from src.table_encoder.tapas.modeling import BertConfig,BertModel

from src.table_encoder.starmie.sdd.pretrain import load_checkpoint, inference_on_table
import torch
import pandas as pd
import numpy as np
import glob
import pickle
import time
import sys
import argparse
from tqdm import tqdm
import json

def process_tables(tables, config):   #replace with our own table processer
    for tid in tqdm(tables.keys(), desc="processing tables"):
        if config["baseline-model"] == "GTR" :    

            constructor = TabularGraph(config["fasttext"], config["merge_same_cells"])
            
            dgl_graph, node_features, table_data = constructor.construct_graph(tables[tid])
            print(dgl_graph.num_nodes)
            if(dgl_graph.num_nodes==0):
                print(tables[tid]["caption"])
                dgl_graph, node_features, table_data = constructor.construct_graph(tables[tid])
                print(dgl_graph.num_nodes)
                
            tables[tid]["dgl_graph"] = dgl_graph
            if isinstance(node_features, np.ndarray):
                node_features = torch.FloatTensor(node_features)
            tables[tid]["node_features"] = node_features

        # if config["baseline-model"] == "TaBERT" :
        #     model = BertTokenizer.from_pretrained(
        #     'path/to/pretrained/model/checkpoint.bin',
        #     )

        #     tables[tid]["context"], tables[tid]["column_encoding"], info_dict = model.encode(
        #     contexts=[model.tokenizer.tokenize(tables[tid]["caption"])],
        #     tables=[tables[tid]]
        #     )
        if config["baseline-model"] == "starmie" :
            # print(tables[tid].columns)
            feature = extractVectors(tables[tid], "starmie", "drop_col", "head", "column", 0, False)
            # tables[tid]["column_encoding"] = extractVectors(tables[tid], "starmie", "drop_col", "head", "column", 0, False)
            print(len(feature))
            tables[tid]["column_encoding"] = np.repeat(feature, len(tables[tid]))
        # dataEmbeds = []

        # start_time = time.time()
        # cl_features = extractVectors(tables, "starmie", "drop_col", "head", "column", 0, False)
        # inference_times += time.time() - start_time
        # print("%s %s inference time: %d seconds" %("starmie", dir, time.time() - start_time))
        # dfs_count = 0
        # for tid in tqdm(tables.keys(), desc="processing tables"):
        #     dfs_count += 1
        #     # get features for this file / dataset
        #     tables[tid]["column_encoding"] = np.array(cl_features[i])
            # dataEmbeds.append((file, cl_features_file))

        # output_path = "src/table_encoder/starmie/vectors/cl_query-test_drop_col_head_column_1.pkl"
        # pickle.dump(dataEmbeds, open(output_path, "wb"))
        

        # print("Benchmark: ", dataFolder)
        # print("Benchmark: starmie")
        # print("--- Total Inference Time: %s seconds ---" % (inference_times))

def process_tables_starmie(tables, config):   #replace with our own table processer
    feature = {}
    for tid in tqdm(tables.keys(), desc="processing tables"):
        
        feature[tid] = {}
        if config["baseline-model"] == "starmie" :
            feature[tid]["table"] = tables[tid]["json"]
            # print(tables[tid].columns)
            feature[tid]["column_encoding"] = extractVectors(tables[tid]["csv"], "starmie", "drop_col", "head", "column", 0, False)
            # tables[tid]["column_encoding"] = extractVectors(tables[tid], "starmie", "drop_col", "head", "column", 0, False)

    return feature     

def process_tables_tapas(tables, config):   #replace with our own table processer
    # 确保模型文件路径正确
    model_name = 'src/table_encoder/tapas/model/'

    # 加载分词器和模型
    tokenizer = BertTokenizer.from_pretrained(model_name)
    model = BertModel.from_pretrained(model_name)

    # 将模型设置为评估模式
    model.eval()

    # 如果您需要使用 GPU
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    for tid in tqdm(tables.keys(), desc="processing tables"):
        
        if config["baseline-model"] == "tapas" :
            
            # print(tables[tid].columns)
            inputs = tokenizer(tables[tid]["table_array"], return_tensors="pt") 
            inputs = {k: v.to(device) for k, v in inputs.items()}
            # tables[tid]["column_encoding"] = extractVectors(tables[tid], "starmie", "drop_col", "head", "column", 0, False)
            # 使用模型进行预测
            with torch.no_grad():
                outputs = model(**inputs)
            
            tables[tid]["table_encoding"] = outputs


# def process_tables_tapas(tables, config):   #replace with our own table processer
#     # 确保模型文件路径正确
#     model_name = 'src/table_encoder/tapas/model/'

#     # 加载分词器和模型
#     tokenizer = BertTokenizer.from_pretrained(model_name)
#     model = TFBertModel.from_pretrained(model_name)

#     # 将模型设置为评估模式
#     model.config.output_hidden_states = True

#     # # 如果您需要使用 GPU
#     # device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
#     # model.to(device)

#     for tid in tqdm(tables.keys(), desc="processing tables"):
        
#         if config["baseline-model"] == "tapas" :
            
#             # print(tables[tid].columns)
#             inputs = tokenizer(tables[tid]["table_array"], return_tensors="tf") 
#             inputs = {k: v for k, v in inputs.items()}
#             # tables[tid]["column_encoding"] = extractVectors(tables[tid], "starmie", "drop_col", "head", "column", 0, False)
#             # 使用模型进行预测
           
#             outputs = model(**inputs)

#             # 提取最后一层的隐藏状态
#             last_hidden_states = outputs.last_hidden_state
            
#             tables[tid]["table_encoding"] = last_hidden_states
 

# def process_queries(queries, q_tables, constructor):  #replace with our own query processer
def process_queries(queries, q_tables, config) :
    nl_queries = {}
    feature = {}
    for qid in tqdm(queries.keys(), desc="processing queries"):
        feature[qid] = {}

        if config["baseline-model"] == "GTR" :

            constructor = TabularGraph(config["fasttext"], config["merge_same_cells"])
            nl_queries = constructor.w2v[" ".join(queries[qid][0])]
            if isinstance(nl_queries, np.ndarray):
                nl_queries = torch.FloatTensor(nl_queries)
        
            feature[qid]["nl"] = queries[qid][0]
            feature[qid]["nl_features"] = nl_queries

            feature[qid]["table"] = queries[qid][1]
            dgl_graph, node_features, table_data = constructor.construct_graph(q_tables[queries[qid][1]])
            feature[qid]["dgl_graph"] = dgl_graph
            if isinstance(node_features, np.ndarray):
                node_features = torch.FloatTensor(node_features)
            feature[qid]["node_features"] = node_features

        if config["baseline-model"] == "starmie" :
            constructor = TabularGraph(config["fasttext"], config["merge_same_cells"])
            nl_queries = constructor.w2v[" ".join(queries[qid][0])]
            if isinstance(nl_queries, np.ndarray):
                nl_queries = torch.FloatTensor(nl_queries)
        
            feature[qid]["nl"] = queries[qid][0]
            feature[qid]["nl_features"] = nl_queries

            # print(queries[qid][0])
            # # print(queries)
            # print(queries[qid][1])
            # print(q_tables[queries[qid][1]])
            # print(q_tables[queries[qid][1]]["json"])

            feature[qid]["table"] = q_tables[queries[qid][1]]["json"]
            # print(q_tables[queries[qid][1]].columns)
            feature[qid]["column_encoding"] = extractVectors(q_tables[queries[qid][1]]["csv"], config["data_dir"], "drop_col", "head", "column", 0, False)

        # if config["baseline-model"] == "TaBERT" :
        #     model = BertTokenizer.from_pretrained(
        #     'path/to/pretrained/model/checkpoint.bin',
        #     )

        #     feature[qid]["nl"] = queries[qid][0]
        #     feature[qid]["table"] = queries[qid][1]

        #     feature[qid]["nl_features"], feature[qid]["column_encoding"], info_dict = model.encode(
        #     contexts=[model.tokenizer.tokenize(" ".join(queries[qid][0]))],
        #     tables=[q_tables[queries[qid][1]]]
        #     )

        if config["baseline-model"] == "tapas" :
            constructor = TabularGraph(config["fasttext"], config["merge_same_cells"])
            nl_queries = constructor.w2v[" ".join(queries[qid][0])]
            if isinstance(nl_queries, np.ndarray):
                nl_queries = torch.FloatTensor(nl_queries)
        
            feature[qid]["nl"] = queries[qid][0]
            feature[qid]["nl_features"] = nl_queries

            feature[qid]["table"] = q_tables[queries[qid][1]]["table_array"]
            feature[qid]["table_encoding"] = extractVectors(q_tables[queries[qid][1]]["csv"], config["data_dir"], "drop_col", "head", "column", 0, False)


        # Here we simply connect two feature vectors
        # Attention: Ensure that the dimensions of the two feature vectors are the same or perform necessary dimensional transformations
        # nl_queries_unsqueeze = torch.tensor(nl_queries).unsqueeze(0)
        # feature[qid]["combined_features"] = torch.cat((nl_queries_unsqueeze, node_features), dim=0)

    return feature

def process_queries_tapas(queries, q_tables, config) :   #replace with our own table processer
    
    nl_queries = {}
    feature = {}
    
    # 确保模型文件路径正确
    model_name = 'src/table_encoder/tapas/model/'

    # 加载分词器和模型
    tokenizer = BertTokenizer.from_pretrained(model_name)
    model = BertModel.from_pretrained(model_name)

    # 将模型设置为评估模式
    model.eval()

    # 如果您需要使用 GPU
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    for qid in tqdm(queries.keys(), desc="processing queries"):
        feature[qid] = {}
        
        if config["baseline-model"] == "tapas" :
        
            feature[qid]["nl"] = queries[qid][0]
            feature[qid]["nl_features"] = nl_queries
            
            # print(tables[tid].columns)
            inputs = tokenizer(queries[qid][0], return_tensors="pt") 
            inputs = {k: v.to(device) for k, v in inputs.items()}
            # tables[tid]["column_encoding"] = extractVectors(tables[tid], "starmie", "drop_col", "head", "column", 0, False)
            # 使用模型进行预测
            with torch.no_grad():
                outputs = model(**inputs)
            
            feature[qid]["nl_features"] = outputs

            feature[qid]["table"] = q_tables[queries[qid][1]]["table_array"]

            # print(tables[tid].columns)
            inputs = tokenizer(q_tables[queries[qid][1]]["table_array"], return_tensors="pt") 
            inputs = {k: v.to(device) for k, v in inputs.items()}
            # tables[tid]["column_encoding"] = extractVectors(tables[tid], "starmie", "drop_col", "head", "column", 0, False)
            # 使用模型进行预测
            with torch.no_grad():
                outputs = model(**inputs)
            
            feature[qid]["table_encoding"] = outputs

    return feature

# def process_queries_tapas(queries, q_tables, config) :   #replace with our own table processer
    
#     nl_queries = {}
#     feature = {}
    
#     # 确保模型文件路径正确
#     model_name = 'src/table_encoder/tapas/model/'

#     # 加载分词器和模型
#     tokenizer = BertTokenizer.from_pretrained(model_name)
#     model = TFBertModel.from_pretrained(model_name)

#     # 将模型设置为评估模式
#     model.config.output_hidden_states = True

#     for qid in tqdm(queries.keys(), desc="processing queries"):
#         feature[qid] = {}
        
#         if config["baseline-model"] == "tapas" :
        
#             feature[qid]["nl"] = queries[qid][0]
#             feature[qid]["nl_features"] = nl_queries
            
#             # print(tables[tid].columns)
#             inputs = tokenizer(queries[qid][0], return_tensors="tf") 
#             inputs = {k: v for k, v in inputs.items()}
#             # tables[tid]["column_encoding"] = extractVectors(tables[tid], "starmie", "drop_col", "head", "column", 0, False)
#             # 使用模型进行预测
            
#             outputs = model(**inputs)

#             # 提取最后一层的隐藏状态
#             last_hidden_states = outputs.last_hidden_state
            
#             feature[qid]["nl_features"] = last_hidden_states


#             feature[qid]["table"] = q_tables[queries[qid][1]]["table_array"]

#             # print(tables[tid].columns)
#             inputs = tokenizer(q_tables[queries[qid][1]]["table_array"], return_tensors="tf") 
#             inputs = {k: v for k, v in inputs.items()}
#             # tables[tid]["column_encoding"] = extractVectors(tables[tid], "starmie", "drop_col", "head", "column", 0, False)
#             # 使用模型进行预测
            
#             outputs = model(**inputs)

#             # 提取最后一层的隐藏状态
#             last_hidden_states = outputs.last_hidden_state
            
#             feature[qid]["table_encoding"] = last_hidden_states

#     return feature

def extractVectors(dfs, dataFolder, augment, sample, table_order, run_id, singleCol=False):  #clx：对表进行预测，但是主要是调用inference_on_tables
    ''' Get model inference on tables
    Args:
        dfs (list of DataFrames): tables to get model inference on
        dataFolder (str): benchmark folder name
        augment (str): augmentation operator used in vector file path (e.g. 'drop_cell')
        sample (str): sampling method used in vector file path (e.g. 'head')
        table_order (str): 'column' or 'row' ordered
        run_id (int): used in file path
        singleCol (boolean): is this for single column baseline
    Return:
        list of features for the dataframe
    '''
    if singleCol:
        model_path = "src/table_encoder/starmie/results/%s_model_%s_%s_%s_%dsingleCol.pt" % ("starmie", augment, sample, table_order,run_id)
    else:
        model_path = "src/table_encoder/starmie/results/%s_model_%s_%s_%s_%d.pt" % ("starmie", augment, sample, table_order,run_id)
    ckpt = torch.load(model_path, map_location=torch.device('cuda'))
    # load_checkpoint from sdd/pretain
    model, trainset = load_checkpoint(ckpt)
    return inference_on_table(dfs, model, trainset, batch_size=1024)

