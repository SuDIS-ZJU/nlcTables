import pickle
import mlflow
import argparse
import time
import numpy as np
from hnsw_search import HNSWSearcher
# import tqdm.auto
import csv
import sys
import os
from tqdm import tqdm

import json
import pandas as pd
import math

def calculate_metrics_at_k2(k):
    # 读取 CSV 文件
    groundtruth_df = pd.read_csv('Join1/Deepjoin/all/deepjoin.csv')
    prediction_df = pd.read_csv('Join1/Deepjoin/final_result/deepjoin_hnsw_20_10_0.7.csv')

    # 构建 ground truth 数据结构
    ground_truth = {}
    for _, row in groundtruth_df.iterrows():
        qid, tid, qcol, tcol, rel = row
        key = (qid, qcol)
        if key not in ground_truth:
            ground_truth[key] = []
        ground_truth[key].append(((tid, tcol), rel))

    # 构建 prediction 数据结构
    predictions = {}
    for _, row in prediction_df.iterrows():
        qid, tid, qcol, tcol, rel = row  # 现在包含 rel 值
        key = (qid, qcol)
        if key not in predictions:
            predictions[key] = []
        predictions[key].append(((tid, tcol), rel))

    # 计算所有正例的总数
    total_positives = sum(len([gt for gt in ground_truth[key] if gt[1] == 1]) for key in ground_truth)

    true_positives = 0
    false_positives = 0
    ndcg_sum = 0.0

    for key in ground_truth:
        if key in predictions:
            # 按照 rel 值降序排序预测结果
            sorted_predictions = sorted(predictions[key], key=lambda x: x[1], reverse=True)
            top_k_predictions = sorted_predictions[:k]  # 只取前 k 个预测结果
            
            # 计算 true positives 和 false positives
            for pred in top_k_predictions:
                if any(pred[0] == gt[0] and gt[1] == 1 for gt in ground_truth[key]):
                    true_positives += 1
                else:
                    false_positives += 1

            # 计算 DCG
            relevance_scores = [0] * k
            for i, pred in enumerate(top_k_predictions):
                for gt in ground_truth[key]:
                    if pred[0] == gt[0]:
                        relevance_scores[i] = gt[1]  # 使用 ground truth 的 rel 值
                        break
            
            dcg = sum(relevance_scores[i] / np.log2(i + 2) for i in range(k))  # +2 to avoid log(1)
            
            # 计算 IDCG
            ideal_relevance_scores = sorted([gt[1] for gt in ground_truth[key]], reverse=True)[:k]
            idcg = sum(ideal_relevance_scores[i] / np.log2(i + 2) for i in range(len(ideal_relevance_scores)))

            # 计算 NDCG
            ndcg = dcg / idcg if idcg > 0 else 0
            ndcg_sum += ndcg

    # 计算平均 precision 和 recall at k
    avg_precision = true_positives / (k * len(predictions)) if k > 0 else 0
    avg_recall = true_positives / total_positives if total_positives > 0 else 0
    avg_ndcg = ndcg_sum / len(ground_truth) if len(ground_truth) > 0 else 0

    return avg_precision, avg_recall, avg_ndcg

import pandas as pd
import numpy as np

def calculate_metrics_at_k(k):
    # 读取 CSV 文件
    groundtruth_df = pd.read_csv('Join1/Deepjoin/all/deepjoin.csv')
    prediction_df = pd.read_csv('Join1/Deepjoin/final_result/deepjoin_hnsw_20_10_0.7.csv')

    # 构建 ground truth 数据结构
    ground_truth = {}
    for _, row in groundtruth_df.iterrows():
        qid, tid, qcol, tcol, rel = row
        key = (qid, qcol)
        if key not in ground_truth:
            ground_truth[key] = []
        ground_truth[key].append(((tid, tcol), rel))

    # 构建 prediction 数据结构
    predictions = {}
    for _, row in prediction_df.iterrows():
        qid, tid, qcol, tcol, rel = row  # 现在包含 rel 值
        key = (qid, qcol)
        if key not in predictions:
            predictions[key] = []
        predictions[key].append(((tid, tcol), rel))

    # 计算所有正例的总数
    total_positives = sum(len([gt for gt in ground_truth[key] if gt[1] == 1]) for key in ground_truth)

    true_positives = 0
    false_positives = 0
    ndcg_sum = 0.0

    for key in ground_truth:
        if key in predictions:
            # 获取预测的 top_k 结果
            top_k_predictions = predictions[key][:k]  # 只取前 k 个预测结果
            
            # 计算 true positives 和 false positives
            for pred in top_k_predictions:
                if any(pred[0] == gt[0] and gt[1] == 1 for gt in ground_truth[key]):
                    true_positives += 1
                else:
                    false_positives += 1

            # 计算 DCG
            relevance_scores = [0] * k
            for i, pred in enumerate(top_k_predictions):
                for gt in ground_truth[key]:
                    if pred[0] == gt[0]:
                        relevance_scores[i] = gt[1]  # 使用 ground truth 的 rel 值
                        break
            
            dcg = sum(relevance_scores[i] / np.log2(i + 2) for i in range(k))  # +2 to avoid log(1)
            
            # 计算 IDCG
            ideal_relevance_scores = sorted([gt[1] for gt in ground_truth[key]], reverse=True)[:k]
            idcg = sum(ideal_relevance_scores[i] / np.log2(i + 2) for i in range(len(ideal_relevance_scores)))

            # 计算 NDCG
            ndcg = dcg / idcg if idcg > 0 else 0
            ndcg_sum += ndcg

    # 计算平均 precision 和 recall at k
    avg_precision = true_positives / (k * len(predictions)) if k > 0 else 0
    avg_recall = true_positives / total_positives if total_positives > 0 else 0
    avg_ndcg = ndcg_sum / len(ground_truth) if len(ground_truth) > 0 else 0

    return avg_precision, avg_recall, avg_ndcg


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--encoder", type=str, default="cl", choices=['sherlock', 'starmie', 'cl', 'tapex'])
    parser.add_argument("--benchmark", type=str, default='test')
    parser.add_argument("--run_id", type=int, default=0)
    parser.add_argument("--single_column", dest="single_column", action="store_true", default=False)
    parser.add_argument("--K", type=int, default=60)
    parser.add_argument("--scal", type=float, default=1.0)
    parser.add_argument("--mlflow_tag", type=str, default=None)
    parser.add_argument("--N", type=int, default=10)
    parser.add_argument("--threshold", type=float, default=0.7)

    hp = parser.parse_args()

    start_time = time.time()
    
    # mlflow logging
    for variable in ["encoder", "benchmark", "single_column", "run_id", "K", "scal"]:
        mlflow.log_param(variable, getattr(hp, variable))

    if hp.mlflow_tag:
        mlflow.set_tag("tag", hp.mlflow_tag)

    encoder = hp.encoder
    singleCol = False
    dataFolder = hp.benchmark
    K = hp.K
    threshold = hp.threshold
    N = hp.N

    # Set augmentation operators, sampling methods, K, and threshold values according to the benchmark
    if 'santos' in dataFolder or dataFolder == 'opendata':
        sampAug = "drop_cell_alphaHead"

    elif dataFolder == 'opendata' or dataFolder == 'test':
        sampAug = "drop_cell_alphaHead"
    singSampAug = "drop_col,sample_row_head"

    table_id = hp.run_id
    # table_path = "/data/final_result/starmie/"+dataFolder+"/"+dataFolder+"_small_with_query.pkl"
    # query_path = "/data/final_result/starmie/"+dataFolder+"/"+dataFolder+"_small_query.pkl"
    # index_path = "/data/final_result/starmie/"+dataFolder+"/hnsw_opendata_small_"+str(table_id)+"_"+str(hp.scal)+".bin"

    table_path = "Join1/Deepjoin/final_result/datalake_infer.pkl"
    query_path = "Join1/Deepjoin/final_result/query_infer.pkl"
    index_path = "Join1/Deepjoin/data/infer_datalake"

    # Call HNSWSearcher from hnsw_search.py
    searcher = HNSWSearcher(table_path, index_path, hp.scal)
    print(f"table_path: {table_path}")
    queries = pickle.load(open(query_path, "rb"))

    start_time = time.time()
    returnedResults = {}
    avgNumResults = []
    query_times = []

    dic = {}
    for qu in queries:
        str_q = qu[0].split('__')[0]
        if str_q not in dic:
            dic[str_q] = []
            # dic[str_q].append(qu)
        # else:
            # continue
        dic[str_q].append(qu)


# path_output = '/data/final_result/starmie/'+dataFolder+'/result/small/hnsw_' + str(K) + '_' + str(N) + '_' + str(threshold) + '.csv'
path_output = 'Join1/Deepjoin/final_result/deepjoin_hnsw_' + str(K) + '_' + str(N) + '_' + str(threshold) + '.csv'
if os.path.exists(path_output):
     os.remove(path_output)

for q in tqdm(queries):
    query_start_time = time.time()
    res, scoreLength = searcher.topk(encoder, q, K, N=N, threshold=threshold) #N=10,
    returnedResults[q[0]] = [r[2] for r in res]
    avgNumResults.append(scoreLength)
    query_times.append(time.time() - query_start_time)

    with open(path_output, 'a', encoding='utf-8', newline='') as file_writer:
        for i in range(0,len(res)):
            # out_data = []
            # out_data.append(q[0])
            # out_data.append(res[i][2].split('/')[-1])
            for j in res[i][1]:
                out_data = []
                out_data.append(q[0].replace("query-test-", ""))
                out_data.append(res[i][2].replace("datalake-test-", ""))
                # with open(q[0]+'.csv', 'r') as csvfile:
                #     csvreader = csv.reader(csvfile)
                #     first_row = next(csvreader)
                #     out_data.append(first_row[j[0]])
                print("q[0]",q[0])
                path = "Join1/Deepjoin/all/query-test/" + q[0].replace("query-test-", "")
                print("path",path)
                with open(path, 'r', encoding='utf-8') as file:
                    csvreader = json.load(file)
                    df = pd.DataFrame(csvreader['data'], columns=csvreader['title'])
                    out_data.append(df.columns[j[0]]) 

                if 'query' in res[i][2]:
                    # path = res[i][2] + '.csv'
                    # with open(path, 'r') as csvfile:
                    #     csvreader = csv.reader(csvfile)
                    #     first_row = next(csvreader)
                    #     out_data.append(first_row[j[1]])
                    print("res[i][2]",res[i][2])
                    path = "Join1/Deepjoin/all/datalake-test/" + res[i][2].replace("datalake-test-", "")
                    print("path",path)
                    with open(path, 'r', encoding='utf-8') as file:
                        csvreader = json.load(file)
                        df = pd.DataFrame(csvreader['data'], columns=csvreader['title'])
                        out_data.append(df.columns[j[1]]) 
                else:
                    # path =res[i][2]+'.csv'
                    # with open(path, 'r') as csvfile:                            
                    #     csvreader = csv.reader(csvfile)
                    #     first_row = next(csvreader)
                    #     out_data.append(first_row[j[1]]) 
                    print("res[i][2]",res[i][2])
                    path = "Join1/Deepjoin/all/datalake-test/" + res[i][2].replace("datalake-test-", "")
                    print("path",path)
                    with open(path, 'r', encoding='utf-8') as file:
                        csvreader = json.load(file)
                        df = pd.DataFrame(csvreader['data'], columns=csvreader['title'])
                        out_data.append(df.columns[j[1]]) 
                
                out_data.append(j[2])
                w = csv.writer(file_writer, delimiter=',')
                w.writerow(out_data)


# for q in queries:
#     if len(returnedResults[q[0]]) < K:
#         print(returnedResults[q[0]])

for i in range (1,5):

        k = 5*i # 设置 k 的值
        avg_precision, avg_recall, avg_ndcg = calculate_metrics_at_k(k)

        print(f"Average Precision at {k}: {avg_precision:.4f}")
        print(f"Average Recall at {k}: {avg_recall:.4f}")
        print(f"Average NDCG at {k}: {avg_ndcg:.4f}")

end_time = time.time()
exc_time = end_time - start_time
print("exc_time",exc_time)



