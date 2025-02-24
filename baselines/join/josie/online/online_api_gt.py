
import math
import time
from tqdm import tqdm
import pandas as pd
import json
from data_process import *
from josie import *
import pickle
from heap import *
import os
import csv



def api(qpath: str,
        save_root: str, 
        result_root: str,
        k:int):
    
    # 读取文件
    with open(os.path.join(save_root, "setMap.pkl"), "rb") as tf:
        setMap = pickle.load(tf)
    outpath = os.path.join(save_root, "outputs")
    tf = open(os.path.join(outpath, "integerSet.json") , "r")
    integerSet = json.load(tf)
    tf = open(os.path.join(outpath, "PLs.json") , "r")
    PLs = json.load(tf)
    print("PLs")
    tf = open(os.path.join(outpath, "rawDict.json"), "r")
    rawDict= json.load(tf)
    print("rawDict")
    tf.close()
    table_names = os.listdir(qpath)
    
    print("load suc!")

    durs = []
    res=[]
    i=0

    result_dict = {}

    # 读取 CSV 文件并遍历每一行
    with open("Join1/Deepjoin/all/deepjoin.csv", 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # 如果有表头，跳过表头
        for row in csv_reader:
            # 提取列值
            qtable = row[0]  # 第一列为 qtable
            dltable = row[1]  # 第二列为 dltable
            qcol = row[2]    # 第三列为 qcol
            dlcol = row[3]   # 第四列为 dlcol
            gt_rel = row[4]  # 第五列为 gt_rel

            # 创建键 qtable_qcol
            key = (qtable, qcol)

            # 如果键不存在于字典中，添加键并初始化一个空列表
            if key not in result_dict:
                result_dict[key] = []

            # 将 (dltable_dlcol, gt_rel) 元组添加到列表中
            result_dict[key].append(((dltable, dlcol), gt_rel))

    # with open("Join1/Deepjoin/all/deepjoin.csv", 'r', encoding='utf-8') as csv_file:
    #     csv_reader = csv.reader(csv_file)
    #     # next(csv_reader)  # 如果有表头，跳过表头
    #     for row in csv_reader:
    #         # 假设 CSV 文件的列顺序是：qtable, dltable, qcol, dlcol, gt_rel
    #         qtable = row[0]  # 第一列为 qtable
    #         dltable = row[1]  # 第二列为 dltable
    #         qcol = row[2]    # 第三列为 qcol
    #         dlcol = row[3]   # 第四列为 dlcol
    #         gt_rel = row[4]  # 第五列为 gt_rel

    for key, value_list in result_dict.items():
        qtable, qcol = key

        # 遍历列表中的每个元组
        for value in value_list:
            dl, gt_rel = value  # 提取 dltable, dlcol 和 gt_rel
            dltable, dlcol = dl
            print(f"  - dltable: {dltable}, dlcol: {dlcol}, gt_rel: {gt_rel}")

    # for table_name in tqdm(table_names):
        i+=1
        # try:
        ignore=False
        query_ID = -1
        table_path = os.path.join(qpath, qtable)

        # df = pd.read_csv(table_path)
        with open(table_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # 将数据转换为 DataFrame
        # 假设数据在 'data' 键下，并且 'title' 键包含列名
        print("******************table_path",table_path)
        df = pd.DataFrame(data['data'], columns=data['title'])

        # for column_name in df.columns:
        query_ID=readQueryID(setMap,qtable,qcol)
        if  query_ID>0:
            ignore = True
        raw_tokens = list(set(df[qcol].tolist()))
        t1 = time.time()
        result=searchMergeProbeCostModelGreedy(integerSet, PLs, raw_tokens, rawDict, setMap, k, ignore,query_ID)
        print(result)
        t2 = time.time()
        if result!=0:
            dur = (t2 - t1)
            durs.append(dur) 
            # print(f"在线处理一个query时间：{dur:.2f}秒,query长度：{len(raw_tokens)}")            
            for x in result:
                # print(table_name,setMap[x+1]["table_name"],column_name,setMap[x+1]["column_name"])
                re=[qtable,setMap[x+1]["table_name"],qcol,setMap[x+1]["column_name"]]
                res.append(re)

    mean = np.mean(durs)
    print(f"平均时间：{mean:.2f}秒")
    df_end = pd.DataFrame(res, columns=['query_table','candidate_table','query_column','candidate_column'])
    result_path=os.path.join(result_root, "join_top"+str(k)+".csv")
    # df_end.to_csv(result_path, index=False)
    try:
        df_end.to_csv(result_path, index=False)
        print("写入成功！")
    except Exception as e:
        print(f"写入失败: {e}")

    for i in range (1,5):

        k = 5*i # 设置 k 的值
        avg_precision, avg_recall, avg_ndcg = calculate_metrics_at_k(k)

        print(f"Average Precision at {k}: {avg_precision:.4f}")
        print(f"Average Recall at {k}: {avg_recall:.4f}")
        print(f"Average NDCG at {k}: {avg_ndcg:.4f}")







# 计算 precision 和 recall at k
def calculate_precision_recall_at_k(k):
    # 读取 CSV 文件
    groundtruth_df = pd.read_csv('Join1/Deepjoin/all/deepjoin.csv')
    prediction_df = pd.read_csv('Join1/Joise/final_result/join_top20.csv')

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
        qid, tid, qcol, tcol = row
        key = (qid, qcol)
        if key not in predictions:
            predictions[key] = []
        predictions[key].append((tid, tcol))

    # 计算所有正例的总数
    total_positives = sum(len([gt for gt in ground_truth[key] if gt[1] == 1]) for key in ground_truth)

    true_positives = 0
    false_positives = 0

    for key in ground_truth:
        if key in predictions:
            # 获取预测的 top_k 结果
            top_k_predictions = predictions[key][:k]  # 只取前 k 个预测结果
            
            # 计算 true positives 和 false positives
            for pred in top_k_predictions:
                if any(pred == gt[0] and gt[1] == 1 for gt in ground_truth[key]):
                    true_positives += 1
                else:
                    false_positives += 1

    # 计算平均 precision 和 recall at k
    avg_precision = true_positives / (k * len(predictions)) if k > 0 else 0
    avg_recall = true_positives / total_positives if total_positives > 0 else 0

    return avg_precision, avg_recall



def calculate_precision_recall_ndcg_at_k(k):
    # 读取 CSV 文件
    groundtruth_df = pd.read_csv('Join1/Deepjoin/all/deepjoin.csv')
    prediction_df = pd.read_csv('Join1/Joise/final_result/join_top20.csv')

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
        qid, tid, qcol, tcol = row
        key = (qid, qcol)
        if key not in predictions:
            predictions[key] = []
        predictions[key].append((tid, tcol))

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
                if any(pred == gt[0] and gt[1] == 1 for gt in ground_truth[key]):
                    true_positives += 1
                else:
                    false_positives += 1

            # 计算 DCG
            relevance_scores = [0] * k
            for i, pred in enumerate(top_k_predictions):
                for gt in ground_truth[key]:
                    if pred == gt[0]:
                        relevance_scores[i] = gt[1]  # 1 for relevant, 0 for irrelevant
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


def calculate_metrics_at_k(k):
    # 读取CSV文件
    groundtruth_df = pd.read_csv('Join1/Deepjoin/all/deepjoin.csv')
    prediction_df = pd.read_csv('Join1/Joise/final_result/join_top20.csv')

    # 构建ground truth数据结构
    ground_truth = {}
    for _, row in groundtruth_df.iterrows():
        qid, tid, qcol, tcol, rel = row
        key = (qid, qcol)
        if key not in ground_truth:
            ground_truth[key] = []
        ground_truth[key].append(((tid, tcol), rel))

    # 构建prediction数据结构
    predictions = {}
    for _, row in prediction_df.iterrows():
        qid, tid, qcol, tcol = row
        key = (qid, qcol)
        if key not in predictions:
            predictions[key] = []
        predictions[key].append((tid, tcol))

    # 初始化指标
    true_positives = 0
    total_positives = sum(len([gt for gt in items if gt[1] == 1]) for items in ground_truth.values())
    ndcg_sum = 0.0
    total_queries = len(ground_truth)

    # 遍历每个查询计算指标
    for key in ground_truth:
        # 处理当前查询的预测结果
        pred_list = predictions.get(key, [])[:k]
        relevant_items = [item for item, rel in ground_truth[key] if rel == 1]
        relevant_set = set(relevant_items)
        num_relevant = len(relevant_items)

        # 计算IDCG
        idcg = 0.0
        for i in range(min(k, num_relevant)):
            idcg += 1.0 / math.log2(i + 2)  # 位置从1开始，i+2对应log2(i+1+1)

        # 计算DCG
        dcg = 0.0
        for i, pred in enumerate(pred_list):
            rel = 1 if pred in relevant_set else 0
            dcg += rel / math.log2(i + 2)  # 位置从1开始，i+2对应log2(i+1+1)

        # 计算NDCG
        ndcg = dcg / idcg if idcg > 0 else 0.0
        ndcg_sum += ndcg

        # 计算True Positives用于Precision和Recall
        if key in predictions:
            for pred in predictions[key][:k]:
                if any(pred == gt[0] and gt[1] == 1 for gt in ground_truth[key]):
                    true_positives += 1

    # 计算平均指标
    avg_precision = true_positives / (k * len(predictions)) if k * len(predictions) > 0 else 0
    avg_recall = true_positives / total_positives if total_positives > 0 else 0
    avg_ndcg = ndcg_sum / total_queries if total_queries > 0 else 0

    return avg_precision, avg_recall, avg_ndcg