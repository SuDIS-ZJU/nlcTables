import pickle
import pickle5 as p
import pandas as pd
from matplotlib import *
from matplotlib import pyplot as plt
import numpy as np
import mlflow
import subprocess

from load_data import load_queries, load_datalake_tables, load_query_tables, load_groundtruth

def loadDictionaryFromPickleFile(dictionaryPath):
    ''' Load the pickle file as a dictionary
    Args:
        dictionaryPath: path to the pickle file
    Return: dictionary from the pickle file
    '''
    filePointer=open(dictionaryPath, 'rb')
    dictionary = p.load(filePointer)
    filePointer.close()
    return dictionary

def saveDictionaryAsPickleFile(dictionary, dictionaryPath):
    ''' Save dictionary as a pickle file
    Args:
        dictionary to be saved
        dictionaryPath: filepath to which the dictionary will be saved
    '''
    filePointer=open(dictionaryPath, 'wb')
    pickle.dump(dictionary,filePointer, protocol=pickle.HIGHEST_PROTOCOL)
    filePointer.close()



def write_trec_result(eval_df, rank_path='src/table_encoder/starmie_ori/trec_rank.txt', qrel_path='src/table_encoder/starmie_ori/trec_qrel.txt'):
    with open(rank_path, 'w') as f_rank, open(qrel_path, 'w') as f_rel:
        qids = [each for each in sorted(list(set([each for each in eval_df['id_left'].unique()])))]
        for qid in qids:
            qid_docs = eval_df[eval_df['id_left'] == qid]
            qid_docs = qid_docs.sort_values(by=['pred'], ascending=False)
            for r, value in enumerate(qid_docs.values):
                f_rank.write(f"{value[0]}\tQ0\t{value[1]}\t{r+1}\t{value[3]}\t0\n")
                f_rel.write(f"{value[0]}\t0\t{value[1]}\t{value[2]}\n")


def get_metrics(metric='ndcg_cut', rank_path='src/table_encoder/starmie_ori/trec_rank.txt', qrel_path='src/table_encoder/starmie_ori/trec_qrel.txt'):
    if metric == 'ndcg_cut':
        metrics = ['ndcg_cut_5', 'ndcg_cut_10', 'ndcg_cut_15', 'ndcg_cut_20']
    elif metric == 'map':
        metrics = ['map']
    else:
        raise ValueError(f"Invalid metric {metric}.")

    results = subprocess.run(['fastText/trec_eval/trec_eval', '-c', '-m', metric, '-q', qrel_path, rank_path],
                             stdout=subprocess.PIPE).stdout.decode('utf-8')

    ndcg_scores = dict()
    for line in results.strip().split("\n"):
        seps = line.split('\t')
        metric_name = seps[0].strip()
        qid = seps[1].strip()
        if metric_name not in metrics or qid != 'all':
            continue
        ndcg_scores[seps[0].strip()] = float(seps[2])
    return ndcg_scores


def calcMetrics(resultFile, gtPath):
    qids = []
    docids = []
    gold_rel = []
    pred_rel = []
        
    queries= load_queries(gtPath)
    # q_tables = load_query_tables("data_dir")
    # dl_tables = load_datalake_tables("data_dir")
    ground_truth = load_groundtruth(gtPath)
    found_one = 0
    for qid in queries.keys():
        for tid, rel in ground_truth[qid].items():
            qids.append(qid)
            docids.append(tid)
            gold_rel.append(rel)
            # print(resultFile)
            # print(qid)
            # print(queries[qid][1])
            # print(tid)
            # print(queries[qid])
            query_key = f"{queries[qid][1]}"
            table_key = f"{tid}"
            # 遍历 resultFile 列表中的每个 scores 字典
            # print(resultFile)
            # print("resultFile:",len(resultFile))
            found_value = None
            for scores_dict in resultFile:
                # 检查 query_key 是否存在于当前字典中
                # print(scores_dict)
                if query_key in scores_dict:
                    # 如果存在，再检查 table_key 是否存在于对应的子字典中
                    if table_key in scores_dict[query_key]:
                        # 如果 table_key 也存在，存储找到的值
                        found_value = scores_dict[query_key][table_key]
                        # pred_rel.append(found_value)
                        # found_one+=1
                        break  # 如果您只想要找到第一个匹配的值，找到后就可以停止循环
            print(found_value)
            if not found_value:
                print(query_key)
                print(table_key)
            pred_rel.append(found_value)
            # if not found_value:
            #     pred_rel.append(0)
            # if query_key in resultFile:
            #     # 如果存在，再检查 table_key 是否存在于对应的子字典中
            #     if table_key in resultFile[query_key]:
            #         # 如果 table_key 也存在，存储找到的值
            #         found_value = resultFile[query_key][table_key]
            #         pred_rel.append(found_value)
            #         break  # 如果您只想要找到第一个匹配的值，找到后就可以停止循环

    # print("id_left:",len(qids))
    # print("id_right:",len(docids))
    # print("true:",len(gold_rel))
    print("*****starmie-pred_rel:",pred_rel)    
    # print("found_one:",found_one) 

    eval_df = pd.DataFrame(data={
        'id_left': qids,
        'id_right': docids,
        'true': gold_rel,
        'pred': pred_rel
    })

    write_trec_result(eval_df)
    metrics = get_metrics('ndcg_cut')
    metrics.update(get_metrics('map'))

    print(metrics)

    # 对每个 qid 的预测结果进行排序
    eval_df['rank'] = eval_df.groupby('id_left')['pred'].rank(method='first', ascending=False)

    # 计算 precision 和 recall at k
    # k = 5  # 例如，我们计算前 5 个结果的 precision 和 recall
    for k in [5, 10, 15, 20]:
        precision_at_k = []
        recall_at_k = []

        for qid in eval_df['id_left'].unique():
            # 获取该 qid 的所有结果
            qid_results = eval_df[eval_df['id_left'] == qid]
            
            # 获取前 k 个结果
            top_k_results = qid_results[qid_results['rank'] <= k]
            
            # 计算前 k 个结果中正例的数量
            true_positives = top_k_results['true'].sum()/2
            
            # 计算 precision at k
            precision = true_positives / k
            precision_at_k.append(precision)
            
            # 计算 recall at k
            total_positives = qid_results['true'].sum()/2
            # print("total_positives",total_positives)
            recall = true_positives / total_positives if total_positives > 0 else 0
            recall_at_k.append(recall)

        # 计算平均 precision 和 recall at k
        avg_precision_at_k = sum(precision_at_k) / len(precision_at_k)
        avg_recall_at_k = sum(recall_at_k) / len(recall_at_k)

        print(f"Average Precision at {k}: {avg_precision_at_k}")
        print(f"Average Recall at {k}: {avg_recall_at_k}")
    # if k % 10 == 0:
#             print(k, "IDEAL RECALL:", sum(ideal_recall)/len(ideal_recall))
#     used_k = [k_range]
#     if max_k >k_range:
#         for i in range(k_range * 2, max_k+1, k_range):
#             used_k.append(i)
#     print("--------------------------")
#     for k in used_k:
#         print("Precision at k = ",k,"=", precision_array[k-1])
#         print("Recall at k = ",k,"=", recall_array[k-1])
#         print("--------------------------")
    
#     map_sum = 0
#     for k in range(0, max_k):
#         map_sum += precision_array[k]
#     mean_avg_pr = map_sum/max_k
#     print("The mean average precision is:", mean_avg_pr)


    return metrics
# def calcMetrics(max_k, k_range, resultFile, gtPath=None, resPath=None, record=True):
#     ''' Calculate and log the performance metrics: MAP, Precision@k, Recall@k
#     Args:
#         max_k: the maximum K value (e.g. for SANTOS benchmark, max_k = 10. For TUS benchmark, max_k = 60)
#         k_range: step size for the K's up to max_k
#         gtPath: file path to the groundtruth
#         resPath: file path to the raw results from the model
#         record (boolean): to log in MLFlow or not
#     Return: MAP, P@K, R@K
#     '''
#     groundtruth = loadDictionaryFromPickleFile(gtPath)
#     # resultFile = loadDictionaryFromPickleFile(resPath)
        
#     # =============================================================================
#     # Precision and recall
#     # =============================================================================
#     precision_array = []
#     recall_array = []
#     for k in range(1, max_k+1):
#         true_positive = 0
#         false_positive = 0
#         false_negative = 0
#         rec = 0
#         ideal_recall = []
#         for table in resultFile:
#             # t28 tables have less than 60 results. So, skipping them in the analysis.
#             if table.split("____",1)[0] != "t_28dc8f7610402ea7": 
#                 if table in groundtruth:
#                     groundtruth_set = set(groundtruth[table])
#                     groundtruth_set = {x.split(".")[0] for x in groundtruth_set}
#                     result_set = resultFile[table][:k]
#                     result_set = [x.split(".")[0] for x in result_set]
#                     # find_intersection = true positives
#                     find_intersection = set(result_set).intersection(groundtruth_set)
#                     tp = len(find_intersection)
#                     fp = k - tp
#                     fn = len(groundtruth_set) - tp
#                     if len(groundtruth_set)>=k: 
#                         true_positive += tp
#                         false_positive += fp
#                         false_negative += fn
#                     rec += tp / (tp+fn)
#                     ideal_recall.append(k/len(groundtruth[table]))
#         precision = true_positive / (true_positive + false_positive)
#         recall = rec/len(resultFile)
#         precision_array.append(precision)
#         recall_array.append(recall)
#         if k % 10 == 0:
#             print(k, "IDEAL RECALL:", sum(ideal_recall)/len(ideal_recall))
#     used_k = [k_range]
#     if max_k >k_range:
#         for i in range(k_range * 2, max_k+1, k_range):
#             used_k.append(i)
#     print("--------------------------")
#     for k in used_k:
#         print("Precision at k = ",k,"=", precision_array[k-1])
#         print("Recall at k = ",k,"=", recall_array[k-1])
#         print("--------------------------")
    
#     map_sum = 0
#     for k in range(0, max_k):
#         map_sum += precision_array[k]
#     mean_avg_pr = map_sum/max_k
#     print("The mean average precision is:", mean_avg_pr)

#     # logging to mlflow
#     if record: # if the user would like to log to MLFlow
#         mlflow.log_metric("mean_avg_precision", mean_avg_pr)
#         mlflow.log_metric("prec_k", precision_array[max_k-1])
#         mlflow.log_metric("recall_k", recall_array[max_k-1])

#     return mean_avg_pr, precision_array[max_k-1], recall_array[max_k-1] 