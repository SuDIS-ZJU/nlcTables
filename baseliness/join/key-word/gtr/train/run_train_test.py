import os
import pickle
import random
import datetime
import pandas as pd
from pytorch_transformers import WarmupLinearSchedule

from src.utils.evaluate import write_trec_result, get_metrics
from src.utils.load_data import load_queries, load_datalake_tables, load_query_tables, load_groundtruth
from src.utils.reproducibility import set_random_seed
from src.model.retrieval import MatchingModel
from src.table_encoder.gtr.tabular_graph import TabularGraph
from src.utils.process_table_and_nl import *

queries = None
dl_tables = None
q_tables = None
ground_truth = None

def evaluate(config, model, query_id_list):
    qids = []
    docids = []
    gold_rel = []
    pred_rel = []

    model.eval()
    with torch.no_grad():
        for qid in tqdm(query_id_list):
            query = queries[qid]
            nlquery_feature = queries[qid]["nl_features"].to("cuda")

            if config["baseline-model"] == "GTR" :
                q_table_dgl_graph = queries[qid]["dgl_graph"].to("cuda")
                q_table_node_features = queries[qid]["node_features"].to("cuda")

            for (tid, rel) in ground_truth[qid].items():
                # if tid not in tables:
                #     continue

                table = dl_tables[tid]

                if config["baseline-model"] == "GTR" :
                    dgl_graph = dl_tables[tid]["dgl_graph"].to("cuda")
                    node_features = dl_tables[tid]["node_features"].to("cuda")

                    score = model(table, query, q_table_dgl_graph, q_table_node_features, dgl_graph, node_features, nlquery_feature).item()

                qids.append(qid)
                docids.append(tid)
                gold_rel.append(rel)
                pred_rel.append(score)

    eval_df = pd.DataFrame(data={
        'id_left': qids,
        'id_right': docids,
        'true': gold_rel,
        'pred': pred_rel
    })

    write_trec_result(eval_df)
    metrics = get_metrics('ndcg_cut')
    metrics.update(get_metrics('map'))


        # 对每个 qid 的预测结果进行排序
    eval_df['rank'] = eval_df.groupby('id_left')['pred'].rank(method='first', ascending=False)

    # 计算 precision 和 recall at k
    for k in [5, 10, 15, 20]:  # 例如，我们计算前 5 个结果的 precision 和 recall
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
            print("total_positives",total_positives)
            recall = true_positives / total_positives if total_positives > 0 else 0
            recall_at_k.append(recall)

        # 计算平均 precision 和 recall at k
        avg_precision_at_k = sum(precision_at_k) / len(precision_at_k)
        avg_recall_at_k = sum(recall_at_k) / len(recall_at_k)

        print(f"Average Precision at {k}: {avg_precision_at_k}")
        print(f"Average Recall at {k}: {avg_recall_at_k}")

    return metrics


def train(config, model, train_query_ids, optimizer, scheduler, loss_func):
    random.shuffle(train_query_ids)

    model.train()

    eloss = 0
    batch_loss = 0
    n_iter = 0
    cnt = 0

    pbar = tqdm(train_query_ids)
    for qid in pbar:
        cnt += 1

        query = queries[qid]
        nlquery_feature = queries[qid]["nl_features"].to("cuda")

        if config["baseline-model"] == "GTR" :
            q_table_dgl_graph = queries[qid]["dgl_graph"].to("cuda")
            q_table_node_features = queries[qid]["node_features"].to("cuda")

        logits = []
        label = None
        pos = 0
        for (tid, rel) in ground_truth[qid].items():
            # if tid not in tables:
            #     continue

            if rel == 2:
                label = torch.LongTensor([pos]).to("cuda")

            table = dl_tables[tid]

            if config["baseline-model"] == "GTR" :
                dgl_graph = dl_tables[tid]["dgl_graph"].to("cuda")
                node_features = dl_tables[tid]["node_features"].to("cuda")
            
                logit = model(table, query, q_table_dgl_graph, q_table_node_features, dgl_graph, node_features, nlquery_feature)
            
            logits.append(logit)

            pos += 1

        if label is None or len(logits) < 2:
            print(qid, query)
            continue

        loss = loss_func(torch.cat(logits).view(1, -1), label.view(1))

        batch_loss += loss

        n_iter += 1

        if n_iter % config["batch_size"] == 0 or cnt == len(train_query_ids):
            batch_loss /= config["batch_size"]
            batch_loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)

            optimizer.step()
            scheduler.step()

            pbar.set_postfix(loss=batch_loss.item())

            optimizer.zero_grad()
            batch_loss = 0

        eloss += loss.item()

    return eloss / len(train_query_ids)


def train_and_test(config):
    set_random_seed()

    start_time = time.time()

    global queries, dl_tables, q_tables, ground_truth

    train_queries = load_queries(config["data_dir"], "train_query.txt")
    dev_queries = load_queries(config["data_dir"], "dev_query.txt")
    test_queries = load_queries(config["data_dir"], "test_query.txt")
    dl_tables = load_datalake_tables(config["data_dir"],config["baseline-model"])
    q_tables = load_query_tables(config["data_dir"],config["baseline-model"])
    ground_truth = load_groundtruth(config["data_dir"])

    # remove invalid queries
    train_query_ids = list(train_queries.keys())
    train_query_ids = [x for x in train_query_ids if x in ground_truth]
    dev_query_ids = list(dev_queries.keys())
    dev_query_ids = [x for x in dev_query_ids if x in ground_truth]
    test_query_ids = list(test_queries.keys())
    test_query_ids = [x for x in test_query_ids if x in ground_truth]

    # queries = {}
    # queries["sentence"] = {}
    # queries["sentence"].update(train_queries)
    # queries["sentence"].update(dev_queries)
    # queries["sentence"].update(test_queries)

    queries = {}
    queries.update(train_queries)
    queries.update(dev_queries)
    queries.update(test_queries)

    del train_queries, dev_queries, test_queries

    # ######## clean the dataset ###########
    # invalid_query = []
    # invalid_table = []
    # missing_pos = []
    # for qid in list(queries["sentence"].keys()):
    #     if qid not in qtrels:
    #         del queries["sentence"][qid]
    #         invalid_query.append(qid)
    #     elif qid not in tables:
    #         del queries["sentence"][qid]
    #         invalid_table.append(qid)
    #     elif qid not in qtrels[qid]:
    #         qtrels[qid][qid] = 1
    #         missing_pos.append(qid)
    # print("invalid_query", len(invalid_query), invalid_query)
    # print("invalid_table", len(invalid_table), invalid_table)
    # print("missing_pos", len(missing_pos), missing_pos)

    # missing_tables = []
    # for qid in list(queries["sentence"].keys()):
    #     for tid in list(qtrels[qid].keys()):
    #         if tid not in tables:
    #             del qtrels[qid][tid]
    #             missing_tables.append(tid)
    # print("missing_tables", len(missing_tables), missing_tables)

    # valid_tables = set()
    # for qid in qtrels.keys():
    #     for tid in qtrels[qid].keys():
    #         valid_tables.add(tid)
    # for tid in list(tables.keys()):
    #     if tid not in valid_tables:
    #         del tables[tid]

    # new_ids = []
    # for qid in train_query_ids:
    #     if qid in queries["sentence"]:
    #         new_ids.append(qid)
    # print("missing train queries", len(train_query_ids) - len(new_ids))
    # train_query_ids = new_ids

    # new_ids = []
    # for qid in dev_query_ids:
    #     if qid in queries["sentence"]:
    #         new_ids.append(qid)
    # print("missing dev queries", len(dev_query_ids) - len(new_ids))
    # dev_query_ids = new_ids

    # new_ids = []
    # for qid in test_query_ids:
    #     if qid in queries["sentence"]:
    #         new_ids.append(qid)
    # print("missing test queries", len(test_query_ids) - len(new_ids))
    # test_query_ids = new_ids
    # #######################################

    # constructor = TabularGraph(config["use_fasttext"], config["fasttext"])
    
    if config["baseline-model"] == "GTR" :
        query_emb_path = '/home/clx/home/code/nl_table_retrieval/src/table_encoder/gtr/query-emb.pickle'
        dltable_emb_path = '/home/clx/home/code/nl_table_retrieval/src/table_encoder/gtr/dltable-emb.pickle'
    
    if os.path.exists(query_emb_path):
        with open(query_emb_path, 'rb') as f:
            queries = pickle.load(f)
        print(f"Loaded existing queries from {query_emb_path}")
    else:
        # 如果文件不存在，处理 queries
        # queries = process_queries(queries, q_tables, constructor)
        queries = process_queries(queries, q_tables, config)

        # 保存处理后的 queries 到 query-emb.pickle
        with open(query_emb_path, 'wb') as f:
            pickle.dump(queries, f)
        print(f"Processed and saved new queries to {query_emb_path}")

    if os.path.exists(dltable_emb_path):
        with open(dltable_emb_path, 'rb') as f:
            dl_tables = pickle.load(f)
        print(f"Loaded existing tables from {dltable_emb_path}")
    else:
        # 如果文件不存在，保存处理后的 dl_tables 到 dltable-emb.pickle
        process_tables(dl_tables, config)
        with open(dltable_emb_path, 'wb') as f:
            pickle.dump(dl_tables, f)
        print(f"Saved new tables to {dltable_emb_path}")

    model = MatchingModel(bert_dir=config["bert_dir"], do_lower_case=config["do_lower_case"],
                          bert_size=config["bert_size"], gnn_output_size=config["gnn_size"])

    if config["use_pretrained_model"]:
        pretrain_model = torch.load(config["pretrained_model_path"])
        model.load_state_dict(pretrain_model, strict=False)

    model = model.to("cuda")

    print(config)
    print(model, flush=True)

    loss_func = torch.nn.CrossEntropyLoss()

    optimizer = torch.optim.Adam([
        {'params': model.bert.parameters(), 'lr': config["bert_lr"]},
        {'params': (x for x in model.parameters() if x not in set(model.bert.parameters())), 'lr': config["gnn_lr"]}
    ])
    scheduler = WarmupLinearSchedule(optimizer, warmup_steps=config["warmup_steps"], t_total=config["total_steps"])

    best_metrics = None

    for epoch in range(config['epoch']):
        eloss = train(config, model, train_query_ids, optimizer, scheduler, loss_func)
        dev_metrics = evaluate(config, model, dev_query_ids)

        if best_metrics is None or dev_metrics[config['key_metric']] > best_metrics[config['key_metric']]:
            best_metrics = dev_metrics
            test_metrics = evaluate(config, model, test_query_ids)
            print(datetime.datetime.now(), 'epoch', epoch, 'train loss', eloss, 'dev', dev_metrics, 'test',
                  test_metrics, "*", flush=True)
            with open('santos_results.txt', 'a') as file:
                # 将字典转换为字符串
                data_dir = config["data_dir"]
                metrics_str = f"gtr_{data_dir} \n epoch {epoch} train loss {eloss} dev {dev_metrics} test {test_metrics} *"
                # 写入字符串到文件，并添加换行符
                file.write(metrics_str + '\n\n\n')
        else:
            print(datetime.datetime.now(), 'epoch', epoch, 'train loss', eloss, 'dev', dev_metrics, flush=True)
            with open('santos_results.txt', 'a') as file:
                # 将字典转换为字符串
                data_dir = config["data_dir"]
                metrics_str = f"gtr_{data_dir} \n epoch {epoch} train loss {eloss} dev {dev_metrics}"
                # 写入字符串到文件，并添加换行符
                file.write(metrics_str + '\n\n\n')
    
    # 记录结束时间
    end_time = time.time()

    # 计算执行时间
    execution_time = end_time - start_time
    print("execution_time",execution_time)
    with open('santos_results.txt', 'a') as file:
                # 将字典转换为字符串
                data_dir = config["data_dir"]
                metrics_str = f"gtr_{data_dir} execution_time {execution_time}"
                # 写入字符串到文件，并添加换行符
                file.write(metrics_str + '\n\n\n')
