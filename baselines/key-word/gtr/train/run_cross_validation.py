import os
import pickle
import random
import datetime
import pandas as pd
from sklearn.model_selection import ShuffleSplit
from pytorch_transformers import WarmupLinearSchedule

from src.utils.process_table_and_nl import *
from src.utils.reproducibility import set_random_seed
from src.utils.load_data import load_queries, load_datalake_tables, load_query_tables, load_groundtruth
from src.utils.evaluate import write_trec_result, get_metrics
from src.model.retrieval import MatchingModel
from src.table_encoder.starmie.retrieval import MatchingModel_starmie
from src.table_encoder.tapas.retrieval import MatchingModel_tapas


# from src.table_encoder.gtr.utils.process_table_and_query import *

from peft import get_peft_model, LoraConfig

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
        for qid in query_id_list:
            query = queries[qid]
            nlquery_feature = queries[qid]["nl_features"].to("cuda")

            if config["baseline-model"] == "GTR" :
                q_table_dgl_graph = queries[qid]["dgl_graph"].to("cuda")
                q_table_node_features = queries[qid]["node_features"].to("cuda")

            if config["baseline-model"] == "TaBERT" :
                nlquery = queries[qid][0]
                tablequery = q_tables[queries[qid][0]]

            if config["baseline-model"] == "starmie" :
                # q_table_dgl_graph = queries[qid]["dgl_graph"].to("cuda")
                q_col_features = torch.tensor(queries[qid]["column_encoding"]).to("cuda")
            
            if config["baseline-model"] == "tapas" :
                # q_table_dgl_graph = queries[qid]["dgl_graph"].to("cuda")
                q_table_features = torch.tensor(queries[qid]["table_encoding"]).to("cuda")    

            for (tid, rel) in ground_truth[qid].items():
                table = dl_tables[tid]

                if config["baseline-model"] == "GTR" :
                    dgl_graph = dl_tables[tid]["dgl_graph"].to("cuda")
                    node_features = dl_tables[tid]["node_features"].to("cuda")

                    score = model(table, query, q_table_dgl_graph, q_table_node_features, dgl_graph, node_features, nlquery_feature).item()

                if config["baseline-model"] == "TaBERT" :
                    score = model(table, nlquery, tablequery)
                
                if config["baseline-model"] == "starmie" :
                    # dgl_graph = dl_tables[tid]["dgl_graph"].to("cuda")
                    dl_col_features = torch.tensor(dl_tables[tid]["column_encoding"]).to("cuda")

                    score = model(table, query, q_col_features, dl_col_features, nlquery_feature).item()

                if config["baseline-model"] == "tapas" :
                    # dgl_graph = dl_tables[tid]["dgl_graph"].to("cuda")
                    dl_table_features = torch.tensor(dl_tables[tid]["table_encoding"]).to("cuda")

                    score = model(table, query, q_table_features, dl_table_features, nlquery_feature).item()

                qids.append(qid)
                docids.append(tid)
                gold_rel.append(rel)
                pred_rel.append(score * config["relevance_score_scale"])

                print("*****pred_rel:",pred_rel)

                torch.cuda.empty_cache()

    eval_df = pd.DataFrame(data={
        'id_left': qids,
        'id_right': docids,
        'true': gold_rel,
        'pred': pred_rel
    })

    write_trec_result(eval_df)
    metrics = get_metrics('ndcg_cut')
    metrics.update(get_metrics('map'))
    return metrics


def train(config, model, train_pairs, optimizer, scheduler, loss_func):
    random.shuffle(train_pairs)

    model.train()

    eloss = 0
    batch_loss = 0
    n_iter = 0
    for (qid, tid, rel) in train_pairs:
        n_iter += 1

        label = rel * 1.0 / config["relevance_score_scale"]

        query = queries[qid]

        if config["baseline-model"] == "GTR" :
            nlquery_feature = queries[qid]["nl_features"].to("cuda")
            q_table_dgl_graph = queries[qid]["dgl_graph"].to("cuda")
            q_table_node_features = queries[qid]["node_features"].to("cuda")

        if config["baseline-model"] == "TaBERT" :
            nlquery = queries[qid][0]
            tablequery = q_tables[queries[qid][0]]

        if config["baseline-model"] == "starmie" :
            nlquery_feature = queries[qid]["nl_features"].to("cuda")
            # q_table_dgl_graph = queries[qid]["dgl_graph"].to("cuda")
            q_col_features = torch.tensor(queries[qid]["column_encoding"]).to("cuda")

        if config["baseline-model"] == "tapas" :
            nlquery_feature = queries[qid]["nl_features"].to("cuda")
            # q_table_dgl_graph = queries[qid]["dgl_graph"].to("cuda")
            q_table_features = torch.tensor(queries[qid]["table_encoding"]).to("cuda")    

        table = dl_tables[tid]

        if config["baseline-model"] == "GTR" :
            print(dl_tables[tid]["dgl_graph"])
            dgl_graph = dl_tables[tid]["dgl_graph"].to("cuda")
            node_features = dl_tables[tid]["node_features"].to("cuda")
            print(q_table_dgl_graph)
            print(dgl_graph)
            prob = model(table, query, q_table_dgl_graph, q_table_node_features, dgl_graph, node_features, nlquery_feature)

        if config["baseline-model"] == "TaBERT" :
            
            prob = model(table, nlquery, tablequery)

        
        if config["baseline-model"] == "starmie" :
            # dgl_graph = dl_tables[tid]["dgl_graph"].to("cuda")
            dl_col_features = torch.tensor(dl_tables[tid]["column_encoding"]).to("cuda")

            prob = model(table, query, q_col_features, dl_col_features, nlquery_feature)

        if config["baseline-model"] == "tapas" :
            # dgl_graph = dl_tables[tid]["dgl_graph"].to("cuda")
            dl_table_features = torch.tensor(dl_tables[tid]["table_encoding"]).to("cuda")

            prob = model(table, query, q_table_features, dl_table_features, nlquery_feature).item()

        loss = loss_func(prob.reshape(-1), torch.FloatTensor([label]).to("cuda"))

        batch_loss += loss

        if n_iter % config["batch_size"] == 0 or n_iter == len(train_pairs):
            batch_loss /= config["batch_size"]
            batch_loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)

            optimizer.step()
            scheduler.step()

            optimizer.zero_grad()
            batch_loss = 0

            torch.cuda.empty_cache()

        eloss += loss.item()

    return eloss / len(train_pairs)


def cross_validation(config):
    set_random_seed()

    # The Answer to the Ultimate Question of Life, the Universe, and Everything is 42
    # -- The Hitchhiker's Guide to the Galaxy
    ss = ShuffleSplit(n_splits=5, train_size=0.8, random_state=42)

    global queries, dl_tables, q_tables, ground_truth
    queries = {}
    queries= load_queries(config["data_dir"])
    dl_tables = load_datalake_tables(config["data_dir"], config["baseline-model"])
    q_tables = load_query_tables(config["data_dir"], config["baseline-model"])
    ground_truth = load_groundtruth(config["data_dir"])

    if config["baseline-model"] == "GTR" :

        model = MatchingModel(bert_dir=config["bert_dir"], do_lower_case=config["do_lower_case"],
                          bert_size=config["bert_size"], gnn_output_size=config["gnn_size"])
        
        total_params = sum(p.numel() for p in model.parameters())
        print(f"Total parameters: {total_params}")
    
        lora_config = LoraConfig(
            r=16,  # LoRA 的秩
            lora_alpha=32,  # LoRA 的 alpha
            lora_dropout=0.1,  # LoRA 的 dropout
            task_type='SEQ_2_SEQ_LM'  # 任务类型，根据你的任务调整
        )

        try:
            model = get_peft_model(model, lora_config)
        except Exception as e:
            print(f"Error in applying LoRA: {e}") 
        
        query_emb_path = '/home/clx/home/code/nl_table_retrieval/src/table_encoder/gtr/query-emb.pickle'
        dltable_emb_path = '/home/clx/home/code/nl_table_retrieval/src/table_encoder/gtr/dltable-emb.pickle'

        start_time = time.time()

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

        end_time = time.time()
        execution_time = end_time - start_time
        with open("src/table_encoder/gtr/result.txt", "a") as file:
            file.write(f"{config['data_dir']} Offline time: {execution_time:.4f} seconds\n")

    if config["baseline-model"] == "TaBERT" :

        model = MatchingModel(bert_dir=config["bert_dir"], do_lower_case=config["do_lower_case"],
                          bert_size=config["bert_size"], gnn_output_size=config["gnn_size"])
        
        lora_config = LoraConfig(
            r=16,  # LoRA 的秩
            lora_alpha=32,  # LoRA 的 alpha
            lora_dropout=0.1,  # LoRA 的 dropout
            task_type='SEQ_2_SEQ_LM'  # 任务类型，根据你的任务调整
        )

        try:
            model = get_peft_model(model, lora_config)
        except Exception as e:
            print(f"Error in applying LoRA: {e}") 
    
    if config["baseline-model"] == "starmie" :

        model = MatchingModel_starmie(bert_dir=config["bert_dir"], do_lower_case=config["do_lower_case"],
                              bert_size=config["bert_size"])
        
        lora_config = LoraConfig(
            r=16,  # LoRA 的秩
            lora_alpha=32,  # LoRA 的 alpha
            lora_dropout=0.1,  # LoRA 的 dropout
            task_type='SEQ_2_SEQ_LM'  # 任务类型，根据你的任务调整
        )

        try:
            model = get_peft_model(model, lora_config)
        except Exception as e:
            print(f"Error in applying LoRA: {e}") 

        query_emb_path = 'src/table_encoder/starmie/vectors/cl_query-test_drop_col_head_column_0.pkl'
        dltable_emb_path = 'src/table_encoder/starmie/vectors/cl_datalake-test_drop_col_head_column_0.pkl'

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
            dl_tables = process_tables_starmie(dl_tables, config)
            with open(dltable_emb_path, 'wb') as f:
                pickle.dump(dl_tables, f)
            print(f"Saved new tables to {dltable_emb_path}")

    if config["baseline-model"] == "tapas" :

        model = MatchingModel_tapas(bert_dir=config["bert_dir"], do_lower_case=config["do_lower_case"],
                              bert_size=config["bert_size"])
        
        lora_config = LoraConfig(
            r=16,  # LoRA 的秩
            lora_alpha=32,  # LoRA 的 alpha
            lora_dropout=0.1,  # LoRA 的 dropout
            task_type='SEQ_2_SEQ_LM'  # 任务类型，根据你的任务调整
        )

        try:
            model = get_peft_model(model, lora_config)
        except Exception as e:
            print(f"Error in applying LoRA: {e}") 

        query_emb_path = 'src/table_encoder/tapas/vectors/nl_query.pkl'
        dltable_emb_path = 'src/table_encoder/tapas/vectors/datalake_tables.pkl'

        if os.path.exists(query_emb_path):
            with open(query_emb_path, 'rb') as f:
                queries = pickle.load(f)
            print(f"Loaded existing queries from {query_emb_path}")
        else:
        # 如果文件不存在，处理 queries
        # queries = process_queries(queries, q_tables, constructor)
            queries = process_queries_tapas(queries, q_tables, config)

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
            dl_tables = process_tables_tapas(dl_tables, config)
            with open(dltable_emb_path, 'wb') as f:
                pickle.dump(dl_tables, f)
            print(f"Saved new tables to {dltable_emb_path}")

    print(config)
    print(model, flush=True)

    # constructor = TabularGraph(config["fasttext"], config["merge_same_cells"])



    # with open('/home/clx/home/code/nl_table_retrieval/src/table_encoder/gtr/query-emb.pickle', 'wb') as f:
    #     pickle.dump(queries, f)

    # process_tables(dl_tables, constructor)

    # with open('/home/clx/home/code/nl_table_retrieval/src/table_encoder/gtr/dltable-emb.pickle', 'wb') as f:
    #     pickle.dump(dl_tables, f)

    loss_func = torch.nn.MSELoss()

    qindex = list(queries.keys())
    sample_index = np.array(range(len(qindex))).reshape((-1, 1))

    best_cv_metrics = [None for _ in range(5)]
    for n_fold, (train_data, validation_data) in enumerate(ss.split(sample_index)):

        train_query_ids = [qindex[idx] for idx in train_data]
        validation_query_ids = [qindex[idx] for idx in validation_data]

        del model

        if config["baseline-model"] == "GTR" :

            model = MatchingModel(bert_dir=config["bert_dir"], do_lower_case=config["do_lower_case"],
                              bert_size=config["bert_size"], gnn_output_size=config["gnn_size"])
            
        if config["baseline-model"] == "TaBERT" :

            model = MatchingModel(bert_dir=config["bert_dir"], do_lower_case=config["do_lower_case"],
                              bert_size=config["bert_size"], gnn_output_size=config["gnn_size"])

        if config["baseline-model"] == "starmie" :

            model = MatchingModel_starmie(bert_dir=config["bert_dir"], do_lower_case=config["do_lower_case"],
                              bert_size=config["bert_size"])
        
        if config["baseline-model"] == "tapas" :

            model = MatchingModel_tapas(bert_dir=config["bert_dir"], do_lower_case=config["do_lower_case"],
                              bert_size=config["bert_size"])
      
        if config["use_pretrained_model"]:
            pretrain_model = torch.load(config["pretrained_model_path"])
            model.load_state_dict(pretrain_model, strict=False)

        model = model.to("cuda")

        optimizer = torch.optim.Adam([
            {'params': model.bert.parameters(), 'lr': config["bert_lr"]},
            {'params': (x for x in model.parameters() if x not in set(model.bert.parameters())), 'lr': config["gnn_lr"]}
        ])
        scheduler = WarmupLinearSchedule(optimizer, warmup_steps=config["warmup_steps"], t_total=config["total_steps"])

        train_pairs = [(qid, tid, rel) for qid in train_query_ids for tid, rel in ground_truth[qid].items()]

        best_metrics = None
        for epoch in range(config['epoch']):
            train(config, model, train_pairs, optimizer, scheduler, loss_func)

            train_metrics = evaluate(config, model, train_query_ids)
            test_metrics = evaluate(config, model, validation_query_ids)

            print('test_metrics')
            print(test_metrics)

            print('best_metrics')
            print(best_metrics)

            if best_metrics is None or test_metrics[config['key_metric']] > best_metrics[config['key_metric']]:
                best_metrics = test_metrics
                best_cv_metrics[n_fold] = best_metrics
                print(datetime.datetime.now(), 'epoch', epoch, 'train', train_metrics, 'test', test_metrics, "*",
                      flush=True)
                with open('santos_results.txt', 'a') as file:
                    # 将字典转换为字符串
                    data_dir = config["data_dir"]
                    metrics_str = f"gtr_cv_{data_dir} \n epoch {epoch} train {train_metrics} test {test_metrics} *"
                    # 写入字符串到文件，并添加换行符
                    file.write(metrics_str + '\n\n\n')
            else:
                print(datetime.datetime.now(), 'epoch', epoch, 'train', train_metrics, 'test', test_metrics, flush=True)
                with open('src/table_encoder/gtr/result.txt', 'a') as file:
                    # 将字典转换为字符串
                    data_dir = config["data_dir"]
                    metrics_str = f"gtr_{data_dir} \n epoch {epoch} train {train_metrics} test {test_metrics} *"
                    # 写入字符串到文件，并添加换行符
                    file.write(metrics_str + '\n\n\n')

    avg_metrics = best_cv_metrics[0]
    for key in avg_metrics.keys():
        for metrics in best_cv_metrics[1:]:
            avg_metrics[key] += metrics[key]
        avg_metrics[key] /= 5
    print("5-fold cv scores", avg_metrics)
