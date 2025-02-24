import torch
import torch.nn as nn
import torch.nn.functional as F
from pytorch_transformers import BertTokenizer, BertModel

from src.table_encoder.gtr.gat import GATEncoder

class MatchingModel(nn.Module):
    def __init__(self, bert_dir='bert-base-uncased', do_lower_case=True, bert_size=768, gnn_output_size=300):
        super().__init__()

        self.tokenizer = BertTokenizer.from_pretrained(bert_dir, do_lower_case=do_lower_case)
        self.bert = BertModel.from_pretrained(bert_dir)

        self.gnn = GATEncoder(input_dim=300, output_dim=gnn_output_size, hidden_dim=300, layer_num=4,
                              activation=nn.LeakyReLU(0.2))

        self.project_table = nn.Sequential(
            nn.Linear(gnn_output_size, 300),
            nn.LayerNorm(300)
        )

        self.dim_reduction = nn.Sequential(
            nn.Linear(1200, 1200),
            nn.Tanh(),
        )

        self.dim_reduction2 = nn.Sequential(
            nn.Linear(1200, 300),
            nn.Tanh(),
        )

        self.regression = nn.Sequential(
            nn.Dropout(0.1),
            nn.Linear(1200, 1024),
            nn.LeakyReLU(0.2),
            nn.Linear(1024, 1),
        )

    def forward(self, table, query, q_table_dgl_graph, q_table_table_embs, dgl_graph, table_embs, q_feat):
        """table retrieval"""
        # bert_rep = self.text_matching(table, query)

        # gnn_rep = self.text_table_matching(dgl_graph, table_embs, q_feat)

        table_rep = self.table_table_matching(q_table_dgl_graph, q_table_table_embs, dgl_graph, table_embs)

        # rep = torch.cat((bert_rep, table_rep, gnn_rep), -1)

        # table_rep = self.query_table_matching(q_table_dgl_graph, q_table_table_embs, dgl_graph, table_embs, q_feat)

        # rep = torch.cat((bert_rep, gnn_rep), -1)

        score = self.regression(table_rep)

        print('forword end')

        return score

    def text_table_matching(self, dgl_graph, table_embs, query_emb):
        """text-table matching module"""
        creps = self.gnn(dgl_graph, table_embs)

        tmapping = self.project_table(creps)
        qmapping = query_emb.repeat(creps.shape[0], 1)

        hidden = torch.cat((tmapping, qmapping, tmapping - qmapping, tmapping * qmapping), 1)
        
        print(hidden.size(0))
        hidden = self.dim_reduction(hidden)

        hidden = torch.max(hidden, 0)[0]

        return hidden

    def text_matching(self, table, query):
        """text-text matching module"""

        tokens = ["[CLS]"]
        tokens += self.tokenizer.tokenize(" ".join(query['nl']))[:64]
        tokens += ["[SEP]"]

        token_types = [0 for _ in range(len(tokens))]

        tokens += self.tokenizer.tokenize(table["caption"])[:20]
        tokens += ["[SEP]"]

        if 'subcaption' in table:
            tokens += self.tokenizer.tokenize(table["subcaption"])[:20]
            # tokens += ["[SEP]"]

        if 'pgTitle' in table:
            tokens += self.tokenizer.tokenize(table["pgTitle"])[:10]
            # tokens += ["[SEP]"]

        if 'secondTitle' in table:
            tokens += self.tokenizer.tokenize(table["secondTitle"])[:10]
            # tokens += ["[SEP]"]

        token_types += [1 for _ in range(len(tokens) - len(token_types))]

        # truncate and pad
        tokens = tokens[:128]
        token_types = token_types[:128]

        assert len(tokens) == len(token_types)

        token_indices = self.tokenizer.convert_tokens_to_ids(tokens)
        tokens_tensor = torch.tensor([token_indices]).to("cuda")
        token_type_tensor = torch.tensor([token_types]).to("cuda")

        outputs = self.bert(tokens_tensor, token_type_ids=token_type_tensor)

        return outputs[1][0]  # pooled output of the [CLS] token
    
    def table_table_matching(self, dgl_graph_a, table_embs_a, dgl_graph_b, table_embs_b):
        """table-table matching module"""
        print("aaaaaaaa")
        creps_a = self.gnn(dgl_graph_a, table_embs_a)
        creps_b = self.gnn(dgl_graph_b, table_embs_b)

        print("bbbbbbbb")
        tmapping_a = self.project_table(creps_a)
        tmapping_b = self.project_table(creps_b)

        max_size = max(tmapping_a.size(0), tmapping_b.size(0))
        print(max_size)

        if max_size > tmapping_a.size(0):
            tmapping_a_padded = F.pad(tmapping_a, (0, 0, 0, max_size - tmapping_a.size(0)))
        else:
            tmapping_a_padded = tmapping_a

        if max_size > tmapping_b.size(0):
            tmapping_b_padded = F.pad(tmapping_b, (0, 0, 0, max_size - tmapping_b.size(0)))
        else:
            tmapping_b_padded = tmapping_b
            
        # tmapping_a_padded = torch.nn.functional.pad(tmapping_a, (0, 0, 0, max_size - tmapping_a.size(0)))
        # tmapping_b_padded = torch.nn.functional.pad(tmapping_b, (0, 0, 0, max_size - tmapping_b.size(0)))
        # Concatenate, subtract, and multiply features


        print("cccccccc")
        hidden = torch.cat((tmapping_a_padded, tmapping_b_padded, tmapping_a_padded - tmapping_b_padded, tmapping_a_padded * tmapping_b_padded), 1)

        print("dddddddd")
        hidden = self.dim_reduction(hidden)

        print("eeeeeeee")
        hidden = torch.max(hidden, 0)[0]

        return hidden
    

    def query_table_matching(self, dgl_graph_a, table_embs_a, dgl_graph_b, table_embs_b, query_emb):
        """table-table matching module"""

        creps = self.gnn(dgl_graph_a, table_embs_a)

        tmapping = self.project_table(creps)
        qmapping = query_emb.repeat(creps.shape[0], 1)

        query = torch.cat((tmapping, qmapping, tmapping - qmapping, tmapping * qmapping), 1)
        
        query = self.dim_reduction2(query)

        creps_b = self.gnn(dgl_graph_b, table_embs_b)
        tmapping_b = self.project_table(creps_b)

        print(f"query.shape[0]:{query.shape[0]},tmapping_b.shape[0]:{tmapping_b.shape[0]}")
        print(f"query.shape[1]:{query.shape[1]},tmapping_b.shape[1]:{tmapping_b.shape[1]}")
        
        # 确保 query 和 tmapping_b 的第一维度一致
        if query.shape[0] < tmapping_b.shape[0]:
        # 选择性采样 tmapping_b
            tmapping_b = tmapping_b[:query.shape[0]]  # 只取前 query.shape[0] 个样本

        elif query.shape[0] > tmapping_b.shape[0]:
        # 重复 query 以匹配 tmapping_b 的大小
            repeat_count = (tmapping_b.shape[0] + query.shape[0] - 1) // query.shape[0]  # 计算重复次数
            query = query.repeat(repeat_count, 1)[:tmapping_b.shape[0]]  # 确保不超过 tmapping_b 的大小

        hidden = torch.cat((query, tmapping_b, query - tmapping_b, query * tmapping_b), 1)

        hidden = self.dim_reduction(hidden)

        hidden = torch.max(hidden, 0)[0]

        return hidden
