
from time import sleep

from process_table_tosentence import process_table_sentense


#先处理相关的小数据集
# filepathstore = "/data/lijiajun/deepjoin_webtables/large"
# dirlist  = ["/data_ssd/webtable/large/small_query"]
# data_names_list =["deepjoin_small_query.pkl"] 
# file_dic_path = "/data/lijiajun/deepjoin_tmp"

# filepathstore = "Join1/Deepjoin/data/infer_datalake"
# dirlist  = ["Join1/Deepjoin/all/query-test"]
# data_names_list =["query_infer.pkl"] 
# file_dic_path = "Join1/Deepjoin/data/deepjoin_tmp"

filepathstore = "Join1/Deepjoin/data/infer_datalake"
dirlist  = ["Join1/Deepjoin/all/datalake-test"]
data_names_list =["datalake_infer.pkl"] 
file_dic_path = "Join1/Deepjoin/data/deepjoin_tmp"

split_num = 10
for i in range(len(data_names_list)):
    process_table_sentense(filepathstore =filepathstore, datadir = dirlist[i],data_pkl_name = data_names_list[i],tmppath=file_dic_path,split_num=10)
    print("process sucess",data_names_list[i])