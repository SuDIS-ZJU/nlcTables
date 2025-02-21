from sdd.pretrain import load_checkpoint, inference_on_tables
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

def extractVectors(dfs, model, augment, sample, table_order, run_id, singleCol=False):  #clx：对表进行预测，但是主要是调用inference_on_tables
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
        model_path = "src/table_encoder/starmie_ori/results/%s_model_%s_%s_%s_%dsingleCol.pt" % (model, augment, sample, table_order,run_id)
    else:
        model_path = "src/table_encoder/starmie_ori/results/%s_model_%s_%s_%s_%d.pt" % (model, augment, sample, table_order,run_id)
    ckpt = torch.load(model_path, map_location=torch.device('cuda'))
    # load_checkpoint from sdd/pretain
    model, trainset = load_checkpoint(ckpt)
    return inference_on_tables(dfs, model, trainset, batch_size=1024)

def get_df(dataFolder):    #clx：取得数据集中的每张表
    ''' Get the DataFrames of each table in a folder
    Args:
        dataFolder: filepath to the folder with all tables
    Return:
        dataDfs (dict): key is the filename, value is the dataframe of that table
    '''
    # dataFiles = glob.glob(dataFolder+"/*.csv")
    dataFiles = glob.glob(dataFolder+"/*.json")
    dataDFs = {}
    for file in dataFiles:
        # df = pd.read_csv(file,lineterminator='\n')
        with open(file, 'r', encoding='utf-8') as fn:
                json_data = json.load(fn)
                # print(file)
                df = pd.DataFrame(data=json_data["data"], columns=json_data["title"])
        if len(df) > 1000:
            # get first 1000 rows
            df = df.head(1000)
        filename = file.split("/")[-1]
        dataDFs[filename] = df
    return dataDFs


if __name__ == '__main__':      #clx：差不多看懂了，但是其中调用函数太多，那些调用的函数很多没看懂
    ''' Get the model features by calling model inference from sdd/pretrain
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", type=str, default="2-colLevel-num2") # can be 'santos', 'santosLarge', 'tus', 'tusLarge', 'wdc'
    # single-column mode without table context
    parser.add_argument("--single_column", dest="single_column", action="store_true")
    parser.add_argument("--run_id", type=int, default=0)
    parser.add_argument("--table_order", type=str, default='column')
    parser.add_argument("--save_model", dest="save_model", action="store_true")

    hp = parser.parse_args()

    # # START PARAMETER: defining the benchmark (dataFolder), if it is a single column baseline,
    # # run_id, table_order, and augmentation operators and sampling method if they are different from default
    model = "starmie_ori"
    isSingleCol = hp.single_column
    # if 'santos' in dataFolder or dataFolder == 'wdc':
    #     ao = 'drop_col'
    #     sm = 'tfidf_entity'
    #     if isSingleCol:
    #         ao = 'drop_cell'
    # elif dataFolder == 'tus':
    #     ao = 'drop_cell'
    #     sm = 'alphaHead'
    # else: # dataFolder = tusLarge
    #     ao = 'drop_cell'
    #     sm = 'tfidf_entity'

    ao = 'drop_col'
    sm = 'head'
    if isSingleCol:
        ao = 'drop_cell'

    run_id = hp.run_id
    table_order = hp.table_order
    # END PARAMETER

    # # Change the data paths to where the benchmarks are stored
    # if dataFolder == 'santos':
    #     DATAPATH = "data/santos/"
    #     dataDir = ['query', 'datalake']
    # elif dataFolder == 'santosLarge':
    #     DATAPATH = 'data/santos-benchmark/real-benchmark/'
    #     dataDir = ['query', 'datalake']
    # elif dataFolder == 'tus':
    #     DATAPATH = 'data/table-union-search-benchmark/small/'
    #     dataDir = ['query', 'benchmark']
    # elif dataFolder == 'tusLarge':
    #     DATAPATH = 'data/table-union-search-benchmark/large/'
    #     dataDir = ['query', 'benchmark']
    # elif dataFolder == 'wdc':
    #     DATAPATH = {'query': 'data/wdc/query', 'benchmark': 'data/wdc/0/'}
    #     dataDir = ['query', 'benchmark']

    DATAPATH = hp.benchmark + "/"
    dataDir = ['query-test', 'datalake-test']

    inference_times = 0
    # dataDir is the query and data lake
    for dir in dataDir:
        print("//==== ", dir)
        # if dataFolder == 'wdc':
        #     DATAFOLDER = DATAPATH[dir]
        # else:
        #     DATAFOLDER = DATAPATH+dir
        DATAFOLDER = DATAPATH+dir
        dfs = get_df(DATAFOLDER)
        print("num dfs:",len(dfs))

        dataEmbeds = []
        dfs_totalCount = len(dfs)
        dfs_count = 0

        # Extract model vectors, and measure model inference time
        start_time = time.time()
        cl_features = extractVectors(list(dfs.values()), model, ao, sm, table_order, run_id, singleCol=isSingleCol)
        inference_times += time.time() - start_time
        print("%s %s inference time: %d seconds" %(model, dir, time.time() - start_time))
        for i, file in enumerate(dfs):
            dfs_count += 1
            # get features for this file / dataset
            cl_features_file = np.array(cl_features[i])
            dataEmbeds.append((file, cl_features_file))
        if dir == 'santos-query':
            saveDir = 'query'
        elif dir == 'benchmark':
            saveDir = 'datalake'
        else: saveDir = dir

        if isSingleCol:
            output_path = "src/table_encoder/%s/vectors/cl_%s_%s_%s_%s_%d_singleCol.pkl" % (model, saveDir, ao, sm, table_order, run_id)
        else:
            output_path = "src/table_encoder/%s/vectors/cl_%s_%s_%s_%s_%d.pkl" % (model, saveDir, ao, sm, table_order, run_id)
        if hp.save_model:
            pickle.dump(dataEmbeds, open(output_path, "wb"))
        # print("Benchmark: ", model)
        print("Benchmark: starmie")
        print("--- Total Inference Time: %s seconds ---" % (inference_times))
