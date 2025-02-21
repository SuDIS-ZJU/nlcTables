import numpy as np
from torch.utils.data import DataLoader
import math
from tqdm import tqdm
import torch
from sentence_transformers import SentenceTransformer, LoggingHandler, losses, util, InputExample
from sentence_transformers.evaluation import EmbeddingSimilarityEvaluator
from torch.nn.parallel import DataParallel
import logging
from datetime import datetime
import os
import csv
import pickle
import multiprocessing
import time


def process_onedataset(dataset_file,model_name ='Join1/Deepjoin/deepjoin/model/output/deepjoin_webtable_training-all-mpnet-base-v2-2025-02-13_06-12-17',
                    storepath = "Join1/Deepjoin/final_result"):
    start_time = time.time()

    path,filename_dataset = os.path.split(dataset_file)

    model = SentenceTransformer(model_name)
    os.makedirs(storepath,exist_ok=True)
    storedata = []
    if os.path.isfile(dataset_file):
        print("process data",dataset_file)
        try:
            #记载数据
            with open(dataset_file,"rb") as f:
                data = pickle.load(f)
                print(data)
            for ele in tqdm(data):
                key,value = ele
                sentence_embeddings = model.encode(value)
                sentence_embeddings_np = np.array(sentence_embeddings)
                tu1 = (key,sentence_embeddings_np)
                storedata.append(tu1)
        except Exception as e:
            print(e)
    storefilename = os.path.join(storepath,filename_dataset)
    with open(storefilename,"wb") as f:
        pickle.dump(storedata,f)
    print("data process sucess",storefilename)
    end_time = time.time()
    exc_time = start_time - end_time
    print("exc_time",exc_time)



    






