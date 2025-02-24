from torch.utils.data import DataLoader
import math
import torch
from sentence_transformers import SentenceTransformer, LoggingHandler, losses, util, InputExample
from sentence_transformers.evaluation import EmbeddingSimilarityEvaluator
from torch.nn.parallel import DataParallel
import logging
from datetime import datetime
import os
import csv



def train(train_samples,dev_samples,model_save_path,model_name = 'all-mpnet-base-v2',
          train_batch_size = 16,num_epochs = 4,cpuid = 3):
    #### Just some code to print debug information to stdout
    logging.basicConfig(format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO,
                        handlers=[LoggingHandler()])
    
    #### model save path
    model_save_path = 'Join1/Deepjoin/deepjoin/model/output/deepjoin_webtable_training-'+model_name+'-'+datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # load model    
    model = SentenceTransformer('Join1/Deepjoin/' + model_name)
    # model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')
    
    # set cuda
    if cpuid==1:
        os.environ["CUDA_VISIBLE_DEVICES"]= "1"
    elif cpuid == 0:
        os.environ["CUDA_VISIBLE_DEVICES"]= "0"
    elif cpuid == 2:
        device_ids = [0,1]
        torch.cuda.set_device(device_ids[0])
        model = DataParallel(model, device_ids=[0, 1]) 
        model = model.module  # 获取原始模型
    else:
        pass
 
    # load data
    train_dataloader = DataLoader(train_samples, shuffle=True, batch_size=train_batch_size)
    train_loss = losses.MultipleNegativesRankingLoss(model=model)
    evaluator = EmbeddingSimilarityEvaluator.from_input_examples(dev_samples)
    #warmup_steps = math.ceil(len(train_dataloader) * num_epochs * 0.1)
    warmup_steps = 10000

    # print("train_samples:",train_samples)
    # print("dev_samples:",dev_samples)

    # train model
    model.fit(train_objectives=[(train_dataloader, train_loss)],
            evaluator=evaluator,
            epochs=num_epochs,
            evaluation_steps=1000,
            warmup_steps=warmup_steps,
            weight_decay=0.01,
            output_path=model_save_path)
    
# test


