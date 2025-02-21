## Folder Structure



```
.
├─── img                    # picture of different experiments
├─── all-mpnet-base-v2      #model name because this file is too large  it is not uploaded ,but you can download it from    https://huggingface.co/sentence-transformers/all-mpnet-base-v2                     
| 
├─── datapreocess           # process data                
| ├─── dataset_to_sentences.py 
| ├─── multi_process_csv.py
| ├─── process_table_tosentence.py         
|   
| 
├─── join             # join algorithms               
| ├─── deepjoin_train.py
| ├─── deepjoin_infer.py
```

<br>



<span id="-getstart"></span>

## 🐳 Getting Started

This is an example of how to set up deepjoin locally. To get a local copy up, running follow these simple example steps.

### Prerequisites

Deepjoin is bulit on pytorch, with torchvision, torchaudio, and transfrmers.

To insall the required packages, you can create a conda environmennt:

```sh
conda create --name deepjoin_env python=3.
```

then use pip to install -r requirements.txt

```sh
pip install -r requirements.txt
```


<span id="-quickstart"></span>

## 🐠 Instruction

Deepjoin is easy to use and extend. Going through the bellowing examples will help you familiar with Deepjoin for detailed instructions, evaluate an existing join/union algorithm on your own dataset, or developing new join/union algorithms.

**Step1: Check your environment**

You need to properly install nvidia driver first. To use GPU in a docker container You also need to install nvidia-docker2 ([Installation Guide](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html#docker)). Then, Please check your CUDA version via `nvidia-smi`

**Step2: Pretrain**

```sh
python deepjoin_train.py --dataset opendata --opendata all-mpnet-base-v2 --model_save_path /deepjoin/model/output  
-- dataset [choose task, str] [opendata, webtable]
--opendata [train_model name str] [all-mpnet-base-v2]
--model_save_path [trained_model_save_path,str]
--file_train_path [pretain_file_path,str]
--train_csv_file [pretrain_file path str]
--storepath [pretrain index store path str]
```

**Step3: infer**

```sh
python dataprocess/dataset_to_sentecnces.py
python deepjoin_infer.py 
-- dataset [choose task, str] [opendata, webtable]
--datafile [infer_tables_file ,str]
--storepath [final_reslut_storepath,str]
```

**Step4: Indexing**

Here are some parameters:

> --benchmark [Choose benchmark, str] [opendata, opendata_large, webtable, webtable_large]

```sh
python index.py --benchmark webtable
```

**Step5: Querying**

> --benchmark [Choose benchmark, str] [opendata, opendata_large, webtable, webtable_large]
>
> --K [Choose top-*k* ,int] [5~60]
>
> --threshold [Choose threshold, float] [0.5~1.0]
>
> --N [Choose N, int] [4, 10, 16, 25, 50]

```sh
python query.py --benchmark webtable --K 5 --N 10 --threshold 0.7
```

<br>
