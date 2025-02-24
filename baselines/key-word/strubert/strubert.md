## Installation

First, install the conda environment `strubert` with supporting libraries.

### First installation method

```bash
conda env create --file scripts/env.yml
```

### Second installation method (manually)

```bash
conda create --name strubert python=3.6
conda activate strubert
pip install torch==1.3.1 torchvision -f https://download.pytorch.org/whl/cu100/torch_stable.html
pip install torch-scatter==1.3.2
pip install fairseq==0.8.0
cd scripts
pip install "--editable=git+https://github.com/huggingface/transformers.git@372a5c1ceec49b52c503707e9657bfaae7c236a0#egg=pytorch_pretrained_bert" --no-cache-dir
pip install -r requirements.txt
```

## Pre-trained model from TaBERT

download TaBERT_Base_(K=3) from [the TaBERT Google Drive shared folder](https://drive.google.com/drive/folders/1fDW9rLssgDAv19OMcFGgFJ5iyd9p7flg?usp=sharing). Please uncompress the tarball files before usage.


### Run

for nlc-union data preparation
```bash
python keyword_based_table_retrieval/transform_data_u.py
```

for nlc-join data preparation
```bash
python keyword_based_table_retrieval/transform_data.py
```

run main.py 
```bash
cd keyword_based_table_retrieval/
chmod +x trec_eval
python main.py \
 --table_folder path/to/wikitables_corpus
 --tabert_path path/to/pretrained/model/checkpoint.bin
 --device 0
 --epochs 3
 --batch_size 4
 --lr 3e-5
```
