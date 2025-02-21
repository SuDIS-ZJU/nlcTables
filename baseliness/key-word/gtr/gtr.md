## Preliminary

[Install DGL](https://docs.dgl.ai/en/0.4.x/install/):
```bash
conda install -c dglteam dgl-cuda10.2  # pay attention to the cuda version
```

[Install fastText](https://fasttext.cc/docs/en/support.html):
```bash
git clone https://github.com/facebookresearch/fastText.git
cd fastText
pip install .
```

Download [pretrained word vectors](https://fasttext.cc/docs/en/pretrained-vectors.html):
```bash
wget https://dl.fbaipublicfiles.com/fasttext/vectors-wiki/wiki.en.zip
unzip wiki.en.zip
```

Install [trec_eval tool](https://github.com/usnistgov/trec_eval):
```bash
git clone https://github.com/usnistgov/trec_eval.git
cd trec_eval
make
```

Install other requirements:
```bash
pip install -r requirements.txt
```

## Run
To run cross validation on WikiTables dataset:
```bash
python run.py --exp cross_validation --config configs/wikitables.json
```
