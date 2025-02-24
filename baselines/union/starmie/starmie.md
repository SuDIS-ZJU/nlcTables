### Running the offline pre-training pipeline:

The main entry point is `run_pretrain.py`. Example command:

```
CUDA_VISIBLE_DEVICES=0 python run_pretrain.py \
  --task viznet \
  --batch_size 64 \
  --lr 5e-5 \
  --lm roberta \
  --n_epochs 3 \
  --max_len 128 \
  --size 10000 \
  --projector 768 \
  --save_model \
  --augment_op drop_col \
  --fp16 \
  --sample_meth head \
  --table_order column \
  --run_id 0
```

Hyperparameters:

* `--task`: the tasks that we current support include "santos", "santosLarge", "tus", "tusLarge", "large", "small", "viznet". The tasks "large" and "small" are for column matching and "viznet" is for column clustering.
* `--batch_size`, `--lr`, `--n_epochs`, `--max_len`: standard batch size, learning rate, number of training epochs, max sequence length
* `--lm`: the language model (we use roberta for all the experiments)
* `--size`: the maximum number of tables/columns used during pre-training
* `--projector`: the dimension of projector (768 by default, same in all the experiments)
* `--save_model`: if this flag is on, the model checkpoint will be saved to the directory specified in the `--logdir` flag, such as `"results/viznet/model_drop_col_head_column_0.pt"`
* `--augment_op`: augmentation operator for contrastive learning. It includes `["drop_col", "sample_row", "sample_row_ordered", "shuffle_col", "drop_cell", "sample_cells", "replace_cells", "drop_head_cells", "drop_num_cells", "swap_cells", "drop_num_col", "drop_nan_col", "shuffle_row"]`
  1. Column-level: `drop_col` (drops a random column), `shuffle_col` (shuffles columns), `drop_num_col` (drops random numeric columns), `drop_nan_col` (drops columns with mostly NaNs)
  2. Row-level: `sample_row` (sample rows), `sample_row_ordered` (sample rows but preserve order), `shuffle_row` (shuffles the order of rows)
  3. Cell-level: `drop_cell` (drops a random cell), `sample_cells` (sample cells), `replace_cells` (sample random cells and replace with first ordered cells), `drop_head_cells` (drop first quarter cells), `drop_num_cells` (drop a sample of numeric cells), `swap_cells` (swap two cells)
* `--sample_meth`: table pre-processing operator that preserves order and de-duplicates. It includes `["head", "alphaHead", "random", "constant", "frequent", "tfidf_token", "tfidf_entity", "tfidf_row", "pmi"]`
  1. Row-level: `tfidf_row` (takes the rows with highest average tfidf scores), `pmi` (get highest pmi of pairs of column with topic column)
  2. Entity-level: `tfidf_entity` (takes entities in a column with highest after tfidf scores over its tokens)
  3. Token-level: `head` (take first N tokens), `alphaHead` (take first N sorted tokens), `random` (randomly sample tokens), `constant` (take every Nth token), `frequent` (take most frequently-occurring tokens), `tfidf_token` (take tokens with highest tfidf scores)
* `--fp16`: half-precision training (always turn this on)
* `--table_order`: row or column order for pre-processing, "row" or "column"
* `--single_column`: if this flag is on, then it will run the single-column variant ignoring all the
table context
* `--mlflow_tag`: use this flag to assign any additional tags for mlflow logging

### Model Inference:
Run `extractVectors.py`. Example command:

```
python extractVectors.py \
  --benchmark santos \
  --table_order column \
  --run_id 0
```

Hyperparameters
* `--benchmark`: the current benchmark for the experiment. Examples include `santos`, `santosLarge`, `tus`, `tusLarge`, `wdc`
* `--single_column`: if this flag is on, then it will retrieve the single-column variant
* `--run_id`: the run_id of the job (I use 0 for experiments)
* `--table_order`: column-ordered or row-ordered (always use `column`)
* `--save_model`: whether to save the vectors in a pickle file, which is then used in the online processing


### Online processing

1. Linear & Bounds: Run `test_naive_search.py`. Some scripts are in `tus_cmd.sh` and `run_tus_all.py` (for slurm scheduling). Example  command:

```
python test_naive_search.py \
  --encoder cl \
  --benchmark santos \
  --augment_op drop_col \
  --sample_meth tfidf_entity \
  --matching linear \
  --table_order column \
  --run_id 0 \
  --K 10 \
  --threshold 0.7
```

Hyperparameters
* `--encoder`: choice of encoder. Options include "cl" (this is for both full Starmie and
singleCol baseline), "sato", "sherlock"
* `--benchmark`: choice of benchmark for data lake. Options include "santos", "santosLarge",
"tus", "tusLarge", "wdc"
* `--augment_op`: choice of augmentation operator
* `--sample_meth`: choice of sampling method
* `--matching`: "linear" matching (full) or "bounds". If you would like to run "greedy", add the
function call to the code
* `--table_order`: "column" or "row" (just use column)
* `--run_id`: always 0
* `--single_column`: when set to True, run the single column baseline
* `--K`: what you would like to set K to in top-K results
* `--threshold`: the similarity threshold

FOR ERROR ANALYSIS: bucket (bucket number between 0 and 5), analysis (either "col" for number of columns, "row" for number of rows,numeric" for percentage of numerical columns

FOR SCALABILITY EXPERIMENTS: scal (what fraction of data lake do we want to get the metrics scores for – 0.2,0.4,0.6,0.8,1.0)
