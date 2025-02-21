### Creating functional dependencies file

SANTOS finds functional dependencies using [Metanome](https://hpi.de/naumann/projects/data-profiling-and-analytics/metanome-data-profiling.html) package. See [Functional Dependency Discovery: An Experimental Evaluation of Seven Algorithms](https://dl.acm.org/doi/pdf/10.14778/2794367.2794377) and [Repeatability - FDs and ODs](https://hpi.de/naumann/projects/repeatability/data-profiling/fds.html) for additional details. You can find the executable files and a python script to generate the pickle file [here](https://drive.google.com/drive/folders/1kNEkIWXYUh4FpLEiro4YMsQBkW-7FpQo?usp=share_link). The folder in the link also contains a README.md file with information on using the executable files and python script.

## Reproducibility

If you want to run SANTOS interactively on SANTOS benchmark, you can check our Demo: [DIALITE](https://tinyurl.com/dialite-sigmod), which is available as a web API. DIALITE is a table discovery and integration pipeline that uses SANTOS for disovering unionable tables from data lakes. For reproducing SANTOS on your machine, please follow the following steps.

1. Download benchmark tables and upload them to their respective subfolders inside [benchmark](benchmark/) folder. You can download both SANTOS benchmarks manually from [zenodo](https://zenodo.org/record/7758091). For convenience, you can also run the following commands on your terminal that are based on [zenodo_get](https://gitlab.com/dvolgyes/zenodo_get) package. The commands automatically download SANTOS Large and SANTOS Small benchmarks, uncompress them and replace placeholder folders with the folders having tables. As the first command takes you to benchmark folder before downloading the benchmarks, make sure that you are in home of the repo. 
    ```
    cd benchmark && zenodo_get 7758091 && rm -r santos_benchmark && unzip santos_benchmark && cd santos_benchmark && rm *.csv && cd .. && rm -r real_tables_benchmark && unzip real_data_lake_benchmark && cd real_data_lake_benchmark && rm *.csv && cd .. && mv real_data_lake_benchmark real_tables_benchmark && rm *.zip && cd ..
    ```
    For TUS benchmark, download them from [this page](https://github.com/RJMillerLab/table-union-search-benchmark) and upload them to their respective subfolders.

2. Download, unzip and upload [YAGO](https://yago-knowledge.org/downloads/yago-4) knowledge base to [yago/yago_original](yago/yago_original) folder.

3. Run [preprocess_yago.py](codes/preprocess_yago.py) to create entity dictionary, type dictionary, inheritance dictionary and relationship dictionary. Then run [Yago_type_counter.py](codes/Yago_type_counter.py), [Yago_subclass_extractor.py](codes/Yago_subclass_extractor.py) and [Yago_subclass_score.py](codes/Yago_subclass_score.py) one after another to generate the type penalization scores. The created dictionaries are stored in [yago/yago_pickle](yago/yago_pickle/). You may delete the downloaded yago files after this step as we do not need orignal yago in [yago/yago_original](yago/yago_original) anymore.

4. Run [data_lake_processing_yago.py](codes/data_lake_processing_yago.py) to create yago inverted index.

5. Run [data_lake_processing_synthesized_kb.py](codes/data_lake_processing_synthesized_kb.py) to create synthesized type dictionary, relationship dictionary and synthesized inverted index.

6. Run [query_santos.py](codes/query_santos.py) to get top-k SANTOS unionable table search results.
