import os
import random
import argparse

from tableLevel.join_table import split_theme_table, split_scaleCol_table, split_cellValue_table
from colLevel.join_col import split_col_table
from multi.join_multi import split_scaleCol_col_table, split_cellValue_col_table,split_cellValue_scaleCol_table

# def process_dataset(exp, dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt):
def split_dataset(dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt):
    
    theme_num = 0
    scaleCol_num = 0
    cellValue_num = 0

    col_num = 0

    cellvalue_col_num = 0
    scaleCol_col_num = 0
    cellValue_scaleCol_num = 0

    theme_dl_num = 0
    scaleCol_dl_num = 0
    cellValue_dl_num = 0

    col_dl_num = 0

    cellvalue_col_dl_num = 0
    scaleCol_col_dl_num = 0
    cellValue_scaleCol_dl_num = 0


    index = 0
    table_level = ['theme','cellValue','cellValue']
    col_level = ['col']
    multi = ['cellvalue_col']
    scale_level = ['scaleCol','scale_col','cellvalue_scaleCol']
    
    for json_file in os.listdir(dataset_folder):
        exps = []

        print("***index:",index)

        if index % 6 in (1,2,3) :
            exps = random.sample(col_level,1) + random.sample(scale_level,1)
        elif index % 6 in (4,5) :
            exps = random.sample(multi,1) + random.sample(scale_level,1)
        else :
            exps =random.sample(table_level,1) + random.sample(scale_level,1)

        # if index % 5 in (3,4) :
        #     exps = random.sample(col_level,1) + random.sample(date_level,2)+ random.sample(scale_level,1)
        # elif index % 5 in (1,2) :
        #     exps = random.sample(col_level,1) + random.sample(table_level,1)+ random.sample(date_level,2)+ random.sample(scale_level,1)
        # else:
        #     exps = random.sample(multi,1) + random.sample(date_level,2)+ random.sample(scale_level,1)

        print(exps)

        # 使用字典或 OrderedDict 来保持元素的顺序
        exps = list(dict.fromkeys(exps))

        print(exps)

        # exps = ['cellvalue_scaleCol']

        for exp in exps:
            # print(exp)
            if exp == 'theme':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1

                index_copy = index

                print(exp)
                if json_file.endswith('.json'):
                    index = split_theme_table(
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt,
                    )
                print("split_theme_table")

                theme_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                theme_dl_num += file_count_1 - file_count_0

            if exp == 'scaleCol':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1

                index_copy = index

                print(exp)
                if json_file.endswith('.json'):
                    index = split_scaleCol_table(         
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt,
                    )    
                print("split_scaleCol_table")

                scaleCol_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                scaleCol_dl_num += file_count_1 - file_count_0

            if exp == 'cellValue':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                
                index_copy = index

                print(exp)
                if json_file.endswith('.json'):
                    index = split_cellValue_table(
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )   
                print("split_cellValue_table") 

                cellValue_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                cellValue_dl_num += file_count_1 - file_count_0

            if exp == 'col':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                
                index_copy = index

                print(exp)
                if json_file.endswith('.json'):
                    index = split_col_table(
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )   
                print("split_col_table")    

                col_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                col_dl_num += file_count_1 - file_count_0
      

            if exp == 'scale_col':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                
                index_copy = index

                print(exp)
                if json_file.endswith('.json'):
                    index = split_scaleCol_col_table(
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    ) 
                print("split_scale_category_table")

                scaleCol_col_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                scaleCol_col_dl_num += file_count_1 - file_count_0

            if exp == 'cellvalue_col':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                
                index_copy = index

                print(exp)
                if json_file.endswith('.json'):
                    index = split_cellValue_col_table(
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )  
                print("split_cellvalue_category_table")

                cellvalue_col_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                cellvalue_col_dl_num += file_count_1 - file_count_0
     
            if exp == 'cellvalue_scaleCol':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                
                index_copy = index

                print(exp)
                if json_file.endswith('.json'):
                    index = split_cellValue_scaleCol_table(
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )  
                print("split_cellValue_scaleCol_table")

                cellValue_scaleCol_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                cellValue_scaleCol_dl_num += file_count_1 - file_count_0


    print("theme_num",theme_num)
    print("scaleCol_num",scaleCol_num)
    print("cellValue_num",cellValue_num)

    print("col_num",col_num)

    print("cellvalue_col_num",cellvalue_col_num)
    print("scaleCol_col_num",scaleCol_col_num)
    print("cellValue_scaleCol_num",cellValue_scaleCol_num)

    print("theme_dl_num",theme_dl_num)
    print("scaleCol_dl_num",scaleCol_dl_num)
    print("cellValue_dl_num",cellValue_dl_num)

    print("col_dl_num",col_dl_num)

    print("cellvalue_col_dl_num",cellvalue_col_dl_num)
    print("scaleCol_col_dl_num",scaleCol_col_dl_num)
    print("cellValue_scaleCol_dl_num",cellValue_scaleCol_dl_num)

    with open('Join1/all/statistic.txt', 'w') as file:
        # 写入内容
        file.write(f"theme_num {theme_num}\n")
        file.write(f"scaleCol_num {scaleCol_num}\n")
        file.write(f"cellValue_num {cellValue_num}\n")
        file.write(f"col_num {col_num}\n")
        file.write(f"cellvalue_col_num {cellvalue_col_num}\n")
        file.write(f"scaleCol_col_num {scaleCol_col_num}\n")
        file.write(f"cellValue_scaleCol_num {cellValue_scaleCol_num}\n")
        file.write(f"theme_dl_num {theme_dl_num}\n")
        file.write(f"scaleCol_dl_num {scaleCol_dl_num}\n")
        file.write(f"cellValue_dl_num {cellValue_dl_num}\n")
        file.write(f"col_dl_num {col_dl_num}\n")
        file.write(f"cellvalue_col_dl_num {cellvalue_col_dl_num}\n")
        file.write(f"scaleCol_col_dl_num {scaleCol_col_dl_num}\n")
        file.write(f"cellValue_scaleCol_dl_num {cellValue_scaleCol_dl_num}\n")



    

# 调用函数


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--exp', type=str, required=True, help='experiment to run')
    # args = parser.parse_args()

    dataset_folder = 'qualified-ori-datalake4_copy/datalake-2575'
    query_folder = 'Join1/all/query-test'
    datalake_folder = 'Join1/all/datalake-test'
    query_txt = 'Join1/all/queries-test.txt'
    groundtruth_txt = 'Join1/all/qtrels-test.txt'

    if not os.path.exists(query_folder):
        os.makedirs(query_folder)
    if not os.path.exists(datalake_folder):
        os.makedirs(datalake_folder)

    # process_dataset(args.exp,dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt)

    split_dataset(dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt)