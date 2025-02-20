import os
import random
import argparse
import shutil
import glob

from tableLevel.union_table import split_theme_table, split_scaleRow_table, split_cellValue_table
from colLevel.union_col import split_category_table, split_numerical_larger_table, split_numerical_smaller_table, split_larger_date_table, split_smaller_date_table
from multi.union_multi import split_scale_numerical_smaller_table, split_scale_numerical_larger_table, split_cellvalue_category_table, split_cellvalue_larger_date_table, split_cellvalue_numerical_larger_table, split_cellvalue_numerical_smaller_table, split_cellvalue_smaller_date_table, split_scale_category_table, split_scale_larger_date_table, split_scale_smaller_date_table
from multi.union_multi2 import multi_split_category_table,multi_split_cellValue_table,load_groundtruth,load_queries,multi_split_larger_date_table,multi_split_numerical_larger_table,multi_split_numerical_smaller_table,multi_split_smaller_date_table,multi_split_scaleRow_table


# def process_dataset(exp, dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt):
def split_dataset(dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt):
    
    index = 0
    index_tmp = 0
    table_level = ['theme','cellValue','cellValue']
    col_level = ['category','numerical','numerical']
    multi = ['cellvalue_category','cellvalue_numerical','numerical-category']
    date_level = ['date','scale_date','cellvalue_date','date-category']
    scale_level = ['scaleRow','scale_category','scale_numerical','scale_date','scaleRow-cellValue']
    mix = ['cellvalue_category','cellvalue_numerical','numerical-category','scale_date','cellvalue_date','date-category','scale_category','scale_numerical','scaleRow-cellValue']

    theme_num = 0
    cellValue_num = 0
    scaleRow_num = 0

    category_num = 0
    numerical_num = 0
    date_num = 0

    cellvalue_category_num = 0
    cellvalue_numerical_num = 0
    cellvalue_date_num = 0
    scale_category_num = 0
    scale_numerical_num = 0
    scale_date_num = 0

    scaleRow_cellValue_num = 0
    numerical_category_num = 0
    date_category_num = 0

    theme_dl_num = 0
    cellValue_dl_num = 0
    scaleRow_dl_num = 0

    category_dl_num = 0
    numerical_dl_num = 0
    date_dl_num = 0

    cellvalue_category_dl_num = 0
    cellvalue_numerical_dl_num = 0
    cellvalue_date_dl_num = 0
    scale_category_dl_num = 0
    scale_numerical_dl_num = 0
    scale_date_dl_num = 0

    scaleRow_cellValue_dl_num = 0
    numerical_category_dl_num = 0
    date_category_dl_num = 0


    
    for json_file in os.listdir(dataset_folder):
        exps = []

        print("***index:",index)
        print("***json_file:",json_file)

        if index % 5 in (1,2,3) :
            exps = random.sample(col_level,1) + random.sample(date_level,2)+ random.sample(scale_level,2)
        else:
            exps = random.sample(multi,1) + random.sample(table_level,1)+ random.sample(date_level,2)+ random.sample(scale_level,2)

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

        # exps = exps = random.sample(mix,3)

        # exps = ['scaleRow-cellValue','numerical-category','date-category']

        for exp in exps:
            # print(exp)
            if exp == 'theme':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1

                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    index = split_theme_table(
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )
                print("split_theme_table")
                theme_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                theme_dl_num += file_count_1 - file_count_0

            if exp == 'scaleRow':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    index = split_scaleRow_table(         
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )    
                print("split_scaleRow_table")
                scaleRow_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                scaleRow_dl_num += file_count_1 - file_count_0

            if exp == 'cellValue':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
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

            if exp == 'category':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    index = split_category_table(
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )   
                print("split_category_table") 
                category_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                category_dl_num += file_count_1 - file_count_0

            if exp == 'numerical':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    if random.choice([True, False]):
                        index = split_numerical_larger_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        )    
                    else:
                        index = split_numerical_smaller_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        )  
                    print("split_numerical_larger_table") 
                    numerical_num += index - index_copy

                    file_count_1 = 0
                    for item in os.listdir(datalake_folder):
                        item_path = os.path.join(datalake_folder, item)
                        if os.path.isfile(item_path):
                            file_count_1 += 1

                    numerical_dl_num += file_count_1 - file_count_0

            if exp == 'date':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    if random.choice([True, False]):
                        index = split_larger_date_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        )    
                    else:
                        index = split_smaller_date_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        ) 
                    print("split_smaller_date_table")
                    date_num += index - index_copy 

                    file_count_1 = 0
                    for item in os.listdir(datalake_folder):
                        item_path = os.path.join(datalake_folder, item)
                        if os.path.isfile(item_path):
                            file_count_1 += 1

                    date_dl_num += file_count_1 - file_count_0

            if exp == 'scale_category':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    index = split_scale_category_table(
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    ) 
                print("split_scale_category_table")
                scale_category_num += index - index_copy 

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                scale_category_dl_num += file_count_1 - file_count_0
        
            if exp == 'scale_numerical':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    if random.choice([True, False]):
                        index = split_scale_numerical_larger_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        )    
                    else:
                        index = split_scale_numerical_smaller_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        )
                    print("split_scale_numerical_larger_table")   
                    scale_numerical_num += index - index_copy

                    file_count_1 = 0
                    for item in os.listdir(datalake_folder):
                        item_path = os.path.join(datalake_folder, item)
                        if os.path.isfile(item_path):
                            file_count_1 += 1

                    scale_numerical_dl_num += file_count_1 - file_count_0

            if exp == 'scale_date':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    if random.choice([True, False]):
                        index = split_scale_larger_date_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        )    
                    else:
                        index = split_scale_smaller_date_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        )
                    print("split_scale_smaller_date_table")  
                    scale_date_num += index - index_copy

                    file_count_1 = 0
                    for item in os.listdir(datalake_folder):
                        item_path = os.path.join(datalake_folder, item)
                        if os.path.isfile(item_path):
                            file_count_1 += 1

                    scale_date_dl_num += file_count_1 - file_count_0


            if exp == 'cellvalue_category':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    index = split_cellvalue_category_table(
                        index,
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )  
                print("split_cellvalue_category_table")
                cellvalue_category_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                cellvalue_category_dl_num += file_count_1 - file_count_0
        
            if exp == 'cellvalue_numerical':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    if random.choice([True, False]):
                        index = split_cellvalue_numerical_larger_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        )    
                    else:
                        index = split_cellvalue_numerical_smaller_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        )
                    print("split_cellvalue_numerical_larger_table")   
                    cellvalue_numerical_num += index - index_copy

                    file_count_1 = 0
                    for item in os.listdir(datalake_folder):
                        item_path = os.path.join(datalake_folder, item)
                        if os.path.isfile(item_path):
                            file_count_1 += 1

                    cellvalue_numerical_dl_num += file_count_1 - file_count_0

            if exp == 'cellvalue_date':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1
                print(exp)
                if json_file.endswith('.json'):
                    index_copy = index
                    if random.choice([True, False]):
                        index = split_cellvalue_larger_date_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        )    
                    else:
                        index = split_cellvalue_smaller_date_table(
                            index,
                            os.path.join(dataset_folder, json_file),
                            query_folder, 
                            datalake_folder, 
                            query_txt, 
                            groundtruth_txt
                        ) 
                    print("split_cellvalue_larger_date_table") 
                    cellvalue_date_num += index - index_copy

                    file_count_1 = 0
                    for item in os.listdir(datalake_folder):
                        item_path = os.path.join(datalake_folder, item)
                        if os.path.isfile(item_path):
                            file_count_1 += 1

                    cellvalue_date_dl_num += file_count_1 - file_count_0


                    
            if exp == 'scaleRow-cellValue':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1

                index_copy = index


                tem_query_folder = 'Union1/multi/multi2/tem/query-test'
                tem_datalake_folder = 'Union1/multi/multi2/tem/datalake-test'
                tem_query_txt = 'Union1/multi/multi2/tem/queries-test.txt'
                tem_groundtruth_txt = 'Union1/multi/multi2/tem/qtrels-test.txt'

                tem_final_query_folder = 'Union1/multi/multi2/tem_final/query-test'
                tem_final_datalake_folder = 'Union1/multi/multi2/tem_final/datalake-test'
                tem_final_query_txt = 'Union1/multi/multi2/tem_final/queries-test.txt'
                tem_final_groundtruth_txt = 'Union1/multi/multi2/tem_final/qtrels-test.txt'

                final_query_folder = query_folder
                final_datalake_folder = datalake_folder
                final_query_txt = query_txt
                final_groundtruth_txt = groundtruth_txt

                if not os.path.exists(tem_query_folder):
                    os.makedirs(tem_query_folder)
                if not os.path.exists(tem_datalake_folder):
                    os.makedirs(tem_datalake_folder)
                
                if not os.path.exists(final_query_folder):
                    os.makedirs(final_query_folder)
                if not os.path.exists(final_datalake_folder):
                    os.makedirs(final_datalake_folder)

                if not os.path.exists(tem_final_query_folder):
                    os.makedirs(tem_final_query_folder)
                if not os.path.exists(tem_final_datalake_folder):
                    os.makedirs(tem_final_datalake_folder)

                # for json_file in os.listdir(dataset_folder):
                if json_file.endswith('.json'):
                    
                    index_tmp += 1
                    multi_split_scaleRow_table(
                        index_tmp,
                        os.path.join(dataset_folder, json_file),
                        tem_query_folder, 
                        tem_datalake_folder, 
                        tem_query_txt, 
                        tem_groundtruth_txt,
                        tem_final_query_folder, 
                        tem_final_datalake_folder, 
                        tem_final_query_txt, 
                        tem_final_groundtruth_txt
                    )   
                try:
                    queries= load_queries(tem_query_txt)
                except Exception as e:
                    print(e)
                    continue 
            
                qtrels = load_groundtruth(tem_groundtruth_txt)

                for query_id, query_info in queries.items():

                    print(f"Query ID: {query_id}")
                    print(f"Query Text: {query_info['query_text']}")
                    print(f"Table Name: {query_info['table_name']}")

                    # 查找对应的groundtruth条目
                    if query_id in qtrels:
                        for gt_info in qtrels[query_id]:
                            print(f"\tGround Truth Table Name: {gt_info['table_name']}")
                            print(f"\tGround Truth Relation Type: {gt_info['rel_type']}")
                            # index_copy = index
                            index,last_gt = multi_split_cellValue_table(
                                query_info['table_name'],
                                query_info['query_text'],
                                gt_info['table_name'],
                                gt_info['rel_type'],
                                index,
                                tem_query_folder,
                                tem_datalake_folder,
                                final_query_folder, 
                                final_datalake_folder, 
                                final_query_txt, 
                                final_groundtruth_txt
                            )    
                            # if index != index_copy:
                            #     print('index',index)
                            #     oritable_id = gt_info['table_name'].strip().split('_')
                            #     last_gt_id = last_gt.strip().split('_')
                            #     print('oritable_id',oritable_id)
                            #     print('last_gt_id',last_gt_id)
                            #     if last_gt_id[2] == oritable_id[2]:
                            #         with open(final_groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                            #             gt_txt.write(f"{index-1}\t0\t{last_gt}\t2\n")

                    else:
                        print("No ground truth data found for this query.")
                    print("\n")

                    # 遍历源文件夹下的所有文件
                    for filename in os.listdir(tem_final_datalake_folder):
                        # 构建源文件和目标文件的完整路径
                        source_file_path = os.path.join(tem_final_datalake_folder, filename)
                        destination_file_path = os.path.join(final_datalake_folder, filename)
                        shutil.copy(source_file_path, destination_file_path)

                    # 打开源文件进行读取
                    with open(tem_final_groundtruth_txt, 'r', encoding='utf-8') as source_file:
                        # 读取所有内容
                        content = source_file.readlines()
                        print('content',content)

                    # # 打开目标文件进行写入，并追加内容
                    # with open(final_groundtruth_txt, 'a', encoding='utf-8') as destination_file:
                    #     # 将内容写入目标文件
                    #     destination_file.write(content)

                    with open(final_groundtruth_txt, 'a', encoding='utf-8') as outfile:
                            for line in content:
                                # 解析原始行内容
                                parts = line.strip().split('\t')
                                if len(parts) != 4:
                                    print(f"跳过无效行: {line}")
                                    continue

                                original_index = int(parts[0])
                                datalake_name_nojson = parts[2]

                                # 复制n遍并更新索引
                                for i in range(index-index_copy):
                                    new_index = index_copy + i + 1
                                    outfile.write(f"{new_index}\t0\t{datalake_name_nojson}\t0\n")

                    if os.path.exists('Union1/multi/multi2/tem') and os.path.isdir('Union1/multi/multi2/tem'):
                        shutil.rmtree('Union1/multi/multi2/tem')
                        # 遍历文件夹下的所有文件和子文件夹
                        # for filename in glob.glob(os.path.join('Union1/multi/multi2/tem', '*')):
                        #     # 检查是否是文件
                        #     if os.path.isfile(filename) or os.path.islink(filename):
                        #         os.remove(filename)  # 删除文件
                        #         print(f"Deleted file: {filename}")
                            # 如果需要删除子文件夹中的所有文件，可以在这里添加代码
                            # elif os.path.isdir(filename):
                            #     shutil.rmtree(filename)  # 删除子文件夹及其所有内容
                    else:
                        print(f"The folder 'Union1/multi/multi2/tem' does not exist.")

                    if os.path.exists('Union1/multi/multi2/tem_final') and os.path.isdir('Union1/multi/multi2/tem_final'):
                        shutil.rmtree('Union1/multi/multi2/tem_final')
                        # 遍历文件夹下的所有文件和子文件夹
                        # for filename in glob.glob(os.path.join('Union1/multi/multi2/tem_final', '*')):
                        #     # 检查是否是文件
                        #     if os.path.isfile(filename) or os.path.islink(filename):
                        #         os.remove(filename)  # 删除文件
                        #         print(f"Deleted file: {filename}")
                            # 如果需要删除子文件夹中的所有文件，可以在这里添加代码
                            # elif os.path.isdir(filename):
                            #     shutil.rmtree(filename)  # 删除子文件夹及其所有内容
                    else:
                        print(f"The folder 'Union1/multi/multi2/tem_final' does not exist.")

                scaleRow_cellValue_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                scaleRow_cellValue_dl_num += file_count_1 - file_count_0


            if exp == 'numerical-category':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1

                index_copy = index

                tem_query_folder = 'Union1/multi/multi2/tem2/query-test'
                tem_datalake_folder = 'Union1/multi/multi2/tem2/datalake-test'
                tem_query_txt = 'Union1/multi/multi2/tem2/queries-test.txt'
                tem_groundtruth_txt = 'Union1/multi/multi2/tem2/qtrels-test.txt'

                tem_final_query_folder = 'Union1/multi/multi2/tem_final2/query-test'
                tem_final_datalake_folder = 'Union1/multi/multi2/tem_final2/datalake-test'
                tem_final_query_txt = 'Union1/multi/multi2/tem_final2/queries-test.txt'
                tem_final_groundtruth_txt = 'Union1/multi/multi2/tem_final2/qtrels-test.txt'

                final_query_folder = query_folder
                final_datalake_folder = datalake_folder
                final_query_txt = query_txt
                final_groundtruth_txt = groundtruth_txt
                
                if not os.path.exists(tem_query_folder):
                    os.makedirs(tem_query_folder)
                if not os.path.exists(tem_datalake_folder):
                    os.makedirs(tem_datalake_folder)
                
                if not os.path.exists(final_query_folder):
                    os.makedirs(final_query_folder)
                if not os.path.exists(final_datalake_folder):
                    os.makedirs(final_datalake_folder)

                if not os.path.exists(tem_final_query_folder):
                    os.makedirs(tem_final_query_folder)
                if not os.path.exists(tem_final_datalake_folder):
                    os.makedirs(tem_final_datalake_folder)
               
            # for json_file in os.listdir(dataset_folder):
                if json_file.endswith('.json'):
                    
                    index_tmp += 1
                    if random.choice([True, False]):
                        multi_split_numerical_larger_table(
                            index_tmp,
                            os.path.join(dataset_folder, json_file),
                            tem_query_folder, 
                            tem_datalake_folder, 
                            tem_query_txt, 
                            tem_groundtruth_txt,
                            tem_final_query_folder, 
                            tem_final_datalake_folder, 
                            tem_final_query_txt, 
                            tem_final_groundtruth_txt
                        )    
                    else:
                        multi_split_numerical_smaller_table(
                            index_tmp,
                            os.path.join(dataset_folder, json_file),
                            tem_query_folder, 
                            tem_datalake_folder, 
                            tem_query_txt, 
                            tem_groundtruth_txt,
                            tem_final_query_folder, 
                            tem_final_datalake_folder, 
                            tem_final_query_txt, 
                            tem_final_groundtruth_txt
                        )  
                try:
                    queries= load_queries(tem_query_txt)
                except Exception as e:
                    print(e)
                    continue 

                qtrels = load_groundtruth(tem_groundtruth_txt)

                for query_id, query_info in queries.items():

                    print(f"Query ID: {query_id}")
                    print(f"Query Text: {query_info['query_text']}")
                    print(f"Table Name: {query_info['table_name']}")

                    # 查找对应的groundtruth条目
                    if query_id in qtrels:
                        for gt_info in qtrels[query_id]:
                            print(f"\tGround Truth Table Name: {gt_info['table_name']}")
                            print(f"\tGround Truth Relation Type: {gt_info['rel_type']}")
                            # index_copy = index
                            index,last_gt = multi_split_category_table(
                                query_info['table_name'],
                                query_info['query_text'],
                                gt_info['table_name'],
                                gt_info['rel_type'],
                                index,
                                tem_query_folder,
                                tem_datalake_folder,
                                final_query_folder, 
                                final_datalake_folder, 
                                final_query_txt, 
                                final_groundtruth_txt
                            )    
                            # if index != index_copy:
                            #     print('index',index)
                            #     oritable_id = gt_info['table_name'].strip().split('_')
                            #     last_gt_id = last_gt.strip().split('_')
                            #     print('oritable_id',oritable_id)
                            #     print('last_gt_id',last_gt_id)
                            #     if last_gt_id[2] == oritable_id[2]:
                            #         with open(final_groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                            #             gt_txt.write(f"{index-1}\t0\t{last_gt}\t2\n")


                    else:
                        print("No ground truth data found for this query.")
                    print("\n")

                    try:
                        # # 打开源文件进行读取
                        with open(tem_final_groundtruth_txt, 'r', encoding='utf-8') as source_file:
                            # 读取所有内容
                            content = source_file.readlines()
                            print('content',content)

                        # # 打开目标文件进行写入，并追加内容
                        # with open(final_groundtruth_txt, 'a', encoding='utf-8') as destination_file:
                        #     # 将内容写入目标文件
                        #     destination_file.write(content)

                        with open(final_groundtruth_txt, 'a', encoding='utf-8') as outfile:
                            for line in content:
                                # 解析原始行内容
                                parts = line.strip().split('\t')
                                if len(parts) != 4:
                                    print(f"跳过无效行: {line}")
                                    continue

                                original_index = int(parts[0])
                                datalake_name_nojson = parts[2]

                                # 复制n遍并更新索引
                                for i in range(index-index_copy):
                                    new_index = index_copy + i + 1
                                    outfile.write(f"{new_index}\t0\t{datalake_name_nojson}\t0\n")

                        # 遍历源文件夹下的所有文件
                        for filename in os.listdir(tem_final_datalake_folder):
                            # 构建源文件和目标文件的完整路径
                            source_file_path = os.path.join(tem_final_datalake_folder, filename)
                            destination_file_path = os.path.join(final_datalake_folder, filename)
                            shutil.copy(source_file_path, destination_file_path)
                    except Exception as e:
                        print(e)

                    if os.path.exists('Union1/multi/multi2/tem2') and os.path.isdir('Union1/multi/multi2/tem2'):
                        shutil.rmtree('Union1/multi/multi2/tem2')
                        # 遍历文件夹下的所有文件和子文件夹
                        # for filename in glob.glob(os.path.join('Union1/multi/multi2/tem2', '*')):
                        #     # 检查是否是文件
                        #     if os.path.isfile(filename) or os.path.islink(filename):
                        #         os.remove(filename)  # 删除文件
                        #         print(f"Deleted file: {filename}")
                            # 如果需要删除子文件夹中的所有文件，可以在这里添加代码
                            # elif os.path.isdir(filename):
                            #     shutil.rmtree(filename)  # 删除子文件夹及其所有内容
                    else:
                        print(f"The folder 'Union1/multi/multi2/tem2' does not exist.")

                    if os.path.exists('Union1/multi/multi2/tem_final2') and os.path.isdir('Union1/multi/multi2/tem_final2'):
                        shutil.rmtree('Union1/multi/multi2/tem_final2')
                        # 遍历文件夹下的所有文件和子文件夹
                        # for filename in glob.glob(os.path.join('Union1/multi/multi2/tem_final2', '*')):
                        #     # 检查是否是文件
                        #     if os.path.isfile(filename) or os.path.islink(filename):
                        #         os.remove(filename)  # 删除文件
                        #         print(f"Deleted file: {filename}")
                            # 如果需要删除子文件夹中的所有文件，可以在这里添加代码
                            # elif os.path.isdir(filename):
                            #     shutil.rmtree(filename)  # 删除子文件夹及其所有内容
                    else:
                        print(f"The folder 'Union1/multi/multi2/tem_final2' does not exist.")

                numerical_category_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                numerical_category_dl_num += file_count_1 - file_count_0


            if exp == 'date-category':
                file_count_0 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_0 += 1

                index_copy = index

                tem_query_folder = 'Union1/multi/multi2/tem1/query-test'
                tem_datalake_folder = 'Union1/multi/multi2/tem1/datalake-test'
                tem_query_txt = 'Union1/multi/multi2/tem1/queries-test.txt'
                tem_groundtruth_txt = 'Union1/multi/multi2/tem1/qtrels-test.txt'

                tem_final_query_folder = 'Union1/multi/multi2/tem_final1/query-test'
                tem_final_datalake_folder = 'Union1/multi/multi2/tem_final1/datalake-test'
                tem_final_query_txt = 'Union1/multi/multi2/tem_final1/queries-test.txt'
                tem_final_groundtruth_txt = 'Union1/multi/multi2/tem_final1/qtrels-test.txt'

                final_query_folder = query_folder
                final_datalake_folder = datalake_folder
                final_query_txt = query_txt
                final_groundtruth_txt = groundtruth_txt
                
                if not os.path.exists(tem_query_folder):
                    os.makedirs(tem_query_folder)
                if not os.path.exists(tem_datalake_folder):
                    os.makedirs(tem_datalake_folder)
                
                if not os.path.exists(final_query_folder):
                    os.makedirs(final_query_folder)
                if not os.path.exists(final_datalake_folder):
                    os.makedirs(final_datalake_folder)

                if not os.path.exists(tem_final_query_folder):
                    os.makedirs(tem_final_query_folder)
                if not os.path.exists(tem_final_datalake_folder):
                    os.makedirs(tem_final_datalake_folder)

                
            # for json_file in os.listdir(dataset_folder):
                if json_file.endswith('.json'):
                    # index_copy = index
                    index_tmp += 1
                    if random.choice([True, False]):
                        multi_split_larger_date_table(
                            index_tmp,
                            os.path.join(dataset_folder, json_file),
                            tem_query_folder, 
                            tem_datalake_folder, 
                            tem_query_txt, 
                            tem_groundtruth_txt,
                            tem_final_query_folder, 
                            tem_final_datalake_folder, 
                            tem_final_query_txt, 
                            tem_final_groundtruth_txt
                        )    
                    else:
                        multi_split_smaller_date_table(
                            index_tmp,
                            os.path.join(dataset_folder, json_file),
                            tem_query_folder, 
                            tem_datalake_folder, 
                            tem_query_txt, 
                            tem_groundtruth_txt,
                            tem_final_query_folder, 
                            tem_final_datalake_folder, 
                            tem_final_query_txt, 
                            tem_final_groundtruth_txt
                        )  
                try:
                    queries= load_queries(tem_query_txt)
                except Exception as e:
                    print(e)
                    continue 

                qtrels = load_groundtruth(tem_groundtruth_txt)

                for query_id, query_info in queries.items():

                    print(f"Query ID: {query_id}")
                    print(f"Query Text: {query_info['query_text']}")
                    print(f"Table Name: {query_info['table_name']}")

                    # 查找对应的groundtruth条目
                    if query_id in qtrels:
                        for gt_info in qtrels[query_id]:
                            print(f"\tGround Truth Table Name: {gt_info['table_name']}")
                            print(f"\tGround Truth Relation Type: {gt_info['rel_type']}")
                            # index_copy = index
                            index,last_gt = multi_split_category_table(
                                query_info['table_name'],
                                query_info['query_text'],
                                gt_info['table_name'],
                                gt_info['rel_type'],
                                index,
                                tem_query_folder,
                                tem_datalake_folder,
                                final_query_folder, 
                                final_datalake_folder, 
                                final_query_txt, 
                                final_groundtruth_txt
                            )    
                            # if index != index_copy:
                            #     print('index',index)
                            #     oritable_id = gt_info['table_name'].strip().split('_')
                            #     last_gt_id = last_gt.strip().split('_')
                            #     print('oritable_id',oritable_id)
                            #     print('last_gt_id',last_gt_id)
                            #     if last_gt_id[2] == oritable_id[2]:
                            #         with open(final_groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                            #             gt_txt.write(f"{index-1}\t0\t{last_gt}\t2\n")

                    else:
                        print("No ground truth data found for this query.")
                    print("\n")

                    try:
                        # 打开源文件进行读取
                        with open(tem_final_groundtruth_txt, 'r', encoding='utf-8') as source_file:
                            # 读取所有内容
                            content = source_file.readlines()
                            print('content',content)

                        # # 打开目标文件进行写入，并追加内容
                        # with open(final_groundtruth_txt, 'a', encoding='utf-8') as destination_file:
                        #     # 将内容写入目标文件
                        #     destination_file.write(content)

                        # with open(input_file, 'r', encoding='utf-8') as infile:
                        #     lines = infile.readlines()

                        with open(final_groundtruth_txt, 'a', encoding='utf-8') as outfile:
                            for line in content:
                                # 解析原始行内容
                                parts = line.strip().split('\t')
                                if len(parts) != 4:
                                    print(f"跳过无效行: {line}")
                                    continue

                                original_index = int(parts[0])
                                datalake_name_nojson = parts[2]

                                # 复制n遍并更新索引
                                for i in range(index-index_copy):
                                    new_index = index_copy + i + 1
                                    outfile.write(f"{new_index}\t0\t{datalake_name_nojson}\t0\n")

                        # 遍历源文件夹下的所有文件
                        for filename in os.listdir(tem_final_datalake_folder):
                            # 构建源文件和目标文件的完整路径
                            source_file_path = os.path.join(tem_final_datalake_folder, filename)
                            destination_file_path = os.path.join(final_datalake_folder, filename)
                            shutil.copy(source_file_path, destination_file_path)

                    except Exception as e:
                        print(e)

                    

                    if os.path.exists('Union1/multi/multi2/tem1') and os.path.isdir('Union1/multi/multi2/tem1'):
                        # 遍历文件夹下的所有文件和子文件夹
                        shutil.rmtree('Union1/multi/multi2/tem1')
                        print(f"Deleted folder and all its contents: Union1/multi/multi2/tem1")
                        # for filename in glob.glob(os.path.join('Union1/multi/multi2/tem1', '*')):
                        #     # 检查是否是文件
                        #     if os.path.isfile(filename) or os.path.islink(filename):
                        #         os.remove(filename)  # 删除文件
                        #         print(f"Deleted file: {filename}")
                        #     # 如果需要删除子文件夹中的所有文件，可以在这里添加代码
                        #     # elif os.path.isdir(filename):
                        #     #     shutil.rmtree(filename)  # 删除子文件夹及其所有内容
                    else:
                        print(f"The folder 'Union1/multi/multi2/tem1' does not exist.")

                    if os.path.exists('Union1/multi/multi2/tem_final1') and os.path.isdir('Union1/multi/multi2/tem_final1'):
                        shutil.rmtree('Union1/multi/multi2/tem_final1')
                        # 遍历文件夹下的所有文件和子文件夹
                        # for filename in glob.glob(os.path.join('Union1/multi/multi2/tem_final1', '*')):
                        #     # 检查是否是文件
                        #     if os.path.isfile(filename) or os.path.islink(filename):
                        #         os.remove(filename)  # 删除文件
                        #         print(f"Deleted file: {filename}")
                            # 如果需要删除子文件夹中的所有文件，可以在这里添加代码
                            # elif os.path.isdir(filename):
                            #     shutil.rmtree(filename)  # 删除子文件夹及其所有内容

                date_category_num += index - index_copy

                file_count_1 = 0
                for item in os.listdir(datalake_folder):
                    item_path = os.path.join(datalake_folder, item)
                    if os.path.isfile(item_path):
                        file_count_1 += 1

                date_category_dl_num += file_count_1 - file_count_0

    print("scale_numerical_num",scale_numerical_num)
    total_query = theme_num + cellValue_num + scaleRow_num + category_num + numerical_num + date_num + cellvalue_category_num + cellvalue_numerical_num + cellvalue_date_num + scale_category_num + scale_numerical_num + scale_date_num 
    total_dl = theme_dl_num + cellValue_dl_num + scaleRow_dl_num + category_dl_num + numerical_dl_num + date_dl_num + cellvalue_category_dl_num + cellvalue_numerical_dl_num + cellvalue_date_dl_num + scale_category_dl_num + scale_numerical_dl_num + scale_date_dl_num 

    print("theme_num",theme_num)
    print("cellValue_num",cellValue_num)
    print("scaleRow_num",scaleRow_num)

    print("category_num",category_num)
    print("numerical_num",numerical_num)
    print("date_num",date_num)

    print("cellvalue_category_num",cellvalue_category_num)
    print("cellvalue_numerical_num",cellvalue_numerical_num)
    print("cellvalue_date_num",cellvalue_date_num)
    print("scale_category_num",scale_category_num)
    print("scale_numerical_num",scale_numerical_num)
    print("scale_date_num",scale_date_num)

    print("scaleRow_cellValue_num",scaleRow_cellValue_num)
    print("numerical_category_num",numerical_category_num)
    print("date_category_num",date_category_num)

    print("theme_dl_num",theme_dl_num)
    print("cellValue_dl_num",cellValue_dl_num)
    print("scaleRow_dl_num",scaleRow_dl_num)

    print("category_dl_num",category_dl_num)
    print("numerical_dl_num",numerical_dl_num)
    print("date_dl_num",date_dl_num)

    print("cellvalue_category_dl_num",cellvalue_category_dl_num)
    print("cellvalue_numerical_dl_num",cellvalue_numerical_dl_num)
    print("cellvalue_date_dl_num",cellvalue_date_dl_num)
    print("scale_category_dl_num",scale_category_dl_num)
    print("scale_numerical_dl_num",scale_numerical_dl_num)
    print("scale_date_dl_num",scale_date_dl_num)

    print("scaleRow_cellValue_dl_num",scaleRow_cellValue_dl_num)
    print("numerical_category_dl_num",numerical_category_dl_num)
    print("date_category_dl_num",date_category_dl_num)

    print("total_query",total_query)
    print("total_dl",total_dl)

    # 将所有值写入txt文件
    with open("Union1/all/statistic.txt", "w", encoding="utf-8") as file:
        file.write(f"theme_num: {theme_num}\n")
        file.write(f"cellValue_num: {cellValue_num}\n")
        file.write(f"scaleRow_num: {scaleRow_num}\n")
        file.write(f"category_num: {category_num}\n")
        file.write(f"numerical_num: {numerical_num}\n")
        file.write(f"date_num: {date_num}\n")
        file.write(f"cellvalue_category_num: {cellvalue_category_num}\n")
        file.write(f"cellvalue_numerical_num: {cellvalue_numerical_num}\n")
        file.write(f"cellvalue_date_num: {cellvalue_date_num}\n")
        file.write(f"scale_category_num: {scale_category_num}\n")
        file.write(f"scale_numerical_num: {scale_numerical_num}\n")
        file.write(f"scale_date_num: {scale_date_num}\n")
        file.write(f"scaleRow_cellValue_num: {scaleRow_cellValue_num}\n")
        file.write(f"numerical_category_num: {numerical_category_num}\n")
        file.write(f"date_category_num: {date_category_num}\n")
        file.write(f"theme_dl_num: {theme_dl_num}\n")
        file.write(f"cellValue_dl_num: {cellValue_dl_num}\n")
        file.write(f"scaleRow_dl_num: {scaleRow_dl_num}\n")
        file.write(f"category_dl_num: {category_dl_num}\n")
        file.write(f"numerical_dl_num: {numerical_dl_num}\n")
        file.write(f"date_dl_num: {date_dl_num}\n")
        file.write(f"cellvalue_category_dl_num: {cellvalue_category_dl_num}\n")
        file.write(f"cellvalue_numerical_dl_num: {cellvalue_numerical_dl_num}\n")
        file.write(f"cellvalue_date_dl_num: {cellvalue_date_dl_num}\n")
        file.write(f"scale_category_dl_num: {scale_category_dl_num}\n")
        file.write(f"scale_numerical_dl_num: {scale_numerical_dl_num}\n")
        file.write(f"scale_date_dl_num: {scale_date_dl_num}\n")
        file.write(f"scaleRow_cellValue_dl_num: {scaleRow_cellValue_dl_num}\n")
        file.write(f"numerical_category_dl_num: {numerical_category_dl_num}\n")
        file.write(f"date_category_dl_num: {date_category_dl_num}\n\n")
        file.write(f"total_query: {total_query}\n")
        file.write(f"total_dl: {total_dl}\n")
                


# 调用函数


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--exp', type=str, required=True, help='experiment to run')
    # args = parser.parse_args()

    dataset_folder = 'qualified-ori-datalake4_copy/datalake-2575'
    
    query_folder = 'Union1/all/query-test'
    datalake_folder = 'Union1/all/datalake-test'
    query_txt = 'Union1/all/queries-test.txt'
    groundtruth_txt = 'Union1/all/qtrels-test.txt'

    

    if not os.path.exists(query_folder):
        os.makedirs(query_folder)
    if not os.path.exists(datalake_folder):
        os.makedirs(datalake_folder)

    

    # process_dataset(args.exp,dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt)

    split_dataset(dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt)

   