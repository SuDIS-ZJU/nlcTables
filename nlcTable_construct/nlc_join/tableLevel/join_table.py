import json
import os
import random
import argparse
from collections import Counter
import csv

# index = 0

def split_theme_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minCol=8, min_dup=0.3, max_dup=0.5, min_split_rate=0.2, template_num=3, shuffle=1, neg_num = 6, pos_num = 3):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_templates = [
        "I want to find more joinable tables, and these tables have topics related to {caption}",
        "I am looking for additional joinable tables that cover the same topic as {caption}",
        "Can you find more joinable tables with similar content to {caption}",
        "I need more joinable tables that are related to the topic of {caption}"
    ]
    
    # 遍历所有表格
    for table_key, original_table in tables.items():
        
        # 获取表格数据
        data = original_table["data"]
        titles = original_table["title"]
        
        # 如果需要打乱数据顺序
        if shuffle == 1:
            random.shuffle(data)
        
        # 确保分割前原表的行数不少于ori_minRow
        if len(data) <= ori_minCol:
            print(f"Table {table_key} has insufficient Cols for splitting.")
            continue

        # global index
        index += 1

        # 根据每列中含有唯一值数量，选取数量第二多的作为目标列
        unique_values_count = [(len(set(row[i] for row in data)), i) for i in range(len(titles))]
        unique_values_count.sort(reverse=True)
        target_col_index = unique_values_count[1][1] if len(unique_values_count) > 1 else 0

        # 将目标列的唯一值平均分成 data1 和 data2
        target_col_values = list(set([row[target_col_index] for row in data]))
        half_size = len(target_col_values) // 2
        random.shuffle(target_col_values)
        data1_values = list(target_col_values)[:half_size]
        data2_values = list(target_col_values)[half_size:]
        data1 = [row for row in data if row[target_col_index] in data1_values]
        data2 = [row for row in data if row[target_col_index] in data2_values]

        # 从剩余列中随机选出 duplicate
        remaining_cols = [i for i in range(len(titles)) if i != target_col_index]
        duplicate_count = random.randint(int(min_dup * len(remaining_cols)), int(max_dup * len(remaining_cols)))
        duplicate_cols = random.sample(remaining_cols, duplicate_count)

        # 随机选择分割比例
        split_rate = random.uniform(min_split_rate, 0.5)

        # 随机选择不重合列
        non_duplicate_cols = [i for i in remaining_cols if i not in duplicate_cols]
        undup1_cols = non_duplicate_cols[:int(len(non_duplicate_cols) * split_rate)]
        undup2_cols = [i for i in non_duplicate_cols if i not in undup1_cols]

        # 获取原表名并去掉前三个字母
        original_name = table_key
        modified_name = original_name[3:]


        # query-table和datalake-table
        query_data = random.sample(data1, int(len(data1) * 0.8))
        
        # 选择特定的列来构成query_data和datalake_data
        query_data = [[row[target_col_index]] + [row[i] for i in duplicate_cols] + [row[i] for i in undup1_cols] for row in query_data]
        
        # 构建新表名
        query_name = f"q{modified_name}_j1_1.json"
        query_name_nojson = f"q{modified_name}_j1_1"

        # 创建新的表格数据字典，保留原始表格的格式
        query_table_data = original_table.copy()

        # 更新分割后的表格数据
        query_table_data["data"] = query_data
        query_table_data["numDataRows"] = len(query_data)
        query_table_data["title"] = [titles[i] for i in [target_col_index] + duplicate_cols + undup1_cols]
        query_table_data["table_array"] = query_table_data["title"] + query_data
        query_table_data["numCols"] = len([target_col_index] + duplicate_cols + undup1_cols)

        pos_final = 0        
        for pos_number in range(1, pos_num + 1):
         
            datalake_data = random.sample(data1, int(len(data1) * 0.8))     
            undup2_cols = random.sample(undup2_cols, int(len(undup2_cols) * 0.8)) 
            datalake_data = [[row[target_col_index]] + [row[i] for i in duplicate_cols] + [row[i] for i in undup2_cols] for row in datalake_data]
        
            datalake_name = f"dl{modified_name}_j1_1_{pos_number}.json"
            datalake_name_nojson = f"dl{modified_name}_j1_1_{pos_number}"
        
            datalake_table_data = original_table.copy()

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["title"] = [titles[i] for i in [target_col_index] + duplicate_cols + undup2_cols]
            datalake_table_data["table_array"] = datalake_table_data["title"] + datalake_data
            datalake_table_data["numCols"] = len([target_col_index] + duplicate_cols + undup2_cols)

            # 写入新表
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)

            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            # 写入deepjoin.csv
            data_row = [query_name, datalake_name, titles[target_col_index], titles[target_col_index], 1]
            with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                writer = csv.writer(gt_csv)
                writer.writerow(data_row)

            pos_final += 1

            # 生成负例
            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(3, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)

                if target_col_index in selected_col_indices:
                    selected_col_indices = [idx for idx in selected_col_indices if idx != target_col_index]
                            
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)] 

                neg_datalake_name = f"dl{modified_name}_j1_1_{pos_number}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_j1_1_{pos_number}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = len(neg_datalake_data)
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

                # 写入deepjoin.csv
                data_row = [query_name, neg_datalake_name, titles[target_col_index], titles[target_col_index], 0]
                with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                    writer = csv.writer(gt_csv)
                    writer.writerow(data_row)
            
            # 生成负例
            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(3, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count) 

                if target_col_index not in selected_col_indices:
                    selected_col_indices.append(target_col_index)
                            
                selected_row_count = random.randint(1, int(len(data2) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data2, selected_row_count)] 

                neg_datalake_name = f"dl{modified_name}_j1_1_{pos_number}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_j1_1_{pos_number}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = len(neg_datalake_data)
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

                # 写入deepjoin.csv
                data_row = [query_name, neg_datalake_name, titles[target_col_index], titles[target_col_index], 0]
                with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                    writer = csv.writer(gt_csv)
                    writer.writerow(data_row)

                

        if pos_final:

            # 写入新表        
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            
            # 随机选择template_num个模板
            selected_templates = query_templates[:template_num]
            
            # 写入query.txt
            with open(query_txt, 'a', encoding='utf-8') as q_txt:
                selected_template = random.choice(selected_templates)
                q_txt.write(f"{index}\t{selected_template.format(caption=original_table['caption'])}\t{query_name_nojson}\n")

    return index

def split_scaleCol_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minCol=15, min_split_num=10, min_dup=0.3, max_dup=0.5, min_split_rate=0.2, template_num=3, shuffle=1, neg_num = 6, pos_num = 3):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_templates = [
        "I want to find more joinable tables with the number of features larger than {min_col_num}",
        "I need to locate additional joinable tables  with a column count exceeding {min_col_num}",
        "Searching for supplementary tables that have more than {min_col_num} columns to ensure sufficient data for our study",
        "Looking to identify further joinable tables with over {min_col_num} features to merge and enhance our dataset"
    ]
    
    # 遍历所有表格
    for table_key, original_table in tables.items():
        
        # 获取表格数据
        data = original_table["data"]
        titles = original_table["title"]
        
        # 如果需要打乱数据顺序
        if shuffle == 1:
            random.shuffle(data)
        
        # 确保分割前原表的行数不少于ori_minRow
        if len(data) <= ori_minCol:
            print(f"Table {table_key} has insufficient Cols for splitting.")
            continue

        # global index
        index += 1

        # 根据每列中含有唯一值数量，选取数量第二多的作为目标列
        unique_values_count = [(len(set(row[i] for row in data)), i) for i in range(len(titles))]
        unique_values_count.sort(reverse=True)
        target_col_index = unique_values_count[1][1] if len(unique_values_count) > 1 else 0

        # 将目标列的唯一值平均分成 data1 和 data2
        target_col_values = list(set([row[target_col_index] for row in data]))
        half_size = len(target_col_values) // 2
        random.shuffle(target_col_values)
        data1_values = list(target_col_values)[:half_size]
        data2_values = list(target_col_values)[half_size:]
        data1 = [row for row in data if row[target_col_index] in data1_values]
        data2 = [row for row in data if row[target_col_index] in data2_values]

        # 从剩余列中随机选出 duplicate
        remaining_cols = [i for i in range(len(titles)) if i != target_col_index]
        duplicate_count = random.randint(int(min_dup * len(remaining_cols)), int(max_dup * len(remaining_cols)))
        duplicate_cols = random.sample(remaining_cols, duplicate_count)

        # 随机选择分割比例
        split_rate = random.uniform(min_split_rate, 0.5)

        # 随机选择不重合列
        non_duplicate_cols = [i for i in remaining_cols if i not in duplicate_cols]
        undup1_cols = non_duplicate_cols[:int(len(non_duplicate_cols) * split_rate)]
        undup2_cols = [i for i in non_duplicate_cols if i not in undup1_cols]

        # 获取原表名并去掉前三个字母
        original_name = table_key
        modified_name = original_name[3:]


        # query-table和datalake-table
        query_data = random.sample(data1, int(len(data1) * 0.8))
        
        # 选择特定的列来构成query_data和datalake_data
        query_data = [[row[target_col_index]] + [row[i] for i in duplicate_cols] + [row[i] for i in undup1_cols] for row in query_data]
        
        # 构建新表名
        query_name = f"q{modified_name}_j1_2.json"
        query_name_nojson = f"q{modified_name}_j1_2"

        # 创建新的表格数据字典，保留原始表格的格式
        query_table_data = original_table.copy()

        # 更新分割后的表格数据
        query_table_data["data"] = query_data
        query_table_data["numDataRows"] = len(query_data)
        query_table_data["title"] = [titles[i] for i in [target_col_index] + duplicate_cols + undup1_cols]
        query_table_data["table_array"] = query_table_data["title"] + query_data
        query_table_data["numCols"] = len([target_col_index] + duplicate_cols + undup1_cols)


        pos_final = 0
        for pos_number in range(1, pos_num + 1):

            datalake_data = random.sample(data1, int(len(data1) * 0.8))        
            datalake_data = [[row[target_col_index]] + [row[i] for i in duplicate_cols] + [row[i] for i in undup2_cols] for row in datalake_data]
        
            datalake_name = f"dl{modified_name}_j1_2_{pos_number}.json"
            datalake_name_nojson = f"dl{modified_name}_j1_2_{pos_number}"   

            datalake_table_data = original_table.copy()

        
            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["title"] = [titles[i] for i in [target_col_index] + duplicate_cols + undup2_cols]
            datalake_table_data["table_array"] = datalake_table_data["title"] + datalake_data
            datalake_table_data["numCols"] = len([target_col_index] + duplicate_cols + undup2_cols)

            if datalake_table_data["numCols"] < min_split_num:
                continue

        
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)

            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            # 写入deepjoin.csv
            data_row = [query_name, datalake_name, titles[target_col_index], titles[target_col_index], 1]
            with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                writer = csv.writer(gt_csv)
                writer.writerow(data_row)

            pos_final += 1

            # 生成负例
            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(3, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)

                if target_col_index in selected_col_indices:
                    selected_col_indices = [idx for idx in selected_col_indices if idx != target_col_index]
                            
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)] 

                neg_datalake_name = f"dl{modified_name}_j1_2_{pos_number}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_j1_2_{pos_number}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = len(neg_datalake_data)
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

                # 写入deepjoin.csv
                data_row = [query_name, neg_datalake_name, titles[target_col_index], titles[target_col_index], 0]
                with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                    writer = csv.writer(gt_csv)
                    writer.writerow(data_row)
            
            # 生成负例
            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(3, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count) 

                if target_col_index not in selected_col_indices:
                    selected_col_indices.append(target_col_index)
                            
                selected_row_count = random.randint(1, int(len(data2) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data2, selected_row_count)] 

                neg_datalake_name = f"dl{modified_name}_j1_2_{pos_number}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_j1_2_{pos_number}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = len(neg_datalake_data)
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

                # 写入deepjoin.csv
                data_row = [query_name, neg_datalake_name, titles[target_col_index], titles[target_col_index], 0]
                with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                    writer = csv.writer(gt_csv)
                    writer.writerow(data_row)

            # 生成负例
            for neg_number in range(neg_num*2 + 1, neg_num*3 + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(min_split_num - 3, min_split_num - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count) 
                            
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)] 

                neg_datalake_name = f"dl{modified_name}_j1_2_{pos_number}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_j1_2_{pos_number}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = len(neg_datalake_data)
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

                # 写入deepjoin.csv
                data_row = [query_name, neg_datalake_name, titles[target_col_index], titles[target_col_index], 0]
                with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                    writer = csv.writer(gt_csv)
                    writer.writerow(data_row)

          # 写入新表
        
        if pos_final:
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            
            # 随机选择template_num个模板
            selected_templates = query_templates[:template_num]
            
            # 写入query.txt
            with open(query_txt, 'a', encoding='utf-8') as q_txt:
                selected_template = random.choice(selected_templates)
                nl_query = selected_template.format(min_col_num=min_split_num)
                if random.choice([True, False]):
                    nl_query += 'with the topic related to {}'.format(original_table["caption"])
                q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")

    return index

def split_cellValue_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minCol=8, min_dup=0.3, max_dup=0.5, min_split_rate=0.2, template_num=3, shuffle=1, difficulty = 0.3, neg_num = 6, pos_num = 3):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_templates = [
        "I want to find more joinable tables, and these tables have keyword {caption}",
        "I am looking for additional joinable tables that cover keyword {caption}",
        "Can you find more joinable tables with keyword {caption}",
        "I need more joinable tables that contain keyword {caption}"
    ]
    
    # 遍历所有表格
    for table_key, original_table in tables.items():
        
        # 获取表格数据
        data = original_table["data"]
        titles = original_table["title"]
        
        # 如果需要打乱数据顺序
        if shuffle == 1:
            random.shuffle(data)
        
        # 确保分割前原表的行数不少于ori_minRow
        if len(data) <= ori_minCol:
            print(f"Table {table_key} has insufficient Cols for splitting.")
            continue

        # global index
        index += 1

        # 根据每列中含有唯一值数量，选取数量第二多的作为目标列
        unique_values_count = [(len(set(row[i] for row in data)), i) for i in range(len(titles))]
        unique_values_count.sort(reverse=True)
        target_col_index = unique_values_count[1][1] if len(unique_values_count) > 1 else 0

        # 将目标列的唯一值平均分成 data1 和 data2
        target_col_values = list(set([row[target_col_index] for row in data]))
        half_size = len(target_col_values) // 2
        random.shuffle(target_col_values)
        data1_values = list(target_col_values)[:half_size]
        data2_values = list(target_col_values)[half_size:]
        data1 = [row for row in data if row[target_col_index] in data1_values]
        data2 = [row for row in data if row[target_col_index] in data2_values]

        # 从剩余列中随机选出 duplicate
        remaining_cols = [i for i in range(len(titles)) if i != target_col_index]
        duplicate_count = random.randint(int(min_dup * len(remaining_cols)), int(max_dup * len(remaining_cols)))
        duplicate_cols = random.sample(remaining_cols, duplicate_count)

        # 随机选择分割比例
        split_rate = random.uniform(min_split_rate, 0.5)

        # 随机选择不重合列
        non_duplicate_cols = [i for i in remaining_cols if i not in duplicate_cols]
        undup1_cols = non_duplicate_cols[:int(len(non_duplicate_cols) * split_rate)]
        undup2_cols = [i for i in non_duplicate_cols if i not in undup1_cols]

        # 获取原表名并去掉前三个字母
        original_name = table_key
        modified_name = original_name[3:]



        # query-table和datalake-table
        query_data_row = random.sample(data1, int(len(data1) * 0.8))
        
        # 选择特定的列来构成query_data和datalake_data
        query_data = [[row[target_col_index]] + [row[i] for i in duplicate_cols] + [row[i] for i in undup1_cols] for row in query_data_row]
        
         # 构建新表名
        query_name = f"q{modified_name}_j1_3.json"
        query_name_nojson = f"q{modified_name}_j1_3"

        # 创建新的表格数据字典，保留原始表格的格式
        query_table_data = original_table.copy()

        # 更新分割后的表格数据
        query_table_data["data"] = query_data
        query_table_data["numDataRows"] = len(query_data)
        query_table_data["title"] = [titles[i] for i in [target_col_index] + duplicate_cols + undup1_cols]
        query_table_data["table_array"] = query_table_data["title"] + query_data
        query_table_data["numCols"] = len([target_col_index] + duplicate_cols + undup1_cols)

  
        

        cellValue_col = random.sample(undup2_cols,1)
        
        datalake_unrep_col_withoutcol = [i for i in undup2_cols if i not in cellValue_col]

        # 统计query_data中所有单元格值的出现频率
        cell_values = [row[cellValue_col[0]] for row in data1]
        value_counts = Counter(cell_values)
        
        # 选择排名前difficulty%的值
        top_values = [value for value, count in value_counts.most_common(int(difficulty * len(value_counts)) + 1)]
        
        # 从排名前difficulty%的值中随机选择一个作为caption
        selected_caption = random.choice(top_values)

        # 删除包含selected_caption的行
        data_with_caption = [row for row in data if selected_caption in row]
        
        datalake_with_caption = [row for row in data1 if selected_caption in row]
        
        # 删除包含selected_caption的行
        data_without_caption = [row for row in data if selected_caption not in row]

        datalake_without_caption = [row for row in data1 if selected_caption not in row]


        if len(data_without_caption) == 0:
            continue

        pos_final = 0
        for pos_number in range(1, 1 + pos_num):
            datalake_data_row = datalake_with_caption + random.sample(datalake_without_caption, int(len(datalake_without_caption) * 0.8))

            datalake_table_cols = [target_col_index]  + duplicate_cols + cellValue_col + random.sample(datalake_unrep_col_withoutcol,int(len(datalake_unrep_col_withoutcol)*0.8))

            datalake_data = [[row[i] for i in datalake_table_cols] for row in datalake_data_row]
            # datalake_data_undup2 = [[row[i] for i in undup2_cols] for row in datalake_data_row]
        
            datalake_name = f"dl{modified_name}_j1_3_{pos_number}.json"
            datalake_name_nojson = f"dl{modified_name}_j1_3_{pos_number}"
            
            datalake_table_data = original_table.copy()

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["title"] = [titles[i] for i in datalake_table_cols]
            datalake_table_data["table_array"] = datalake_table_data["title"] + datalake_data
            datalake_table_data["numCols"] = len(datalake_table_cols)

            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            # 写入deepjoin.csv
            data_row = [query_name, datalake_name, titles[target_col_index], titles[target_col_index], 1]
            with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                writer = csv.writer(gt_csv)
                writer.writerow(data_row)

            pos_final += 1

                # 生成负例
            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(3, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)

                if target_col_index in selected_col_indices:
                    selected_col_indices = [idx for idx in selected_col_indices if idx != target_col_index]
                            
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)] 

                neg_datalake_name = f"dl{modified_name}_j1_3_{pos_number}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_j1_3_{pos_number}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = len(neg_datalake_data)
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

                # 写入deepjoin.csv
                data_row = [query_name, neg_datalake_name, titles[target_col_index], titles[target_col_index], 0]
                with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                    writer = csv.writer(gt_csv)
                    writer.writerow(data_row)
            
            # 生成负例
            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(3, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count) 

                if target_col_index not in selected_col_indices:
                    selected_col_indices.append(target_col_index)
                            
                selected_row_count = random.randint(1, int(len(data2) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data2, selected_row_count)] 

                neg_datalake_name = f"dl{modified_name}_j1_3_{pos_number}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_j1_3_{pos_number}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = len(neg_datalake_data)
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

                # 写入deepjoin.csv
                data_row = [query_name, neg_datalake_name, titles[target_col_index], titles[target_col_index], 0]
                with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                    writer = csv.writer(gt_csv)
                    writer.writerow(data_row)

            # 生成负例
            for neg_number in range(neg_num*2 + 1, neg_num*3 + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(3, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)
                            
                if int(len(data_without_caption) * 0.5) <= 1:
                    continue
                selected_row_count = random.randint(1, int(len(data_without_caption) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data_without_caption, selected_row_count)] 

                neg_datalake_name = f"dl{modified_name}_j1_3_{pos_number}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_j1_3_{pos_number}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = len(neg_datalake_data)
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

                # 写入deepjoin.csv
                data_row = [query_name, neg_datalake_name, titles[target_col_index], titles[target_col_index], 0]
                with open('Join1/all/deepjoin.csv', 'a', encoding='utf-8') as gt_csv:
                    writer = csv.writer(gt_csv)
                    writer.writerow(data_row)


        if pos_final:           
            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            
            
            # 随机选择template_num个模板
            selected_templates = query_templates[:template_num]
            
            # 写入query.txt
            with open(query_txt, 'a', encoding='utf-8') as q_txt:
                selected_template = random.choice(selected_templates)
                nl_query = selected_template.format(caption=selected_caption)
                if random.choice([True, False]):
                    nl_query += 'with the topic related to {}'.format(original_table["caption"])        
                q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")

    return index



def process_dataset(exp, dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt):

    if exp == 'theme':
        for json_file in os.listdir(dataset_folder):
            if json_file.endswith('.json'):
                split_theme_table(
                    os.path.join(dataset_folder, json_file),
                    query_folder, 
                    datalake_folder, 
                    query_txt, 
                    groundtruth_txt
                )

    if exp == 'scaleRow':
        for json_file in os.listdir(dataset_folder):
            if json_file.endswith('.json'):
                split_scaleCol_table(
                    os.path.join(dataset_folder, json_file),
                    query_folder, 
                    datalake_folder, 
                    query_txt, 
                    groundtruth_txt
                )    

    if exp == 'cellValue':
        for json_file in os.listdir(dataset_folder):
            if json_file.endswith('.json'):
                split_cellValue_table(
                    os.path.join(dataset_folder, json_file),
                    query_folder, 
                    datalake_folder, 
                    query_txt, 
                    groundtruth_txt
                )    



# 调用函数


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--exp', type=str, required=True, help='experiment to run')
#     args = parser.parse_args()

# dataset_folder = 'qualified-ori-datalake2/datalake-1'
# query_folder = 'Union1/tableLevel/query-test'
# datalake_folder = 'Union1/tableLevel/datalake-test'
# query_txt = 'Union1/tableLevel/queries-test.txt'
# groundtruth_txt = 'Union1/tableLevel/qtrels-test.txt'

# if not os.path.exists(query_folder):
#     os.makedirs(query_folder)
# if not os.path.exists(datalake_folder):
#     os.makedirs(datalake_folder)

# process_dataset(args.exp,dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt)