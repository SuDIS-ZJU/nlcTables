import json
import os
import random
import argparse
from collections import Counter

# index = 0

def split_theme_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, max_duplicate=0.1, min_split_rate=0.2, template_num=3, shuffle=1, neg_num = 10,pos_num = 5):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_templates = [
        "I want to find more unionable tables, and these tables have topics related to {caption}.",
        "I am looking for additional tables that cover the same topic as {caption}.",
        "Can you find more tables with similar content to {caption}.",
        "I need more tables that are related to the topic of {caption}."
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
        if len(data) <= ori_minRow:
            print(f"Table {table_key} has insufficient rows for splitting.")
            continue

        # global index
        index += 1

        # 计算重叠行数
        split_duplicate = random.uniform(0, max_duplicate)
        overlap_rows = int(len(data) * split_duplicate)

        # 计算非重叠部分的最大分割比例
        max_split = len(data) - overlap_rows

        # 在min_split_rate和50%之间随机选择一个分割比例
        split_rate = random.uniform(min_split_rate, 0.5)
        split_index = int(max_split * split_rate)

        split_flag = random.choice([True, False])

        # 随机分割表格，确保重叠部分
        if split_flag:
            query_data = data[:split_index] + data[-overlap_rows:]
        else:
            query_data = data[split_index:-overlap_rows] + data[-overlap_rows:]


        
        # 随机选择分割比例
        rep_rate = random.uniform(min_split_rate, 0.5)
        
        # 计算重合列的数量
        num_rep_cols = int(len(titles) * rep_rate)
        
        # 随机选择重合列
        rep_col = random.sample(range(len(titles)), num_rep_cols)
        
        # 计算不重合列的数量
        num_unrep_cols = len(titles) - num_rep_cols
        
        # 选择不重合列
        unrep_col = [i for i in range(len(titles)) if i not in rep_col]
        
        # 按split_rate随机分成两部分query_unrep_col和datalake_unrep_col
        query_unrep_col = random.sample(unrep_col, int(num_unrep_cols * rep_rate))
        datalake_unrep_col = [i for i in unrep_col if i not in query_unrep_col]
        
        # 构建query table和datalake table的列
        query_table_cols = rep_col + query_unrep_col

        query_data = [[row[i] for i in query_table_cols] for row in query_data]

        # 获取原表名并去掉前三个字母
        original_name = table_key
        modified_name = original_name[3:]

        # 构建新表名
        query_name = f"q{modified_name}_1_1.json"
        query_name_nojson = f"q{modified_name}_1_1"

         # 创建新的表格数据字典，保留原始表格的格式
        query_table_data = original_table.copy()

        # 更新分割后的表格数据
        query_table_data["data"] = query_data
        query_table_data["numDataRows"] = len(query_data)
        query_table_data["title"] = [titles[i] for i in query_table_cols]
        query_table_data["table_array"] = query_table_data["title"] + query_data
        

        
        pos_final = 0
        for pos_number in range(1, pos_num + 1):

            if split_flag:
                datalake_data = data[split_index:-overlap_rows] + data[-overlap_rows:]
            else:
                datalake_data = data[:split_index] + data[-overlap_rows:]

            datalake_unrep_col_num = int(len(datalake_unrep_col)*random.uniform(0.2, 0.8))

            datalake_table_cols = rep_col + random.sample(datalake_unrep_col,datalake_unrep_col_num)

            datalake_data = [[row[i] for i in datalake_table_cols] for row in datalake_data]
            
            datalake_name = f"dl{modified_name}_1_1_{pos_number}.json"
            datalake_name_nojson = f"dl{modified_name}_1_1_{pos_number}"
        
            datalake_table_data = original_table.copy()
            
            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["title"] = [titles[i] for i in datalake_table_cols]
            datalake_table_data["table_array"] = datalake_table_data["title"] + datalake_data
            
            
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)            
            
            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            pos_final += 1

            # 生成负例
            # for neg_number in range(1, neg_num + 1):
            #     # 随机选择列数，确保少于原表
            #     selected_col_count = random.randint(1, len(titles) - 1)
            #     selected_col_indices = random.sample(range(len(titles)), selected_col_count)
                
            #     # 随机选择行数，确保不少于1行
            #     if len(data[:-overlap_rows])>1:
            #         selected_row_count = random.randint(1, int(len(data[:-overlap_rows]) * 0.5))
            #         neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data[:-overlap_rows], selected_row_count)] + [[row[i] for i in selected_col_indices] for row in data[-overlap_rows:]]
            #     else:
            #         selected_row_count = random.randint(1, int(len(data) * 0.5))
            #         neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)] 

            #     neg_datalake_name = f"dl{modified_name}_1_1_{pos_number}_n_{neg_number}.json"
            #     neg_datalake_name_nojson = f"dl{modified_name}_{pos_number}_1_1_n_{neg_number}"
            #     neg_datalake_table_data = original_table.copy()
            #     neg_datalake_table_data["data"] = neg_datalake_data
            #     neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
            #     neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
            #     neg_datalake_table_data["numCols"] = selected_col_count
            #     neg_datalake_table_data["numDataRows"] = len(neg_datalake_data)
                
            #     with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
            #         json.dump(neg_datalake_table_data, dl_file, indent=4)
            #     with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
            #         gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")
        
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

def split_scaleRow_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=500, min_split_num=200, min_split_rate=0.2, template_num=3, shuffle=1, neg_num = 10, pos_num = 5):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_templates = [
    "I want to find more unionable tables with the number of rows larger than {min_row_num}.",
    "I need to locate additional tables with a row count exceeding {min_row_num}.",
    "Searching for supplementary tables that have more than {min_row_num} rows to ensure sufficient data for our study.",
    "Looking to identify further tables with over {min_row_num} rows to merge and enhance our dataset."
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
        if len(data) <= ori_minRow:
            print(f"Table {table_key} has insufficient rows for splitting.")
            continue

        # global index
        index += 1

        # 确保datalake_data的数据量不少于min_split_num
        min_split_num = max(min_split_num, 1)  # 至少保证有1行数据
        
        # 剩余数据
        remaining_data = data[min_split_num:]
        
        # 在剩余数据中随机选择重叠部分
        overlap_rows = random.randint(1, len(remaining_data) // 2)
        overlap_data = remaining_data[-overlap_rows:]
        
        # 非重叠部分的剩余数据
        non_overlap_data = remaining_data[:-overlap_rows]
        
        # 随机选择一个分割比例，确保非重叠部分被随机分割
        max_split = len(non_overlap_data)
        split_index = random.randint(0, max_split)

        # 随机选择分割比例
        rep_rate = random.uniform(min_split_rate, 0.5)
        
        # 计算重合列的数量
        num_rep_cols = int(len(titles) * rep_rate)
        
        print("rep_rate",rep_rate)
        print("titles",titles)
        print("range(len(titles))",range(len(titles)))
        print("num_rep_cols",num_rep_cols)
        # 随机选择重合列
        rep_col = random.sample(range(len(titles)), num_rep_cols)
        
        # 计算不重合列的数量
        num_unrep_cols = len(titles) - num_rep_cols
        
        # 选择不重合列
        unrep_col = [i for i in range(len(titles)) if i not in rep_col]

        # 按split_rate随机分成两部分query_unrep_col和datalake_unrep_col
        query_unrep_col = random.sample(unrep_col, int(num_unrep_cols * rep_rate))
        datalake_unrep_col = [i for i in unrep_col if i not in query_unrep_col]
        

        # 获取原表名并去掉前三个字母
        original_name = table_key
        modified_name = original_name[3:]

        
        # 随机分割非重叠部分
        query_data_non_overlap = non_overlap_data[:split_index]

        query_data = query_data_non_overlap + overlap_data

        # 构建query table和datalake table的列
        query_table_cols = rep_col + query_unrep_col

        query_data = [[row[i] for i in query_table_cols] for row in query_data]


        # 构建新表名
        query_name = f"q{modified_name}_1_2.json"
        query_name_nojson = f"q{modified_name}_1_2"
        
        # 创建新的表格数据字典，保留原始表格的格式
        query_table_data = original_table.copy()

        # 更新分割后的表格数据
        query_table_data["data"] = query_data
        query_table_data["numDataRows"] = len(query_data)
        query_table_data["title"] = [titles[i] for i in query_table_cols]
        query_table_data["table_array"] = query_table_data["title"] + query_data
        
        pos_final = 0
        for pos_number in range(1, pos_num + 1):

            datalake_data_non_overlap = non_overlap_data[split_index:]
            
            datalake_data = data[:min_split_num] + datalake_data_non_overlap + overlap_data        
            
            datalake_unrep_col_num = int(len(datalake_unrep_col)*random.uniform(0.2, 0.8))

            datalake_table_cols = rep_col + random.sample(datalake_unrep_col,datalake_unrep_col_num)

            datalake_data = [[row[i] for i in datalake_table_cols] for row in datalake_data]

            datalake_name = f"dl{modified_name}_1_2_{pos_number}.json"
            datalake_name_nojson = f"dl{modified_name}_1_2_{pos_number}"
            
            datalake_table_data = original_table.copy()

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["title"] = [titles[i] for i in datalake_table_cols]
            datalake_table_data["table_array"] = datalake_table_data["title"] + datalake_data
            
            
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            pos_final += 1

            # 生成负例
            # for neg_number in range(1, neg_num + 1):
            #     # 随机选择列数，确保少于原表
            #     selected_col_count = random.randint(1, len(titles) - 1)
            #     selected_col_indices = random.sample(range(len(titles)), selected_col_count)
                
            #     # 随机选择行数，确保不少于1行
            #     selected_row_count = random.randint(1, int(len(data) * 0.5))
            #     neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
            #     # # 随机选择行数，确保不少于1行
            #     # if len(non_overlap_data)>1:
            #     #     selected_row_count = random.randint(1, int(len(non_overlap_data) * 0.5))
            #     #     neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(non_overlap_data, selected_row_count)] + [[row[i] for i in selected_col_indices] for row in overlap_data]
            #     # else:
            #     #     selected_row_count = random.randint(1, int(len(data) * 0.5))
            #     #     neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)] 

            #     neg_datalake_name = f"dl{modified_name}_1_2_n_{neg_number}.json"
            #     neg_datalake_name_nojson = f"dl{modified_name}_1_2_n_{neg_number}"
            #     neg_datalake_table_data = original_table.copy()
            #     neg_datalake_table_data["data"] = neg_datalake_data
            #     neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
            #     neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
            #     neg_datalake_table_data["numCols"] = selected_col_count
            #     neg_datalake_table_data["numDataRows"] = selected_row_count
                
            #     with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
            #         json.dump(neg_datalake_table_data, dl_file, indent=4)
            #     with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
            #         gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择小于min_split_num的行数
                selected_row_count = random.randint(min_split_num - 10, min_split_num - 1)
                neg_datalake_data = random.sample(data,selected_row_count)

                selected_col_count = random.randint(1, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)

                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in neg_datalake_data] 

                # if selected_row_count <= len(overlap_data):
                #     neg_datalake_data = random.sample(overlap_data, selected_row_count)
                # else:
                #     neg_datalake_data = random.sample(data,selected_row_count-len(overlap_data)) + overlap_data

                neg_datalake_name = f"dl{modified_name}_1_2_{pos_number}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_1_2_{pos_number}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")


        if pos_final:
            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)

            # 随机选择template_num个模板
            selected_templates = query_templates[:template_num]
            
            # 写入query.txt
            with open(query_txt, 'a', encoding='utf-8') as q_txt:
                selected_template = random.choice(selected_templates)
                nl_query = selected_template.format(min_row_num=min_split_num)
                if random.choice([True, False]):
                    nl_query += 'with the topic related to {}'.format(original_table["caption"])
                q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
    return index

def split_cellValue_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, max_duplicate=0.1, min_split_rate=0.2, template_num=3, shuffle=1, difficulty = 0.3, neg_num = 10,pos_num = 5):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_templates = [
        "I want to find more unionable tables, and these tables contain keyword: {caption}.",
        "I am looking for additional tables that cover keyword: {caption}.",
        "Can you find more tables that related to keyword: {caption}.",
        "Retrieve more tables that are correlated to keyword: {caption}."
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
        if len(data) <= ori_minRow:
            print(f"Table {table_key} has insufficient rows for splitting.")
            continue

        # global index
        index += 1

        # 计算重叠行数
        split_duplicate = random.uniform(0, max_duplicate)
        overlap_rows = int(len(data) * split_duplicate)

        # 计算非重叠部分的最大分割比例
        max_split = len(data) - overlap_rows

        # 在min_split_rate和50%之间随机选择一个分割比例
        split_rate = random.uniform(min_split_rate, 0.5)
        split_index = int(max_split * split_rate)

        # # 随机分割表格，确保重叠部分
        # if random.choice([True, False]):
        #     query_data = data[:split_index] + data[-overlap_rows:]
        #     datalake_data = data[split_index:-overlap_rows] + data[-overlap_rows:]
        # else:
        #     query_data = data[split_index:-overlap_rows] + data[-overlap_rows:]
        #     datalake_data = data[:split_index] + data[-overlap_rows:]

        split_flag = random.choice([True, False])

        # 随机分割表格，确保重叠部分
        if split_flag:
            query_data = data[:split_index] + data[-overlap_rows:]
        else:
            query_data = data[split_index:-overlap_rows] + data[-overlap_rows:]


        # 随机选择分割比例
        rep_rate = random.uniform(min_split_rate, 0.5)
        
        # 计算重合列的数量
        num_rep_cols = int(len(titles) * rep_rate)
        
        # 随机选择重合列
        rep_col = random.sample(range(len(titles)), num_rep_cols)
        
        # 计算不重合列的数量
        num_unrep_cols = len(titles) - num_rep_cols
        
        # 选择不重合列
        unrep_col = [i for i in range(len(titles)) if i not in rep_col]
        
        # 按split_rate随机分成两部分query_unrep_col和datalake_unrep_col
        query_unrep_col = random.sample(unrep_col, int(num_unrep_cols * rep_rate))
        datalake_unrep_col = [i for i in unrep_col if i not in query_unrep_col]
        
        # 构建query table和datalake table的列
        query_table_cols = rep_col + query_unrep_col

        query_data = [[row[i] for i in query_table_cols] for row in query_data]

        # 获取原表名并去掉前三个字母
        original_name = table_key
        modified_name = original_name[3:]

        # 构建新表名
        query_name = f"q{modified_name}_1_3.json"
        query_name_nojson = f"q{modified_name}_1_3"

        # 创建新的表格数据字典，保留原始表格的格式
        query_table_data = original_table.copy()

        # 更新分割后的表格数据
        query_table_data["data"] = query_data
        query_table_data["numDataRows"] = len(query_data)
        query_table_data["title"] = [titles[i] for i in query_table_cols]
        query_table_data["table_array"] = query_table_data["title"] + query_data

        if split_flag:
            datalake_data = data[split_index:-overlap_rows] + data[-overlap_rows:]
        else:
            datalake_data = data[:split_index] + data[-overlap_rows:]
        
        cellValue_col = random.sample(datalake_unrep_col,1)

        datalake_unrep_col_withoutcol = [i for i in datalake_unrep_col if i not in cellValue_col]

        # 统计query_data中所有单元格值的出现频率
        cell_values = [row[cellValue_col[0]] for row in datalake_data]
        value_counts = Counter(cell_values)
        
        # 选择排名前difficulty%的值
        top_values = [value for value, count in value_counts.most_common(int(difficulty * len(value_counts)) + 1)]
        
        # 从排名前difficulty%的值中随机选择一个作为caption
        selected_caption = random.choice(top_values)

        # 选择包含 selected_caption 的行
        data_with_caption = [row for row in data if selected_caption in row]

        datalake_with_caption = [row for row in datalake_data if selected_caption in row]
        
        # 删除包含selected_caption的行
        data_without_caption = [row for row in data if selected_caption not in row]

        datalake_without_caption = [row for row in datalake_data if selected_caption not in row]
        
        if len(data_without_caption) == 0:
            continue
        overlapData_without_caption = [row for row in data[-overlap_rows:] if selected_caption not in row]
        nonoverlapData_without_caption = [row for row in data[:-overlap_rows] if selected_caption not in row]

        
        pos_final = 0
        for pos_number in range(1, pos_num + 1):

            datalake_table_data = original_table.copy()

            datalake_name = f"dl{modified_name}_1_3_{pos_number}.json"
            datalake_name_nojson = f"dl{modified_name}_1_3_{pos_number}"

            datalake_data = datalake_with_caption + random.sample(datalake_without_caption,int(len(datalake_without_caption)*0.5))

            datalake_table_cols = rep_col + cellValue_col + random.sample(datalake_unrep_col_withoutcol,int(len(datalake_unrep_col_withoutcol)*0.5))

            datalake_data = [[row[i] for i in datalake_table_cols] for row in datalake_data]


            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["title"] = [titles[i] for i in datalake_table_cols]
            datalake_table_data["table_array"] = datalake_table_data["title"] + datalake_data

            # # 统计query_data中所有单元格值的出现频率
            # cell_values = [cell for row in datalake_data for cell in row]
            # value_counts = Counter(cell_values)
            
            # # 选择排名前difficulty%的值
            # top_values = [value for value, count in value_counts.most_common(int(difficulty * len(value_counts)) + 1)]
            
            # # 从排名前difficulty%的值中随机选择一个作为caption
            # selected_caption = random.choice(top_values)

            # # 删除包含selected_caption的行
            # data_without_caption = [row for row in data if selected_caption not in row]
            # if len(data_without_caption) == 0:
            #     continue
            # overlapData_without_caption = [row for row in data[-overlap_rows:] if selected_caption not in row]
            # nonoverlapData_without_caption = [row for row in data[:-overlap_rows] if selected_caption not in row]

           
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)       
            
            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            pos_final += 1

            # # 生成负例
            # for neg_number in range(1, neg_num + 1):
            #     # 随机选择列数，确保少于原表
            #     selected_col_count = random.randint(1, len(titles) - 1)
            #     selected_col_indices = random.sample(range(len(titles)), selected_col_count)
                
            #     # # 随机选择行数，确保不少于1行
            #     # selected_row_count = random.randint(1, int(len(data) * 0.5))
            #     # neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
            #     # 随机选择行数，确保不少于1行
            #     if len(nonoverlapData_without_caption)>1:
            #         selected_row_count = random.randint(1, int(len(nonoverlapData_without_caption) * 0.5))
            #         neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(nonoverlapData_without_caption, selected_row_count)] + [[row[i] for i in selected_col_indices] for row in overlapData_without_caption]
            #     else:
            #         selected_row_count = random.randint(1, int(len(data) * 0.5))
            #         neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)] 


            #     neg_datalake_name = f"dl{modified_name}_1_3_n_{neg_number}.json"
            #     neg_datalake_name_nojson = f"dl{modified_name}_1_3_n_{neg_number}"
            #     neg_datalake_table_data = original_table.copy()
            #     neg_datalake_table_data["data"] = neg_datalake_data
            #     neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
            #     neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
            #     neg_datalake_table_data["numCols"] = selected_col_count
            #     neg_datalake_table_data["numDataRows"] = len(neg_datalake_data)
                
            #     with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
            #         json.dump(neg_datalake_table_data, dl_file, indent=4)
            #     with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
            #         gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
                
                if len(nonoverlapData_without_caption)>1:
                    selected_row_count = random.randint(1, len(nonoverlapData_without_caption))
                    neg_datalake_data = random.sample(nonoverlapData_without_caption, selected_row_count)  + overlapData_without_caption
                else:
                    selected_row_count = random.randint(1, len(data_without_caption))
                    neg_datalake_data = random.sample(data_without_caption, selected_row_count)

                selected_col_count = random.randint(1, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)

                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in neg_datalake_data] 
                
                neg_datalake_name = f"dl{modified_name}_1_3_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_1_3_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

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
                    groundtruth_txt,
                    10, 0.1, 0.2, 3, 1
                )

    if exp == 'scaleRow':
        for json_file in os.listdir(dataset_folder):
            if json_file.endswith('.json'):
                split_scaleRow_table(
                    os.path.join(dataset_folder, json_file),
                    query_folder, 
                    datalake_folder, 
                    query_txt, 
                    groundtruth_txt,
                    500, 200, 3, 1
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