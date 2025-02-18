import json
import os
import random
import argparse
from collections import Counter
import numpy as np


def split_scale_category_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, col_scope=0.6, max_duplicate=0.1, min_split_rate=0.2, template_num=3, shuffle=1, difficulty = 0.3, neg_num = 10, min_split_num = 100):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_templates = [
        "I want to find more large-scale unionable tables having more than {scale} rows, and these tables have keyword: {caption}.",
        "I need to identify additional large-scale unionable tables with over {scale} rows, which include the keyword: {caption}.",
        "I am looking for further large-scale tables that can be unioned, having more than {scale} rows, and containing the keyword: {caption}.",
        "I want to find more extensive unionable tables with a row count exceeding {scale}, and these tables should have the keyword: {caption}."
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

        if len(data) >= 500:
            data = data[:500]  # 取前500条数据
        else:
            continue

        categorical_cols = [i for i in range(len(original_table['title'])) if i not in original_table['numericColumns'] and i not in original_table['dateColumns']]
        unique_values_count = [
        len(set(row[i] for row in original_table['data'] if row[i].strip() != '')) 
        for i in categorical_cols
        ]    #ori-table需要标注列类别标签，代码也需要小幅度修改

        # 按唯一值数量排序
        sorted_cols = sorted(zip(categorical_cols, unique_values_count), key=lambda x: x[1], reverse=True)

        # 过滤掉唯一值数量为1的列
        sorted_cols = [(col, count) for col, count in sorted_cols if count > 1]

        # 如果过滤后没有列，则不选择任何列
        if not sorted_cols:
            continue
        else:
            # 计算前60%的列数量
            num_cols_to_select = int(len(sorted_cols) * col_scope)

        # 选择前60%的列
        selected_cols = [col for col, count in sorted_cols[:num_cols_to_select]]

        split_num = int(len(selected_cols) * 0.5)

        selected_cols = random.sample(selected_cols, split_num)

        num_i = 0
        for col in selected_cols:

            # global index
            index += 1
            num_i += 1 

            unique_cats = list(set([row[col] for row in original_table['data']]))

            value_counts = Counter(unique_cats)
        
            # 选择排名前difficulty%的值
            top_values = [value for value, count in value_counts.most_common(int(difficulty * len(value_counts)) + 1)]
            
            # 从排名前difficulty%的值中随机选择一个作为caption
            selected_caption = random.choice(top_values)

            # 先把原表中包含 selected_caption 的所有行中的至少80%给 datalake_data
            data_with_caption = [row for row in original_table['data'] if row[col] == selected_caption]
            
            # 随机决定给 datalake_data 的比例（80% - 100%）
            datalake_proportion = random.uniform(0.8, 1.0)
            datalake_data = data_with_caption[:int(len(data_with_caption) * datalake_proportion)]

            # **********************这里也是改变随机比例

            # 对于 query_data，从包含 selected_caption 的行中随机选择0%到50%的比例
            query_proportion = random.uniform(0, 0.5)
            query_data = data_with_caption[:int(len(data_with_caption) * query_proportion)]

            # 处理除了 selected_caption 的整个表格
            data_without_caption = [row for row in original_table['data'] if row[col] != selected_caption]

            # 计算重叠行数
            split_duplicate = random.uniform(0, max_duplicate)
            overlap_rows = int(len(data_without_caption) * split_duplicate)

            # 计算非重叠部分的最大分割比例
            max_split = len(data_without_caption) - overlap_rows

            # 在 min_split_rate 和 50% 之间随机选择一个分割比例
            split_rate = random.uniform(min_split_rate, 0.5)
            split_index = int(max_split * split_rate)

            # # 随机分割表格，确保重叠部分
            # if random.choice([True, False]):
            #     query_data += data_without_caption[:split_index] + data_without_caption[-overlap_rows:]
            #     datalake_data += data_without_caption[split_index:-overlap_rows] + data_without_caption[-overlap_rows:]
            # else:
            #     query_data += data_without_caption[split_index:-overlap_rows] + data_without_caption[-overlap_rows:]
            #     datalake_data += data_without_caption[:split_index] + data_without_caption[-overlap_rows:]

            query_data += data_without_caption[:split_index] + data_without_caption[-overlap_rows:]
            datalake_data += data_without_caption[split_index:-overlap_rows] + data_without_caption[-overlap_rows:]

            # 获取原表名并去掉前三个字母
            original_name = table_key
            modified_name = original_name[3:]
            
            query_name = f"q{table_key[3:]}_3_1_{num_i}.json"
            query_name_nojson = f"q{table_key[3:]}_3_1_{num_i}"
            datalake_name = f"dl{table_key[3:]}_3_1_{num_i}.json"
            datalake_name_nojson = f"dl{table_key[3:]}_3_1_{num_i}"

            # 创建新的表格数据字典，保留原始表格的格式
            query_table_data = original_table.copy()
            datalake_table_data = original_table.copy()

            if len(datalake_data) < min_split_num:
                continue

            # 更新分割后的表格数据
            query_table_data["data"] = query_data
            query_table_data["numDataRows"] = len(query_data)
            query_table_data["table_array"] = titles + query_data

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["table_array"] = titles + datalake_data


            # 删除包含selected_caption的行
            data_without_caption = [row for row in data if selected_caption not in row]
            if len(data_without_caption) == 0:
                continue

            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            # 随机选择template_num个模板
            selected_templates = query_templates[:template_num]
            
            # 写入query.txt
            with open(query_txt, 'a', encoding='utf-8') as q_txt:
                selected_template = random.choice(selected_templates)
                nl_query = selected_template.format(caption=selected_caption,scale=min_split_num)
                q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            # 生成负例
            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(1, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)
                
                # 随机选择行数，确保不少于1行
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
                neg_datalake_name = f"dl{modified_name}_3_1_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_1_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
                print(query_name)
                print(len(data_without_caption))
                selected_row_count = random.randint(1, min(len(data_without_caption), ori_minRow))
                neg_datalake_data = random.sample(data_without_caption, selected_row_count)
                
                neg_datalake_name = f"dl{modified_name}_3_1_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_1_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")  

            for neg_number in range(2*neg_num + 1, 3*neg_num + 1):
                # 随机选择小于min_split_num的行数
                selected_row_count = random.randint(min_split_num - 10, min_split_num - 1)
                neg_datalake_data = random.sample(data,selected_row_count)

                # if selected_row_count <= len(overlap_data):
                #     neg_datalake_data = random.sample(overlap_data, selected_row_count)
                # else:
                #     neg_datalake_data = random.sample(data,selected_row_count-len(overlap_data)) + overlap_data

                neg_datalake_name = f"dl{modified_name}_1_3_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_1_3_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")
    
    return index

import re
import unicodedata
from datetime import datetime

def parse_value(value):
    """尝试将值转换为浮点数，忽略无法转换的值"""
    # 去除字符串开头和结尾的空格和特殊字符
    value = unicodedata.normalize('NFKD', value).encode('ASCII', 'ignore').decode('ASCII').strip()

    # 尝试直接转换为浮点数
    try:
        return float(value)
    except (ValueError, TypeError):
        pass

    match = re.match(r'^\d{1,3}(?:,\d{3})*(?:\.\d+)?$', value.replace(',', ''))
    if match:
        try:
            # 直接转换处理后的字符串为浮点数
            return float(value.replace(',', ''))
        except (ValueError, TypeError):
            pass

    # 检查是否符合 "+X.X%" 或 "-X.X%" 格式
    if value.endswith('%'):
        try:
            percent_value = float(value[:-1].replace(',', ''))  # 去掉百分号和逗号
            return percent_value / 100  # 返回百分比的小数形式
        except (ValueError, TypeError):
            pass

    # 检查是否符合 “￥X” 格式
    if value.startswith('$'):
        try:
            return float(value[1:].replace(',', ''))  # 去掉货币符号和逗号
        except (ValueError, TypeError):
            pass

    # 检查是否符合 “X.X ℉” 格式
    if '℉' in value:
        try:
            return float(value.replace('℉', ''))  # 去掉 '℉'
        except (ValueError, TypeError):
            pass

    # 检查是否符合 “X.X in” 格式
    if 'in' in value:
        try:
            return float(value.replace('in', ''))  # 去掉 'in'
        except (ValueError, TypeError):
            pass

    # 检查是否符合 “X.X mi” 格式
    if 'mi' in value:
        try:
            return float(value.replace('mi', ''))  # 去掉 'mi'
        except (ValueError, TypeError):
            pass

    # 检查是否符合 “X.X GB” 格式
    if 'GB' in value:
        try:
            return float(value.replace('GB', ''))  # 去掉 'GB'
        except (ValueError, TypeError):
            pass

    # 检查是否符合 “X.X MB/Sec” 格式
    if 'MB/Sec' in value:
        try:
            return float(value.replace('MB/Sec', ''))  # 去掉 'MB/Sec'
        except (ValueError, TypeError):
            pass

    if 'mm^3' in value:
        try:
            return float(value.replace('mm^3', ''))  # 去掉 'MB/Sec'
        except (ValueError, TypeError):
            pass
    
    if 'points' in value:
        try:
            return float(value.replace('points', ''))
        except (ValueError, TypeError):
            pass

    if 'Minutes' in value:
        try:
            return float(value.replace('Minutes', ''))
        except (ValueError, TypeError):
            pass

    if 'stars' in value:
        try:
            return float(value.replace('stars', ''))
        except (ValueError, TypeError):
            pass
    
    if '\u00b0F' in value:
        try:
            return float(value.replace('\u00b0F', ''))
        except (ValueError, TypeError):
            pass

    if '\u00b0N' in value:
        try:
            return float(value.replace('\u00b0N', ''))
        except (ValueError, TypeError):
            pass

    if '\u00b0W' in value:
        try:
            return float(value.replace('\u00b0W', ''))
        except (ValueError, TypeError):
            pass

    if '(\u221210)' in value:
        try:
            return float(value.replace('(\u221210)', ''))
        except (ValueError, TypeError):
            pass


    # 检查是否符合 “.X” 格式
    if value.startswith('.'):
        try:
            return float(value)  # 尝试转换为浮点数
        except (ValueError, TypeError):
            pass

    # 检查是否符合 “#XXXXX” 格式
    if value.startswith('#'):
        try:
            return float(value[1:])  # 去掉 '#' 并尝试转换为浮点数
        except (ValueError, TypeError):
            pass

    # 检查是否符合 “X.Xmph” 格式
    if 'mph' in value:
        try:
            return float(value.replace('mph', ''))  # 去掉 'mph'
        except (ValueError, TypeError):
            pass

    # 检查是否符合 “X.X*” 格式
    if value.endswith('*'):
        try:
            return float(value[:-1])  # 去掉 '*' 并尝试转换为浮点数
        except (ValueError, TypeError):
            pass

    # 如果所有转换尝试都失败，则返回 None
    return None

def split_scale_numerical_larger_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, datalake_quality=0.8, neg_num = 10, shuffle=1, template_num=3, min_split_num = 100):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_larger_templates = [
        "I am looking for more large-scale unionable tables that have {caption} larger than {seg_num} and contain more than {scale} rows.",
        "I need to identify additional large-scale tables that are unionable, with {caption} exceeding {seg_num} and comprising more than {scale} rows.",
        "I am searching for further large-scale unionable tables, where {caption} is greater than {seg_num} and the tables have over {scale} rows.",
        "I want to find more extensive unionable tables with {caption} larger than {seg_num} and containing more than {scale} rows."
    ]

    query_average_templates = [
        "I am searching for more large-scale unionable tables where {caption} is approximately {template_average} and the tables contain more than {scale} rows.",
        "I need to identify additional large-scale tables that are unionable, with {caption} close to {template_average} and having over {scale} rows.",
        "I am looking for further large-scale unionable tables, where {caption} is around {template_average} and the tables include more than {scale} rows.",
        "I want to find more extensive unionable tables with {caption} near {template_average} and containing more than {scale} rows."
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

        if len(data) >= 1000:
            data = data[:1000]  # 取前500条数据
        else:
            continue

        num_cols = original_table['numericColumns']

        if not num_cols:
            continue    #ori-table需要标注列类别标签，代码也需要小幅度修改

        # split_num = int(len(num_cols) * 0.5)
        # split_num = 1
        split_num = min(1,int(len(num_cols) * 0.2))

        selected_cols = random.sample(num_cols, split_num)

        num_i = 0
        for col in selected_cols:

            # global index
            index += 1
            
            num_i += 1 

            col_data = []
            clean_data = []
            unclean_data = []

            for row in data:
                # 尝试解析并移除无效值
                parsed_value = parse_value(row[col])
                if parsed_value is None:
                    unclean_data.append(row)
                else:
                    col_data.append(parsed_value)
                    clean_data.append(row)

            # col_data = [float(row[col]) for row in data]
            
            if len(col_data) < 10:
                continue

            segnum = np.percentile(col_data, random.uniform(20, 80))

            # 将数据分为大于和小于等于 segnum 的两组
            above_segnum_data = [row for row in clean_data if parse_value(row[col]) > segnum]
            below_or_equal_segnum_data = [row for row in clean_data if parse_value(row[col]) <= segnum]

            # 从大于 segnum 的组中随机选择 80%-100% 的行作为 datalake_data 的一部分
            datalake_above_proportion = random.uniform(0.8, 1.0)
            datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * datalake_above_proportion)]

            # 计算 datalake-qualifynum
            datalake_qualifynum = len(datalake_above_segnum)
            print("datalake_qualifynum:", datalake_qualifynum)

            # 从小于等于 segnum 的组中随机选择 0 到 datalake-qualifynum/4 的行作为 datalake_data 的另一部分
            datalake_below_proportion = random.uniform(0, min(len(below_or_equal_segnum_data),datalake_qualifynum / 4))
            datalake_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * datalake_below_proportion)]

            # 合并 datalake_data
            datalake_data = datalake_above_segnum + datalake_below_segnum

            print("datalake_data:",len(datalake_data))
            if len(datalake_data) < 10:
                continue
            average = round(np.mean([parse_value(row[col]) for row in datalake_data]))

            # 从大于 segnum 的组中随机选择 0%-30% 的行作为 query_data 的一部分
            query_above_proportion = random.uniform(0, 0.3)
            query_above_segnum = above_segnum_data[int(len(above_segnum_data) * (1 - query_above_proportion)):]

            # 从小于等于 segnum 的组中随机选择 20%-80% 的行作为 query_data 的另一部分
            query_below_proportion = random.uniform(0.2, 0.8)
            query_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * query_below_proportion)]

            # 合并 query_data
            query_data = query_above_segnum + query_below_segnum
            
            # 将unclean_data按随机比例分配给query_data和datalake_data
            unclean_proportion = random.uniform(0.2, 0.8)
            selected_unclean_for_query = random.sample(unclean_data, int(len(unclean_data) * unclean_proportion))
            query_data += selected_unclean_for_query

            # 获取原表名并去掉前三个字母
            original_name = table_key
            modified_name = original_name[3:]
            
            query_name = f"q{table_key[3:]}_3_2_{num_i}.json"
            query_name_nojson = f"q{table_key[3:]}_3_2_{num_i}"
            datalake_name = f"dl{table_key[3:]}_3_2_{num_i}.json"
            datalake_name_nojson = f"dl{table_key[3:]}_3_2_{num_i}"

            # 创建新的表格数据字典，保留原始表格的格式
            query_table_data = original_table.copy()
            datalake_table_data = original_table.copy()

            # 更新分割后的表格数据
            query_table_data["data"] = query_data
            query_table_data["numDataRows"] = len(query_data)
            query_table_data["table_array"] = titles + query_data

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["table_array"] = titles + datalake_data

            if len(datalake_data) < min_split_num:
                continue


            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            if random.choice([True, False]):
                # 随机选择template_num个模板
                selected_templates = query_larger_templates[:template_num]
            
                # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], seg_num=segnum, scale=min_split_num)
                    # nl_query += f" having more than {min_split_num} rows"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
                # 写入groundtruth.txt
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            else:
                # 随机选择template_num个模板
                selected_templates = query_average_templates[:template_num]
            
                # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], template_average=average, scale=min_split_num)
                    # nl_query += f" having more than {min_split_num} rows"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
                # 写入groundtruth.txt
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            # 生成负例
            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(1, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)
                
                # 随机选择行数，确保不少于1行
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue

                neg_datalake_name = f"dl{modified_name}_3_2_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_2_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
            
                # 从小于等于 segnum 的组中随机选择 60%-80% 的行作为 datalake_data 的一部分
                neg_datalake_below_proportion = random.uniform(0.6, 0.8)
                neg_datalake_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * neg_datalake_below_proportion)]

                # 计算 datalake-qualifynum
                neg_datalake_qualifynum = len(neg_datalake_below_segnum)
                # print("neg_datalake_qualifynum:", neg_datalake_qualifynum)

                # 从大于 segnum 的组中随机选择 0 到 datalake-qualifynum 的行作为 datalake_data 的另一部分
                neg_datalake_above_proportion = random.uniform(0, min(len(above_segnum_data),neg_datalake_qualifynum))
                neg_datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_above_proportion)]

                # 合并 datalake_data
                neg_datalake_data = neg_datalake_below_segnum + neg_datalake_above_segnum

                # 从 unclean_data 中随机选择一定比例的数据添加到 neg_datalake_data
                unclean_proportion = random.uniform(0.1, 0.3)  # 假设我们想要添加 10%-30% 的 unclean_data
                num_unclean_to_add = int(len(unclean_data) * unclean_proportion)
                added_unclean_data = random.sample(unclean_data, num_unclean_to_add)
                neg_datalake_data += added_unclean_data

                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue

                neg_datalake_name = f"dl{modified_name}_3_2_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_2_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 

            for neg_number in range(2*neg_num + 1, 3*neg_num + 1):
                # 随机选择小于min_split_num的行数
                selected_row_count = random.randint(min_split_num - 10, min_split_num - 1)
                neg_datalake_data = random.sample(data,selected_row_count)

                # if selected_row_count <= len(overlap_data):
                #     neg_datalake_data = random.sample(overlap_data, selected_row_count)
                # else:
                #     neg_datalake_data = random.sample(data,selected_row_count-len(overlap_data)) + overlap_data

                neg_datalake_name = f"dl{modified_name}_3_2_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_2_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 


    return index

def split_scale_numerical_smaller_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, datalake_quality=0.8, neg_num = 10, shuffle=1, template_num=3, min_split_num=100):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_larger_templates = [
        "I am looking for additional large-scale unionable tables where {caption} is less than {seg_num} and the tables contain more than {scale} rows.",
        "I need to identify more large-scale tables that are unionable, with {caption} smaller than {seg_num} and having over {scale} rows.",
        "I am searching for further large-scale unionable tables, where {caption} is smaller than {seg_num} and the tables include more than {scale} rows.",
        "I want to find more extensive unionable tables with {caption} less than {seg_num} and containing more than {scale} rows."
    ]

    query_average_templates = [
        "I am searching for more large-scale unionable tables where {caption} is approximately {template_average} and the tables contain more than {scale} rows.",
        "I need to identify additional large-scale tables that are unionable, with {caption} close to {template_average} and having over {scale} rows.",
        "I am looking for further large-scale unionable tables, where {caption} is around {template_average} and the tables include more than {scale} rows.",
        "I want to find more extensive unionable tables with {caption} near {template_average} and containing more than {scale} rows."
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
        
        print(len(data))
        if len(data) >= 500:
            data = data[:500]  # 取前500条数据
        else:
            continue

        num_cols = original_table['numericColumns']

        if not num_cols:
            continue    #ori-table需要标注列类别标签，代码也需要小幅度修改

        split_num = int(len(num_cols) * 0.5)

        selected_cols = random.sample(num_cols, split_num)

        num_i = 0
        for col in selected_cols:

            # global index
            index += 1
            
            num_i += 1 

            col_data = []
            clean_data = []
            unclean_data = []

            # col_data = [float(row[col]) for row in data]
            for row in data:
                # 尝试解析并移除无效值
                parsed_value = parse_value(row[col])
                if parsed_value is None:
                    unclean_data.append(row)
                else:
                    col_data.append(parsed_value)
                    clean_data.append(row)

            if len(col_data) < 10:
                continue

            segnum = np.percentile(col_data, random.uniform(20, 80))

            # 将数据分为大于和小于等于 segnum 的两组
            above_segnum_data = [row for row in clean_data if parse_value(row[col]) > segnum]
            below_or_equal_segnum_data = [row for row in clean_data if parse_value(row[col]) <= segnum]

            # 从大于 segnum 的组中随机选择 80%-100% 的行作为 datalake_data 的一部分
            datalake_below_proportion = random.uniform(0.8, 1.0)
            datalake_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * datalake_below_proportion)]

            # 计算 datalake-qualifynum
            datalake_qualifynum = len(datalake_below_segnum)
            # print("datalake_qualifynum:", datalake_qualifynum)

            # 从小于等于 segnum 的组中随机选择 0 到 datalake-qualifynum/4 的行作为 datalake_data 的另一部分
            datalake_above_proportion = random.uniform(0, min(len(above_segnum_data),datalake_qualifynum / 4))
            datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * datalake_above_proportion)]

            # 合并 datalake_data
            datalake_data = datalake_below_segnum + datalake_above_segnum

            print("datalake_data:",len(datalake_data))
            if len(datalake_data) < 10:
               continue

            # 计算 datalake_data 在 row[col] 的平均值
            average = round(np.mean([parse_value(row[col]) for row in datalake_data]))

            # 从大于 segnum 的组中随机选择 0%-30% 的行作为 query_data 的一部分
            query_below_proportion = random.uniform(0, 0.3)
            query_below_segnum = below_or_equal_segnum_data[int(len(below_or_equal_segnum_data) * (1 - query_below_proportion)):]

            # 从小于等于 segnum 的组中随机选择 20%-80% 的行作为 query_data 的另一部分
            query_above_proportion = random.uniform(0.2, 0.8)
            query_above_segnum = above_segnum_data[:int(len(above_segnum_data) * query_above_proportion)]

            # 合并 query_data
            query_data = query_below_segnum + query_above_segnum 
            

            # 获取原表名并去掉前三个字母
            original_name = table_key
            modified_name = original_name[3:]
            
            query_name = f"q{table_key[3:]}_3_2_{num_i}.json"
            query_name_nojson = f"q{table_key[3:]}_3_2_{num_i}"
            datalake_name = f"dl{table_key[3:]}_3_2_{num_i}.json"
            datalake_name_nojson = f"dl{table_key[3:]}_3_2_{num_i}"

            # 创建新的表格数据字典，保留原始表格的格式
            query_table_data = original_table.copy()
            datalake_table_data = original_table.copy()

            # 更新分割后的表格数据
            query_table_data["data"] = query_data
            query_table_data["numDataRows"] = len(query_data)
            query_table_data["table_array"] = titles + query_data

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["table_array"] = titles + datalake_data

            if len(datalake_data) < min_split_num:
                continue

            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            if random.choice([True, False]):
                # 随机选择template_num个模板
                selected_templates = query_larger_templates[:template_num]
            
                # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], seg_num=segnum, scale=min_split_num)
                    # nl_query += f" having more than {min_split_num} rows"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
            # 写入groundtruth.txt
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")
            
            else:
                # 随机选择template_num个模板
                selected_templates = query_average_templates[:template_num]
            
                # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], template_average=average, scale=min_split_num)
                    # nl_query += f" having more than {min_split_num} rows"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
            # 写入groundtruth.txt
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            

            # 生成负例
            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(1, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)
                
                # 随机选择行数，确保不少于1行
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue


                neg_datalake_name = f"dl{modified_name}_3_2_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_2_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
            
                # 从小于等于 segnum 的组中随机选择 60%-80% 的行作为 datalake_data 的一部分
                neg_datalake_above_proportion = random.uniform(0.6, 0.8)
                neg_datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_above_proportion)]

                # 计算 datalake-qualifynum
                neg_datalake_qualifynum = len(neg_datalake_above_segnum)
                print("neg_datalake_qualifynum:", neg_datalake_qualifynum)

                # 从大于 segnum 的组中随机选择 0 到 datalake-qualifynum 的行作为 datalake_data 的另一部分
                neg_datalake_below_proportion = random.uniform(0, min(len(below_or_equal_segnum_data),neg_datalake_qualifynum))
                neg_datalake_below_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_below_proportion)]

                # 合并 datalake_data
                neg_datalake_data = neg_datalake_above_segnum + neg_datalake_below_segnum

                # 从 unclean_data 中随机选择一定比例的数据添加到 neg_datalake_data
                unclean_proportion = random.uniform(0.1, 0.3)  # 假设我们想要添加 10%-30% 的 unclean_data
                num_unclean_to_add = int(len(unclean_data) * unclean_proportion)
                added_unclean_data = random.sample(unclean_data, num_unclean_to_add)
                neg_datalake_data += added_unclean_data
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue


                neg_datalake_name = f"dl{modified_name}_3_2_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_2_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 
            
            for neg_number in range(2*neg_num + 1, 3*neg_num + 1):
                # 随机选择小于min_split_num的行数
                selected_row_count = random.randint(min_split_num - 10, min_split_num - 1)
                neg_datalake_data = random.sample(data,selected_row_count)

                # if selected_row_count <= len(overlap_data):
                #     neg_datalake_data = random.sample(overlap_data, selected_row_count)
                # else:
                #     neg_datalake_data = random.sample(data,selected_row_count-len(overlap_data)) + overlap_data

                neg_datalake_name = f"dl{modified_name}_3_2_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_2_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

    
    return index

global_fmt = None
import copy

def parse_date1(date_str):
    """检查日期字符串是否为脏值"""
    # 支持的日期格式列表
    date_formats = [
        "%Y/%m/%d",  # 年/月/日
        "%Y.%m.%d",  # 年.月.日
        "%Y-%m-%d",  # 年-月-日
        "%d-%m-%Y %H:%M:%S",  # 日-月-年 时:分:秒
        "%d.%m.%Y %H:%M",  # 日-月-年 时:分:秒
        "%m/%d/%Y %I:%M:%S %p",  # 日-月-年 时:分:秒
        "%m/%d/%Y %H:%M",  # 日-月-年 时:分
        "%m/%d/%Y",   # 月/日/年
        "%d/%m/%Y",   # 日/月/年
        "%d.%m.%Y",   # 日/月/年
        "%d %B %Y", # 日 月 年
        "%d %b %Y",
        "%d-%b-%y",       # 日-月缩写-年（例如：01-Nov-16）
        "%b %d, %y",
        "%m/%d",   # 月/日
        "%m/%Y",   # 月/年
        "%b%y",   # 月/年
        "%Y-%m-%dT%H:%M:%S.%fZ",  # 年-月-日T时:分:秒.毫秒Z（UTC）
        "%I:%M %p",  # 时:分 %p（上下午）
        "%b %d",     # 月缩写 日
        "%A, %b %d",  # 星期全称，月缩写，日
        "%H:%M:%S",
        "%Hh%Mm%Ss",
        "%B", #月
    ]

    for fmt in date_formats:
        try:
            datetime.strptime(date_str, fmt)
            return True  # 返回匹配的格式
        except ValueError:
            continue
    # 如果所有格式都不匹配，则返回 None
    return None

def parse_date2(date_str):
    """解析日期字符串为日期对象"""
    
    global global_fmt
    if global_fmt:
        try:
            return datetime.strptime(date_str, global_fmt)
        except ValueError:
            raise ValueError(f"Date {date_str} does not match the global format {global_fmt}")

def parse_date3(date_str):
    """解析日期字符串为日期对象"""
    # 支持的日期格式列表
    date_formats = [
        "%Y/%m/%d",  # 年/月/日
        "%Y.%m.%d",  # 年.月.日
        "%Y-%m-%d",  # 年-月-日
        "%d-%m-%Y %H:%M:%S",  # 日-月-年 时:分:秒
        "%d.%m.%Y %H:%M",  # 日-月-年 时:分:秒
        "%m/%d/%Y %I:%M:%S %p",  # 日-月-年 时:分:秒
        "%m/%d/%Y %H:%M",  # 日-月-年 时:分
        "%m/%d/%Y",   # 月/日/年
        "%d/%m/%Y",   # 日/月/年
        "%d.%m.%Y",   # 日/月/年
        "%d %B %Y", # 日 月 年
        "%d %b %Y",
        "%d-%b-%y",       # 日-月缩写-年（例如：01-Nov-16）
        "%b %d, %y",
        "%m/%d",   # 月/日
        "%m/%Y",   # 月/年
        "%b%y",   # 月/年
        "%Y-%m-%dT%H:%M:%S.%fZ",  # 年-月-日T时:分:秒.毫秒Z（UTC）
        "%I:%M %p",  # 时:分 %p（上下午）
        "%b %d",     # 月缩写 日
        "%A, %b %d",  # 星期全称，月缩写，日
        "%H:%M:%S",
        "%Hh%Mm%Ss",
        "%B" #月
    ]
    
    # 尝试使用每个支持的日期格式解析日期字符串
    for fmt in date_formats:
        print("******************************")
        try:
            # 如果解析成功，更新全局日期格式并返回日期对象   
            global global_fmt
            global_fmt = fmt
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    # 如果所有格式都不匹配，则抛出异常
    raise ValueError(f"Date {date_str} is not in the correct format")

def split_scale_larger_date_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, neg_num=10, shuffle=1, template_num=3, min_split_num=100):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_larger_templates = [
        "I am searching for additional unionable tables where {caption} is later than {seg_num} and the tables contain more than {scale} rows.",
        "I need to identify more unionable tables with {caption} after {seg_num} and having over {scale} rows.",
        "I am looking for more unionable datasets that include {caption} dates following {seg_num} and the tables include more than {scale} rows.",
        "Are there any other unionable tables showing {caption} later than {seg_num} and containing more than {scale} rows?"
    ]

    query_average_templates = [
        "I am looking for additional unionable tables where {caption} is approximately {template_average} and the tables contain more than {scale} rows.",
        "I need to identify more unionable tables with {caption} around {template_average} and having over {scale} rows.",
        "I am searching for further unionable tables, where {caption} is close to {template_average} and the tables include more than {scale} rows.",
        "I want to find more unionable tables with {caption} near {template_average} and containing more than {scale} rows."
    ]

    date_formats = [
        "%Y/%m/%d",  # 年/月/日
        "%Y.%m.%d",  # 年.月.日
        "%Y-%m-%d",  # 年-月-日
        "%d/%m/%Y",   # 日/月/年
        "%d-%m-%Y %H:%M:%S",  # 日-月-年 时:分:秒
        "%d.%m.%Y %H:%M",  # 日-月-年 时:分:秒
        "%m/%d/%Y %I:%M:%S %p",  # 日-月-年 时:分:秒
        "%m/%d/%Y %H:%M",  # 日-月-年 时:分
        "%m/%d/%Y",   # 月/日/年        
        "%d.%m.%Y",   # 日/月/年
        "%d %B %Y", # 日 月 年
        "%d %b %Y",
        "%d-%b-%y",       # 日-月缩写-年（例如：01-Nov-16）
        "%b %d, %y",
        "%m/%d",   # 月/日
        "%m/%Y",   # 月/年
        "%b%y",   # 月/年
        "%Y-%m-%dT%H:%M:%S.%fZ",  # 年-月-日T时:分:秒.毫秒Z（UTC）
        "%I:%M %p",  # 时:分 %p（上下午）
        "%b %d",     # 月缩写 日
        "%A, %b %d",  # 星期全称，月缩写，日
        "%H:%M:%S",
        "%B" #月
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

        if len(data) >= 1000:
            data = data[:1000]  # 取前500条数据
        else:
            continue

        data_copy = copy.deepcopy(data)

        date_cols = original_table['dateColumns']  # 假设日期列在dateColumns中标记
        
        if not date_cols:
            continue  # 如果没有日期列，则跳过

        # split_num = min(1,int(len(num_cols) * 0.2))

        # selected_cols = random.sample(num_cols, split_num)
        
        num_i = 0
        for col in date_cols:
            # global index
            index += 1
            num_i += 1 

            data_loop = copy.deepcopy(data_copy)
            print("ori_data",data_loop)

            clean_data = []
            clean_data_copy = []
            unclean_data = []
            for row in data_loop:
                parsed_value = parse_date1(row[col])
                if parsed_value is None:
                    unclean_data.append(row)
                else:
                    clean_data.append(row)

            clean_data_copy = copy.deepcopy(clean_data)

            if len(clean_data_copy) < 10:
                continue

            global global_fmt
            global_fmt = "%Y/%m/%d"

            # col_data = [parse_date(row[col]) for row in data]
            while True:  # 使用 while 循环来重新解析表格
                need_reparse = False
                print("clean_data_copy:",clean_data_copy[:10])
                clean_data = copy.deepcopy(clean_data_copy)
                print("clean_data:",clean_data[:10])
                print("2222222222")
                for row in clean_data:
                    try:
                        print("111111111")
                        print(row[col])
                        print(global_fmt)
                        parse_date = datetime.strptime(row[col], global_fmt)
                        print(row[col])
                        print("666666666666666666666666")
                        row[col] = parse_date
                        print(global_fmt)
                    except ValueError as e:
                        # 如果出现不符合当前 global_fmt 的情况，重置 global_fmt
                        # global_fmt = None
                        print("++++++++++++++++++++++++++++")
                        print(e)
                        for fmt in date_formats:
                            print("******************************")
                            try:
                                # 如果解析成功，更新全局日期格式并返回日期对象      
                                global_fmt = fmt
                                datetime.strptime(row[col], global_fmt)
                                break
                            except ValueError:
                                continue
                        # row[col] = parse_date3(row[col])
                        print(global_fmt)
                        need_reparse = True
                        # clean_data = clean_data_copy
                        break  # 跳出当前循环，重新解析
                if not need_reparse:
                    break  # 如果没有需要重新解析的情况，退出 while 循环
                else:
                    # 重置 global_fmt 后，重新解析整张表格
                    continue
            print(f"Processed {table_key} with format {global_fmt}")

            col_data = [row[col] for row in clean_data]
            if len(col_data) < 10:
                continue

            # segnum = np.percentile(col_data, random.uniform(20, 80))

            # print(col_data)

            timestamps = [dt.timestamp() for dt in col_data]  # 转换为时间戳
            
            percentile_value = np.percentile(timestamps, random.uniform(20, 80))
            segnum = datetime.fromtimestamp(percentile_value) 

            

            # 将数据分为大于和小于等于 segnum 的两组
            # print(segnum)
            above_segnum_data = [row for row in clean_data if row[col] > segnum]
            below_or_equal_segnum_data = [row for row in clean_data if row[col] <= segnum]

            # 从大于 segnum 的组中随机选择 80%-100% 的行作为 datalake_data 的一部分
            datalake_above_proportion = random.uniform(0.8, 1.0)
            datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * datalake_above_proportion)]

            # 计算 datalake-qualifynum
            datalake_qualifynum = len(datalake_above_segnum)

            # 从小于等于 segnum 的组中随机选择 0 到 datalake-qualifynum/4 的行作为 datalake_data 的另一部分
            datalake_below_proportion = random.uniform(0, min(len(below_or_equal_segnum_data), datalake_qualifynum / 4))
            datalake_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * datalake_below_proportion)]

            # 合并 datalake_data
            datalake_data = datalake_above_segnum + datalake_below_segnum
            
            average_flag = 1
            try:
            # 计算平均值
                timestamps_datalake = [dt[col].timestamp() for dt in datalake_data]
                average_timestamp = np.mean(timestamps_datalake)  # 计算时间戳的平均值
                average_datetime = datetime.fromtimestamp(average_timestamp)  # 转换回 datetime 对象
                average_datetime_str = average_datetime.strftime(global_fmt)
            except Exception as e:
                print(e)
                average_flag = 0


            for row in datalake_data:
                try:
                    row[col] =  row[col].strftime(global_fmt)
                    print("datalake_data row[col]:", row[col])
                except Exception as e:
                    print(e)

            print("datalake_data:", datalake_data)

            print("datalake_data:",len(datalake_data))
            if len(datalake_data) < 10:
               continue

            # # 计算 datalake_data 在 row[col] 的平均值
            # average = round(np.mean([float(row[col]) for row in datalake_data]))

            # 从大于 segnum 的组中随机选择 0%-30% 的行作为 query_data 的一部分
            query_above_proportion = random.uniform(0, 0.3)
            query_above_segnum = above_segnum_data[int(len(above_segnum_data) * (1 - query_above_proportion)):]

            # 从小于等于 segnum 的组中随机选择 20%-80% 的行作为 query_data 的另一部分
            query_below_proportion = random.uniform(0.2, 0.8)
            query_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * query_below_proportion)]

            # 合并 query_data
            query_data = query_above_segnum + query_below_segnum

            for row in query_data:
                print(row[col])
                try:
                    row[col] =  row[col].strftime(global_fmt)
                except Exception as e:
                    print(e)


            # 获取原表名并去掉前三个字母
            original_name = table_key
            modified_name = original_name[3:]

            query_name = f"q{table_key[3:]}_3_3_{num_i}.json"
            query_name_nojson = f"q{table_key[3:]}_3_3_{num_i}"
            datalake_name = f"dl{table_key[3:]}_3_3_{num_i}.json"
            datalake_name_nojson = f"dl{table_key[3:]}_3_3_{num_i}"

            # 创建新的表格数据字典，保留原始表格的格式
            query_table_data = original_table.copy()
            datalake_table_data = original_table.copy()

            # 更新分割后的表格数据
            query_table_data["data"] = query_data
            query_table_data["numDataRows"] = len(query_data)
            query_table_data["table_array"] = titles + query_data

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["table_array"] = titles + datalake_data

            if len(datalake_data) < min_split_num:
                continue

            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            if average_flag:
                if random.choice([True, False]):
                    # 随机选择template_num个模板
                    selected_templates = query_larger_templates[:template_num]
                
                    # 写入query.txt
                    with open(query_txt, 'a', encoding='utf-8') as q_txt:
                        selected_template = random.choice(selected_templates)
                        nl_query = selected_template.format(caption=titles[col], seg_num=segnum, scale=min_split_num)
                        # nl_query += f" having more than {min_split_num} rows"
                        q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
                
                # # 写入groundtruth.txt
                #     with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                #         gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")
                
                else:
                    # 随机选择template_num个模板
                    selected_templates = query_average_templates[:template_num]
                
                    # 写入query.txt
                    with open(query_txt, 'a', encoding='utf-8') as q_txt:
                        selected_template = random.choice(selected_templates)
                        nl_query = selected_template.format(caption=titles[col], template_average=average_datetime_str, scale=min_split_num)
                        # nl_query += f" having more than {min_split_num} rows"
                        q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")

            else:
                selected_templates = query_larger_templates[:template_num]
                
                    # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], seg_num=segnum, scale=min_split_num)
                    # nl_query += f" having more than {min_split_num} rows"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(1, len(titles) - 1)
                available_indices = [i for i in range(len(titles)) if i != col]
                selected_col_indices = random.sample(available_indices, selected_col_count)
                
                # 随机选择行数，确保不少于1行
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue
                
                # for row in neg_datalake_data:
                #     print(row[col])
                #     try:
                #         row[col] =  row[col].strftime(global_fmt)
                #     except Exception as e:
                #         print(e)

                neg_datalake_name = f"dl{modified_name}_3_3_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_3_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
            
                # 从小于等于 segnum 的组中随机选择 60%-80% 的行作为 datalake_data 的一部分
                neg_datalake_below_proportion = random.uniform(0.6, 0.8)
                neg_datalake_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * neg_datalake_below_proportion)]

                # 计算 datalake-qualifynum
                neg_datalake_qualifynum = len(neg_datalake_below_segnum)
                print("neg_datalake_qualifynum:", neg_datalake_qualifynum)

                # 从大于 segnum 的组中随机选择 0 到 datalake-qualifynum 的行作为 datalake_data 的另一部分
                neg_datalake_above_proportion = random.uniform(0, min(len(above_segnum_data),neg_datalake_qualifynum))
                neg_datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_above_proportion)]

                # 合并 datalake_data
                neg_datalake_data =  neg_datalake_below_segnum + neg_datalake_above_segnum

                # 从 unclean_data 中随机选择一定比例的数据添加到 neg_datalake_data
                unclean_proportion = random.uniform(0.1, 0.3)  # 假设我们想要添加 10%-30% 的 unclean_data
                num_unclean_to_add = int(len(unclean_data) * unclean_proportion)
                added_unclean_data = random.sample(unclean_data, num_unclean_to_add)
                neg_datalake_data += added_unclean_data
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue

                for row in neg_datalake_data:
                    print(row[col])
                    try:
                        row[col] =  row[col].strftime(global_fmt)
                    except Exception as e:
                        print(e)


                neg_datalake_name = f"dl{modified_name}_3_3_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_3_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 

            for neg_number in range(2*neg_num + 1, 3*neg_num + 1):
                # 随机选择小于min_split_num的行数
                selected_row_count = random.randint(min_split_num - 10, min_split_num - 1)
                neg_datalake_data = random.sample(data,selected_row_count)

                # if selected_row_count <= len(overlap_data):
                #     neg_datalake_data = random.sample(overlap_data, selected_row_count)
                # else:
                #     neg_datalake_data = random.sample(data,selected_row_count-len(overlap_data)) + overlap_data

                neg_datalake_name = f"dl{modified_name}_3_3_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_3_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 

    return index

def split_scale_smaller_date_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, neg_num=10, shuffle=1, template_num=3, min_split_num=100):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_smaller_templates = [
        "I am searching for additional unionable tables where {caption} is before {seg_num} and the tables contain more than {scale} rows.",
        "I need to identify more unionable tables with {caption} prior to {seg_num} and having over {scale} rows.",
        "I am looking for further unionable tables, where {caption} is earlier than {seg_num} and the tables include more than {scale} rows.",
        "I want to find more unionable tables with {caption} before {seg_num} and containing more than {scale} rows."
    ]

    query_average_templates = [
        "I am looking for additional unionable tables where {caption} is around {template_average} and the tables contain more than {scale} rows.",
        "I need to identify more unionable tables with {caption} approximately {template_average} and having over {scale} rows.",
        "I am searching for further unionable tables, where {caption} is close to {template_average} and the tables include more than {scale} rows.",
        "I want to find more unionable tables with {caption} near {template_average} and containing more than {scale} rows."
    ]

    date_formats = [
        "%Y/%m/%d",  # 年/月/日
        "%Y.%m.%d",  # 年.月.日
        "%Y-%m-%d",  # 年-月-日
        "%d/%m/%Y",   # 日/月/年
        "%d-%m-%Y %H:%M:%S",  # 日-月-年 时:分:秒
        "%d.%m.%Y %H:%M",  # 日-月-年 时:分:秒
        "%m/%d/%Y %I:%M:%S %p",  # 日-月-年 时:分:秒
        "%m/%d/%Y %H:%M",  # 日-月-年 时:分
        "%m/%d/%Y",   # 月/日/年        
        "%d.%m.%Y",   # 日/月/年
        "%d %B %Y", # 日 月 年
        "%d %b %Y",
        "%d-%b-%y",       # 日-月缩写-年（例如：01-Nov-16）
        "%b %d, %y",
        "%m/%d",   # 月/日
        "%m/%Y",   # 月/年
        "%b%y",   # 月/年
        "%Y-%m-%dT%H:%M:%S.%fZ",  # 年-月-日T时:分:秒.毫秒Z（UTC）
        "%I:%M %p",  # 时:分 %p（上下午）
        "%b %d",     # 月缩写 日
        "%A, %b %d",  # 星期全称，月缩写，日
        "%H:%M:%S",
        "%B", #月
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

        if len(data) >= 1000:
            data = data[:1000]  # 取前500条数据
        else:
            continue

        data_copy = copy.deepcopy(data)

        date_cols = original_table['dateColumns']  # 假设日期列在dateColumns中标记
        
        if not date_cols:
            continue  # 如果没有日期列，则跳过
        
        # split_num = int(len(num_cols) * 0.5)

        # selected_cols = random.sample(num_cols, split_num)

        num_i = 0
        for col in date_cols:
            # global index
            index += 1
            num_i += 1 

            data_loop = copy.deepcopy(data_copy)
            print("ori_data",data_loop)

            clean_data = []
            clean_data_copy = []
            unclean_data = []
            for row in data_loop:
                parsed_value = parse_date1(row[col])
                if parsed_value is None:
                    unclean_data.append(row)
                else:
                    clean_data.append(row)

            clean_data_copy = copy.deepcopy(clean_data)

            print(f'first_clean_data:{clean_data_copy}')

            global global_fmt
            global_fmt = "%Y/%m/%d"

            # col_data = [parse_date(row[col]) for row in data]
            while True:  # 使用 while 循环来重新解析表格
                need_reparse = False
                print("clean_data_copy:",clean_data_copy[:10])
                clean_data = copy.deepcopy(clean_data_copy)
                print("clean_data:",clean_data[:10])
                print("2222222222")
                for row in clean_data:
                    try:
                        print("111111111")
                        print(row[col])
                        print(global_fmt)
                        parse_date = datetime.strptime(row[col], global_fmt)
                        print(row[col])
                        print("666666666666666666666666")
                        row[col] = parse_date
                        print(global_fmt)
                    except ValueError as e:
                        # 如果出现不符合当前 global_fmt 的情况，重置 global_fmt
                        # global_fmt = None
                        print("++++++++++++++++++++++++++++")
                        print(e)
                        for fmt in date_formats:
                            print("******************************")
                            try:
                                # 如果解析成功，更新全局日期格式并返回日期对象      
                                global_fmt = fmt
                                datetime.strptime(row[col], global_fmt)
                                break
                            except ValueError:
                                continue
                        # row[col] = parse_date3(row[col])
                        print(global_fmt)
                        need_reparse = True
                        # clean_data = clean_data_copy
                        break  # 跳出当前循环，重新解析
                if not need_reparse:
                    break  # 如果没有需要重新解析的情况，退出 while 循环
                else:
                    # 重置 global_fmt 后，重新解析整张表格
                    continue
            print(f"Processed {table_key} with format {global_fmt}")

            col_data = [row[col] for row in clean_data]
            if len(col_data) < 10:
                continue

            # segnum = np.percentile(col_data, random.uniform(20, 80))

            print(col_data)

            timestamps = [dt.timestamp() for dt in col_data]  # 转换为时间戳
            
            percentile_value = np.percentile(timestamps, random.uniform(20, 80))
            segnum = datetime.fromtimestamp(percentile_value) 

            

            # 将数据分为大于和小于等于 segnum 的两组
            # print(segnum)
            above_segnum_data = [row for row in clean_data if row[col] > segnum]
            below_or_equal_segnum_data = [row for row in clean_data if row[col] <= segnum]

            # 从大于 segnum 的组中随机选择 80%-100% 的行作为 datalake_data 的一部分
            datalake_below_proportion = random.uniform(0.8, 1.0)
            datalake_below_segnum = above_segnum_data[:int(len(below_or_equal_segnum_data) * datalake_below_proportion)]

            # 计算 datalake-qualifynum
            datalake_qualifynum = len(datalake_below_segnum)

            # 从小于等于 segnum 的组中随机选择 0 到 datalake-qualifynum/4 的行作为 datalake_data 的另一部分
            datalake_above_proportion = random.uniform(0, min(len(above_segnum_data), datalake_qualifynum / 4))
            datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * datalake_above_proportion)]

            # 合并 datalake_data
            datalake_data = datalake_below_segnum + datalake_above_segnum

            average_flag = 1
            try:
            # 计算平均值
                timestamps_datalake = [dt[col].timestamp() for dt in datalake_data]
                average_timestamp = np.mean(timestamps_datalake)  # 计算时间戳的平均值
                average_datetime = datetime.fromtimestamp(average_timestamp)  # 转换回 datetime 对象
                average_datetime_str = average_datetime.strftime(global_fmt)
            except Exception as e:
                print(e)
                average_flag = 0

            for row in datalake_data:
                try:
                    row[col] =  row[col].strftime(global_fmt)
                    print("datalake_data row[col]:", row[col])
                except Exception as e:
                    print(e)

            print("datalake_data:", datalake_data)

            print("datalake_data:",len(datalake_data))
            if len(datalake_data) < 10:
               continue

            # # 计算 datalake_data 在 row[col] 的平均值
            # average = round(np.mean([float(row[col]) for row in datalake_data]))

            # 从大于 segnum 的组中随机选择 0%-30% 的行作为 query_data 的一部分
            query_below_proportion = random.uniform(0, 0.3)
            query_below_segnum = above_segnum_data[int(len(below_or_equal_segnum_data) * (1 - query_below_proportion)):]

            # 从小于等于 segnum 的组中随机选择 20%-80% 的行作为 query_data 的另一部分
            query_above_proportion = random.uniform(0.2, 0.8)
            query_above_segnum = above_segnum_data[:int(len(above_segnum_data) * query_above_proportion)]

            # 合并 query_data
            query_data = query_below_segnum + query_above_segnum

            for row in query_data:
                print(row[col])
                try:
                    row[col] =  row[col].strftime(global_fmt)
                except Exception as e:
                    print(e)


            # 获取原表名并去掉前三个字母
            original_name = table_key
            modified_name = original_name[3:]

            query_name = f"q{table_key[3:]}_3_3_{num_i}.json"
            query_name_nojson = f"q{table_key[3:]}_3_3_{num_i}"
            datalake_name = f"dl{table_key[3:]}_3_3_{num_i}.json"
            datalake_name_nojson = f"dl{table_key[3:]}_3_3_{num_i}"

            # 创建新的表格数据字典，保留原始表格的格式
            query_table_data = original_table.copy()
            datalake_table_data = original_table.copy()

            # 更新分割后的表格数据
            query_table_data["data"] = query_data
            query_table_data["numDataRows"] = len(query_data)
            query_table_data["table_array"] = titles + query_data

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["table_array"] = titles + datalake_data

            if len(datalake_data) < min_split_num:
                continue

            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            
            if average_flag:
                if random.choice([True, False]):
                    # 随机选择template_num个模板
                    selected_templates = query_smaller_templates[:template_num]
                
                    # 写入query.txt
                    with open(query_txt, 'a', encoding='utf-8') as q_txt:
                        selected_template = random.choice(selected_templates)
                        nl_query = selected_template.format(caption=titles[col], seg_num=segnum, scale=min_split_num)
                        # nl_query += f" having more than {min_split_num} rows"
                        q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
                
                # # 写入groundtruth.txt
                #     with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                #         gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")
                
                else:
                    # 随机选择template_num个模板
                    selected_templates = query_average_templates[:template_num]
                
                    # 写入query.txt
                    with open(query_txt, 'a', encoding='utf-8') as q_txt:
                        selected_template = random.choice(selected_templates)
                        nl_query = selected_template.format(caption=titles[col], template_average=average_datetime_str, scale=min_split_num)
                        # nl_query += f" having more than {min_split_num} rows"
                        q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")

            else:
                selected_templates = query_smaller_templates[:template_num]
                
                    # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], seg_num=segnum, scale=min_split_num)
                    # nl_query += f" having more than {min_split_num} rows"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(1, len(titles) - 1)
                available_indices = [i for i in range(len(titles)) if i != col]
                selected_col_indices = random.sample(available_indices, selected_col_count)
                
                # 随机选择行数，确保不少于1行
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue
                
                # for row in neg_datalake_data:
                #     print(row[col])
                #     try:
                #         row[col] =  row[col].strftime(global_fmt)
                #     except Exception as e:
                #         print(e)

                neg_datalake_name = f"dl{modified_name}_3_3_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_3_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
            
                # 从小于等于 segnum 的组中随机选择 60%-80% 的行作为 datalake_data 的一部分
                neg_datalake_above_proportion = random.uniform(0.6, 0.8)
                neg_datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_above_proportion)]

                # 计算 datalake-qualifynum
                neg_datalake_qualifynum = len(neg_datalake_above_segnum)
                print("neg_datalake_qualifynum:", neg_datalake_qualifynum)

                # 从大于 segnum 的组中随机选择 0 到 datalake-qualifynum 的行作为 datalake_data 的另一部分
                neg_datalake_below_proportion = random.uniform(0, min(len(below_or_equal_segnum_data),neg_datalake_qualifynum))
                neg_datalake_below_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_below_proportion)]

                # 合并 datalake_data
                neg_datalake_data = neg_datalake_above_segnum + neg_datalake_below_segnum

                # 从 unclean_data 中随机选择一定比例的数据添加到 neg_datalake_data
                unclean_proportion = random.uniform(0.1, 0.3)  # 假设我们想要添加 10%-30% 的 unclean_data
                num_unclean_to_add = int(len(unclean_data) * unclean_proportion)
                added_unclean_data = random.sample(unclean_data, num_unclean_to_add)
                neg_datalake_data += added_unclean_data
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue

                for row in neg_datalake_data:
                    print(row[col])
                    try:
                        row[col] =  row[col].strftime(global_fmt)
                    except Exception as e:
                        print(e)


                neg_datalake_name = f"dl{modified_name}_3_3_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_3_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 

            for neg_number in range(2*neg_num + 1, 3*neg_num + 1):
                # 随机选择小于min_split_num的行数
                selected_row_count = random.randint(min_split_num - 10, min_split_num - 1)
                neg_datalake_data = random.sample(data,selected_row_count)

                # if selected_row_count <= len(overlap_data):
                #     neg_datalake_data = random.sample(overlap_data, selected_row_count)
                # else:
                #     neg_datalake_data = random.sample(data,selected_row_count-len(overlap_data)) + overlap_data

                neg_datalake_name = f"dl{modified_name}_3_3_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_3_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

    return index

def split_cellvalue_category_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, col_scope=0.6, max_duplicate=0.1, min_split_rate=0.2, template_num=3, shuffle=1, difficulty = 0.3, neg_num = 10):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_templates = [
        "I need to locate additional unionable tables where the value of {col_name} is {caption} and the tables contain the keyword {second_caption}.",
        "I am searching for more unionable tables with the value of {col_name} being {caption} and containing the keyword {second_caption}.",
        "I aim to identify further unionable tables that have {caption} as the value of {col_name} and include the keyword {second_caption}.",
        "I want to find additional unionable tables where {col_name} equals {caption} and the tables contain the keyword {second_caption}."
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

        if len(data) >= 1000:
            data = data[:1000]  # 取前500条数据

        categorical_cols = [i for i in range(len(original_table['title'])) if i not in original_table['numericColumns'] and i not in original_table['dateColumns']]
        unique_values_count = [
        len(set(row[i] for row in original_table['data'] if row[i].strip() != '')) 
        for i in categorical_cols
        ]    #ori-table需要标注列类别标签，代码也需要小幅度修改

        # 按唯一值数量排序
        sorted_cols = sorted(zip(categorical_cols, unique_values_count), key=lambda x: x[1], reverse=True)

        # 过滤掉唯一值数量为1的列
        sorted_cols = [(col, count) for col, count in sorted_cols if count > 1]

        # 如果过滤后没有列，则不选择任何列
        if not sorted_cols:
            continue
        else:
            # 计算前60%的列数量
            num_cols_to_select = int(len(sorted_cols) * col_scope)

        # 选择前60%的列
        selected_cols = [col for col, count in sorted_cols[:num_cols_to_select]]

        split_num = int(len(selected_cols) * 0.5)

        selected_cols = random.sample(selected_cols, split_num)

        num_i = 0
        for col in selected_cols:

            # global index
            index += 1
            num_i += 1 

            unique_cats = list(set([row[col] for row in original_table['data']]))

            value_counts = Counter(unique_cats)
        
            # 选择排名前difficulty%的值
            top_values = [value for value, count in value_counts.most_common(int(difficulty * len(value_counts)) + 1)]
            
            # 从排名前difficulty%的值中随机选择一个作为caption
            selected_caption = random.choice(top_values)

            # 先把原表中包含 selected_caption 的所有行中的至少80%给 datalake_data
            data_with_caption = [row for row in original_table['data'] if row[col] == selected_caption]
            
            # 随机决定给 datalake_data 的比例（80% - 100%）
            datalake_proportion = random.uniform(0.8, 1.0)
            datalake_data = data_with_caption[:int(len(data_with_caption) * datalake_proportion)]

            # 对于 query_data，从包含 selected_caption 的行中随机选择0%到50%的比例
            query_proportion = random.uniform(0, 0.5)
            query_data = data_with_caption[:int(len(data_with_caption) * query_proportion)]

            # 处理除了 selected_caption 的整个表格
            data_without_caption = [row for row in original_table['data'] if row[col] != selected_caption]

            # 计算重叠行数
            split_duplicate = random.uniform(0, max_duplicate)
            overlap_rows = int(len(data_without_caption) * split_duplicate)

            # 计算非重叠部分的最大分割比例
            max_split = len(data_without_caption) - overlap_rows

            # 在 min_split_rate 和 50% 之间随机选择一个分割比例
            split_rate = random.uniform(min_split_rate, 0.5)
            split_index = int(max_split * split_rate)

            # 随机分割表格，确保重叠部分
            if random.choice([True, False]):
                query_data += data_without_caption[:split_index] + data_without_caption[-overlap_rows:]
                datalake_data += data_without_caption[split_index:-overlap_rows] + data_without_caption[-overlap_rows:]
            else:
                query_data += data_without_caption[split_index:-overlap_rows] + data_without_caption[-overlap_rows:]
                datalake_data += data_without_caption[:split_index] + data_without_caption[-overlap_rows:]

            # 统计query_data中所有单元格值的出现频率
            cell_values = [cell for row in datalake_data for cell in row]
            value_counts = Counter(cell_values)

            # 选择排名前difficulty%的值
            top_values = [value for value, count in value_counts.most_common(int(difficulty * len(value_counts)) + 1)]

            # 从排名前difficulty%的值中随机选择一个作为caption
            selected_second_caption = random.choice(top_values)

            # 删除包含selected_caption的行
            data_without_second_caption = [row for row in data if selected_caption not in row]
            overlapData_without_second_caption = [row for row in data[-overlap_rows:] if selected_caption not in row]
            nonoverlapData_without_second_caption = [row for row in data[:-overlap_rows] if selected_caption not in row]

            # 获取原表名并去掉前三个字母
            original_name = table_key
            modified_name = original_name[3:]
            
            query_name = f"q{table_key[3:]}_3_4_{num_i}.json"
            query_name_nojson = f"q{table_key[3:]}_3_4_{num_i}"
            datalake_name = f"dl{table_key[3:]}_3_4_{num_i}.json"
            datalake_name_nojson = f"dl{table_key[3:]}_3_4_{num_i}"

            # 创建新的表格数据字典，保留原始表格的格式
            query_table_data = original_table.copy()
            datalake_table_data = original_table.copy()

            # 更新分割后的表格数据
            query_table_data["data"] = query_data
            query_table_data["numDataRows"] = len(query_data)
            query_table_data["table_array"] = titles + query_data

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["table_array"] = titles + datalake_data


            # 删除包含selected_caption的行
            data_without_caption = [row for row in data if selected_caption not in row]
            if len(data_without_caption) == 0 :
                continue

            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            # 随机选择template_num个模板
            selected_templates = query_templates[:template_num]
            
            # 写入query.txt
            with open(query_txt, 'a', encoding='utf-8') as q_txt:
                selected_template = random.choice(selected_templates)
                nl_query = selected_template.format(caption=selected_caption, col_name=titles[col],second_caption = selected_second_caption)
                # nl_query += f" and mention {selected_second_caption}"
                q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            # 生成负例
            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(1, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)
                
                # 随机选择行数，确保不少于1行
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
                neg_datalake_name = f"dl{modified_name}_3_4_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_4_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
                print(query_name)
                print(len(data_without_caption))
                selected_row_count = random.randint(1, min(len(data_without_caption), ori_minRow))
                neg_datalake_data = random.sample(data_without_caption, selected_row_count)
                
                neg_datalake_name = f"dl{modified_name}_3_4_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_4_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 

            for neg_number in range(neg_num*2 + 1, neg_num*3 + 1):
            # 随机选择行数，确保不少于1行且小于原表
            
                if len(nonoverlapData_without_second_caption)>1:
                    selected_row_count = random.randint(1, len(nonoverlapData_without_second_caption))
                    neg_datalake_data = random.sample(nonoverlapData_without_second_caption, selected_row_count)  + overlapData_without_second_caption
                else:
                    selected_row_count = random.randint(1, len(data_without_caption))
                    neg_datalake_data = random.sample(data_without_caption, selected_row_count)
                
                neg_datalake_name = f"dl{modified_name}_3_4_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_4_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")   

    return index

def split_cellvalue_numerical_larger_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, datalake_quality=0.8, neg_num = 10, shuffle=1, template_num=3, difficulty=0.3):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_larger_templates = [
        "I am searching for additional unionable tables where {caption} is larger than {seg_num} and the tables contain the keyword {second_caption}.",
        "I need to identify more unionable tables with {caption} greater than {seg_num} and containing the keyword {second_caption}.",
        "I am looking for further unionable tables, where {caption} exceeds {seg_num} and the tables include the keyword {second_caption}.",
        "I want to find more unionable tables with {caption} larger than {seg_num} and containing the keyword {second_caption}."
    ]

    query_average_templates = [
        "I am searching for additional unionable tables where {caption} is approximately {template_average} and the tables include the keyword {second_caption}.",
        "I need to identify more unionable tables with {caption} close to {template_average} and containing the keyword {second_caption}.",
        "I am looking for further unionable tables, where {caption} is around {template_average} and the tables have the keyword {second_caption}.",
        "I want to find more unionable tables with {caption} near {template_average} and containing the keyword {second_caption}."
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

        if len(data) >= 1000:
            data = data[:1000]  # 取前500条数据

        num_cols = original_table['numericColumns']

        if not num_cols:
            continue    #ori-table需要标注列类别标签，代码也需要小幅度修改

        split_num = int(len(num_cols) * 0.5)

        selected_cols = random.sample(num_cols, split_num)

        num_i = 0
        for col in selected_cols:

            # global index
            index += 1
            num_i += 1 

            col_data = []
            clean_data = []
            unclean_data = []

            for row in data:
                # 尝试解析并移除无效值
                parsed_value = parse_value(row[col])
                if parsed_value is None:
                    unclean_data.append(row)
                else:
                    col_data.append(parsed_value)
                    clean_data.append(row)

            # col_data = [float(row[col]) for row in data]
            
            if len(col_data) < 10:
                continue

            segnum = np.percentile(col_data, random.uniform(20, 80))

            # 将数据分为大于和小于等于 segnum 的两组
            above_segnum_data = [row for row in clean_data if parse_value(row[col]) > segnum]
            below_or_equal_segnum_data = [row for row in clean_data if parse_value(row[col]) <= segnum]

            # 从大于 segnum 的组中随机选择 80%-100% 的行作为 datalake_data 的一部分
            datalake_above_proportion = random.uniform(0.8, 1.0)
            datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * datalake_above_proportion)]

            # 计算 datalake-qualifynum
            datalake_qualifynum = len(datalake_above_segnum)
            print("datalake_qualifynum:", datalake_qualifynum)

            # 从小于等于 segnum 的组中随机选择 0 到 datalake-qualifynum/4 的行作为 datalake_data 的另一部分
            datalake_below_proportion = random.uniform(0, min(len(below_or_equal_segnum_data),datalake_qualifynum / 4))
            datalake_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * datalake_below_proportion)]

            # 合并 datalake_data
            datalake_data = datalake_above_segnum + datalake_below_segnum

            print("datalake_data:",len(datalake_data))
            if len(datalake_data) < 10:
                continue
            average = round(np.mean([parse_value(row[col]) for row in datalake_data]))

            # 从大于 segnum 的组中随机选择 0%-30% 的行作为 query_data 的一部分
            query_above_proportion = random.uniform(0, 0.3)
            query_above_segnum = above_segnum_data[int(len(above_segnum_data) * (1 - query_above_proportion)):]

            # 从小于等于 segnum 的组中随机选择 20%-80% 的行作为 query_data 的另一部分
            query_below_proportion = random.uniform(0.2, 0.8)
            query_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * query_below_proportion)]

            # 合并 query_data
            query_data = query_above_segnum + query_below_segnum
            
            # 将unclean_data按随机比例分配给query_data和datalake_data
            unclean_proportion = random.uniform(0.2, 0.8)
            selected_unclean_for_query = random.sample(unclean_data, int(len(unclean_data) * unclean_proportion))
            query_data += selected_unclean_for_query

            # 获取原表名并去掉前三个字母
            original_name = table_key
            modified_name = original_name[3:]
            
            query_name = f"q{table_key[3:]}_3_5_{num_i}.json"
            query_name_nojson = f"q{table_key[3:]}_3_5_{num_i}"
            datalake_name = f"dl{table_key[3:]}_3_5_{num_i}.json"
            datalake_name_nojson = f"dl{table_key[3:]}_3_5_{num_i}"

            # 创建新的表格数据字典，保留原始表格的格式
            query_table_data = original_table.copy()
            datalake_table_data = original_table.copy()

            # 更新分割后的表格数据
            query_table_data["data"] = query_data
            query_table_data["numDataRows"] = len(query_data)
            query_table_data["table_array"] = titles + query_data

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["table_array"] = titles + datalake_data

            # 统计query_data中所有单元格值的出现频率
            cell_values = [cell for row in datalake_data for cell in row]
            value_counts = Counter(cell_values)

            # 选择排名前difficulty%的值
            top_values = [value for value, count in value_counts.most_common(int(difficulty * len(value_counts)) + 1)]

            # 从排名前difficulty%的值中随机选择一个作为caption
            selected_caption = random.choice(top_values)

            # 删除包含selected_caption的行
            data_without_caption = [row for row in data if selected_caption not in row]

            if len(data_without_caption)==0:
                continue

            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            if random.choice([True, False]):
                # 随机选择template_num个模板
                selected_templates = query_larger_templates[:template_num]
            
                # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], seg_num=segnum, second_caption=selected_caption)
                    # nl_query += f" and mention {selected_caption}"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
                # 写入groundtruth.txt
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            else:
                # 随机选择template_num个模板
                selected_templates = query_average_templates[:template_num]
            
                # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], template_average=average, second_caption=selected_caption)
                    # nl_query += f" and mention {selected_caption}"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
                # 写入groundtruth.txt
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            # 生成负例
            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(1, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)
                
                # 随机选择行数，确保不少于1行
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue

                neg_datalake_name = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
            
                # 从小于等于 segnum 的组中随机选择 60%-80% 的行作为 datalake_data 的一部分
                neg_datalake_below_proportion = random.uniform(0.6, 0.8)
                neg_datalake_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * neg_datalake_below_proportion)]

                # 计算 datalake-qualifynum
                neg_datalake_qualifynum = len(neg_datalake_below_segnum)
                # print("neg_datalake_qualifynum:", neg_datalake_qualifynum)

                # 从大于 segnum 的组中随机选择 0 到 datalake-qualifynum 的行作为 datalake_data 的另一部分
                neg_datalake_above_proportion = random.uniform(0, min(len(above_segnum_data),neg_datalake_qualifynum))
                neg_datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_above_proportion)]

                # 合并 datalake_data
                neg_datalake_data = neg_datalake_below_segnum + neg_datalake_above_segnum

                # 从 unclean_data 中随机选择一定比例的数据添加到 neg_datalake_data
                unclean_proportion = random.uniform(0.1, 0.3)  # 假设我们想要添加 10%-30% 的 unclean_data
                num_unclean_to_add = int(len(unclean_data) * unclean_proportion)
                added_unclean_data = random.sample(unclean_data, num_unclean_to_add)
                neg_datalake_data += added_unclean_data

                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue

                neg_datalake_name = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 

            for neg_number in range(neg_num*2 + 1, neg_num*3 + 1):
                # 随机选择行数，确保不少于1行且小于原表
                
                selected_row_count = random.randint(1, len(data_without_caption))
                neg_datalake_data = random.sample(data_without_caption, selected_row_count)
                
                neg_datalake_name = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

    return index

def split_cellvalue_numerical_smaller_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, datalake_quality=0.8, neg_num = 10, shuffle=1, template_num=3, difficulty=0.3):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_larger_templates = [
        "I am searching for additional unionable tables where {caption} is less than {seg_num} and the tables include the keyword {second_caption}.",
        "I need to identify more unionable tables with {caption} smaller than {seg_num} and containing the keyword {second_caption}.",
        "I am looking for further unionable tables, where {caption} is below {seg_num} and the tables have the keyword {second_caption}.",
        "I want to find more unionable tables with {caption} less than {seg_num} and containing the keyword {second_caption}."
    ]

    query_average_templates = [
        "I am searching for additional unionable tables where {caption} is approximately {template_average} and the tables include the keyword {second_caption}.",
        "I need to identify more unionable tables with {caption} close to {template_average} and containing the keyword {second_caption}.",
        "I am looking for further unionable tables, where {caption} is around {template_average} and the tables have the keyword {second_caption}.",
        "I want to find more unionable tables with {caption} near {template_average} and containing the keyword {second_caption}."
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
        
        print(len(data))
        if len(data) >= 1000:
            data = data[:1000]  # 取前500条数据
        print(len(data))

        num_cols = original_table['numericColumns']

        if not num_cols:
            continue    #ori-table需要标注列类别标签，代码也需要小幅度修改

        split_num = int(len(num_cols) * 0.5)

        selected_cols = random.sample(num_cols, split_num)

        num_i = 0
        for col in selected_cols:

            # global index
            index += 1
            num_i += 1 

            col_data = []
            clean_data = []
            unclean_data = []

            # col_data = [float(row[col]) for row in data]
            for row in data:
                # 尝试解析并移除无效值
                parsed_value = parse_value(row[col])
                if parsed_value is None:
                    unclean_data.append(row)
                else:
                    col_data.append(parsed_value)
                    clean_data.append(row)

            if len(col_data) < 10:
                continue

            segnum = np.percentile(col_data, random.uniform(20, 80))

            # 将数据分为大于和小于等于 segnum 的两组
            above_segnum_data = [row for row in clean_data if parse_value(row[col]) > segnum]
            below_or_equal_segnum_data = [row for row in clean_data if parse_value(row[col]) <= segnum]

            # 从大于 segnum 的组中随机选择 80%-100% 的行作为 datalake_data 的一部分
            datalake_below_proportion = random.uniform(0.8, 1.0)
            datalake_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * datalake_below_proportion)]

            # 计算 datalake-qualifynum
            datalake_qualifynum = len(datalake_below_segnum)
            # print("datalake_qualifynum:", datalake_qualifynum)

            # 从小于等于 segnum 的组中随机选择 0 到 datalake-qualifynum/4 的行作为 datalake_data 的另一部分
            datalake_above_proportion = random.uniform(0, min(len(above_segnum_data),datalake_qualifynum / 4))
            datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * datalake_above_proportion)]

            # 合并 datalake_data
            datalake_data = datalake_below_segnum + datalake_above_segnum

            print("datalake_data:",len(datalake_data))
            if len(datalake_data) < 10:
               continue

            # 计算 datalake_data 在 row[col] 的平均值
            average = round(np.mean([parse_value(row[col]) for row in datalake_data]))

            # 从大于 segnum 的组中随机选择 0%-30% 的行作为 query_data 的一部分
            query_below_proportion = random.uniform(0, 0.3)
            query_below_segnum = below_or_equal_segnum_data[int(len(below_or_equal_segnum_data) * (1 - query_below_proportion)):]

            # 从小于等于 segnum 的组中随机选择 20%-80% 的行作为 query_data 的另一部分
            query_above_proportion = random.uniform(0.2, 0.8)
            query_above_segnum = above_segnum_data[:int(len(above_segnum_data) * query_above_proportion)]

            # 合并 query_data
            query_data = query_below_segnum + query_above_segnum 
            

            # 获取原表名并去掉前三个字母
            original_name = table_key
            modified_name = original_name[3:]
            
            query_name = f"q{table_key[3:]}_3_5_{num_i}.json"
            query_name_nojson = f"q{table_key[3:]}_3_5_{num_i}"
            datalake_name = f"dl{table_key[3:]}_3_5_{num_i}.json"
            datalake_name_nojson = f"dl{table_key[3:]}_3_5_{num_i}"

            # 创建新的表格数据字典，保留原始表格的格式
            query_table_data = original_table.copy()
            datalake_table_data = original_table.copy()

            # 更新分割后的表格数据
            query_table_data["data"] = query_data
            query_table_data["numDataRows"] = len(query_data)
            query_table_data["table_array"] = titles + query_data

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["table_array"] = titles + datalake_data

            # 统计query_data中所有单元格值的出现频率
            cell_values = [cell for row in datalake_data for cell in row]
            value_counts = Counter(cell_values)

            # 选择排名前difficulty%的值
            top_values = [value for value, count in value_counts.most_common(int(difficulty * len(value_counts)) + 1)]

            # 从排名前difficulty%的值中随机选择一个作为caption
            selected_caption = random.choice(top_values)

            # 删除包含selected_caption的行
            data_without_caption = [row for row in data if selected_caption not in row]

            if len(data_without_caption)==0:
                continue

            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            if random.choice([True, False]):
                # 随机选择template_num个模板
                selected_templates = query_larger_templates[:template_num]
            
                # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], seg_num=segnum, second_caption=selected_caption)
                    # nl_query += f" and mention {selected_caption}"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
            # 写入groundtruth.txt
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")
            
            else:
                # 随机选择template_num个模板
                selected_templates = query_average_templates[:template_num]
            
                # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], template_average=average, second_caption=selected_caption)
                    # nl_query += f" and mention {selected_caption}"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
            # 写入groundtruth.txt
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            

            # 生成负例
            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(1, len(titles) - 1)
                selected_col_indices = random.sample(range(len(titles)), selected_col_count)
                
                # 随机选择行数，确保不少于1行
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue


                neg_datalake_name = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
            
                # 从小于等于 segnum 的组中随机选择 60%-80% 的行作为 datalake_data 的一部分
                neg_datalake_above_proportion = random.uniform(0.6, 0.8)
                neg_datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_above_proportion)]

                # 计算 datalake-qualifynum
                neg_datalake_qualifynum = len(neg_datalake_above_segnum)
                print("neg_datalake_qualifynum:", neg_datalake_qualifynum)

                # 从大于 segnum 的组中随机选择 0 到 datalake-qualifynum 的行作为 datalake_data 的另一部分
                neg_datalake_below_proportion = random.uniform(0, min(len(below_or_equal_segnum_data),neg_datalake_qualifynum))
                neg_datalake_below_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_below_proportion)]

                # 合并 datalake_data
                neg_datalake_data = neg_datalake_above_segnum + neg_datalake_below_segnum

                # 从 unclean_data 中随机选择一定比例的数据添加到 neg_datalake_data
                unclean_proportion = random.uniform(0.1, 0.3)  # 假设我们想要添加 10%-30% 的 unclean_data
                num_unclean_to_add = int(len(unclean_data) * unclean_proportion)
                added_unclean_data = random.sample(unclean_data, num_unclean_to_add)
                neg_datalake_data += added_unclean_data
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue


                neg_datalake_name = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 
            
            for neg_number in range(neg_num*2 + 1, neg_num*3 + 1):
                # 随机选择行数，确保不少于1行且小于原表
                
                selected_row_count = random.randint(1, len(data_without_caption))
                neg_datalake_data = random.sample(data_without_caption, selected_row_count)
                
                neg_datalake_name = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_5_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

    return index

def split_cellvalue_larger_date_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, neg_num=10, shuffle=1, template_num=3,difficulty=0.3):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_larger_templates = [
        "I am on the lookout for additional unionable tables where {caption} is subsequent to {seg_num} and the tables feature the keyword {second_caption}.",
        "I need to track down more unionable tables with {caption} that comes after {seg_num} and includes the keyword {second_caption}.",
        "I am exploring further unionable tables, where {caption} is more recent than {seg_num} and the tables incorporate the keyword {second_caption}.",
        "I aim to discover more unionable tables with {caption} that is later than {seg_num} and contains the keyword {second_caption}."
    ]

    query_average_templates = [
        "I'm seeking out additional unionable tables where {caption} is roughly {template_average} and the tables include the keyword {second_caption}.",
        "I need to uncover more unionable tables with {caption} hovering around {template_average} and featuring the keyword {second_caption}.",
        "I'm investigating further unionable tables, where {caption} is in the vicinity of {template_average} and the tables have the keyword {second_caption}.",
        "I aim to locate more unionable tables with {caption} close to {template_average} and containing the keyword {second_caption}."
    ]

    date_formats = [
        "%Y/%m/%d",  # 年/月/日
        "%Y.%m.%d",  # 年.月.日
        "%Y-%m-%d",  # 年-月-日
        "%d/%m/%Y",   # 日/月/年
        "%d-%m-%Y %H:%M:%S",  # 日-月-年 时:分:秒
        "%d.%m.%Y %H:%M",  # 日-月-年 时:分:秒
        "%m/%d/%Y %I:%M:%S %p",  # 日-月-年 时:分:秒
        "%m/%d/%Y %H:%M",  # 日-月-年 时:分
        "%m/%d/%Y",   # 月/日/年        
        "%d.%m.%Y",   # 日/月/年
        "%d %B %Y", # 日 月 年
        "%d %b %Y",
        "%d-%b-%y",       # 日-月缩写-年（例如：01-Nov-16）
        "%b %d, %y",
        "%m/%d",   # 月/日
        "%m/%Y",   # 月/年
        "%b%y",   # 月/年
        "%Y-%m-%dT%H:%M:%S.%fZ",  # 年-月-日T时:分:秒.毫秒Z（UTC）
        "%I:%M %p",  # 时:分 %p（上下午）
        "%b %d",     # 月缩写 日
        "%A, %b %d",  # 星期全称，月缩写，日
        "%H:%M:%S",
        "%B" #月
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

        if len(data) >= 1000:
            data = data[:1000]  # 取前500条数据

        data_copy = copy.deepcopy(data)

        date_cols = original_table['dateColumns']  # 假设日期列在dateColumns中标记
        
        if not date_cols:
            continue  # 如果没有日期列，则跳过
        
        num_i = 0
        for col in date_cols:
            # global index
            index += 1
            num_i += 1 

            data_loop = copy.deepcopy(data_copy)
            print("ori_data",data_loop)

            clean_data = []
            clean_data_copy = []
            unclean_data = []
            for row in data_loop:
                parsed_value = parse_date1(row[col])
                if parsed_value is None:
                    unclean_data.append(row)
                else:
                    clean_data.append(row)

            clean_data_copy = copy.deepcopy(clean_data)

            print(f'first_clean_data:{clean_data_copy}')

            global global_fmt
            global_fmt = "%Y/%m/%d"

            # col_data = [parse_date(row[col]) for row in data]
            while True:  # 使用 while 循环来重新解析表格
                need_reparse = False
                print("clean_data_copy:",clean_data_copy[:10])
                clean_data = copy.deepcopy(clean_data_copy)
                print("clean_data:",clean_data[:10])
                print("2222222222")
                for row in clean_data:
                    try:
                        print("111111111")
                        print(row[col])
                        print(global_fmt)
                        parse_date = datetime.strptime(row[col], global_fmt)
                        print(row[col])
                        print("666666666666666666666666")
                        row[col] = parse_date
                        print(global_fmt)
                    except ValueError as e:
                        # 如果出现不符合当前 global_fmt 的情况，重置 global_fmt
                        # global_fmt = None
                        print("++++++++++++++++++++++++++++")
                        print(e)
                        for fmt in date_formats:
                            print("******************************")
                            try:
                                # 如果解析成功，更新全局日期格式并返回日期对象      
                                global_fmt = fmt
                                datetime.strptime(row[col], global_fmt)
                                break
                            except ValueError:
                                continue
                        # row[col] = parse_date3(row[col])
                        print(global_fmt)
                        need_reparse = True
                        # clean_data = clean_data_copy
                        break  # 跳出当前循环，重新解析
                if not need_reparse:
                    break  # 如果没有需要重新解析的情况，退出 while 循环
                else:
                    # 重置 global_fmt 后，重新解析整张表格
                    continue
            print(f"Processed {table_key} with format {global_fmt}")

            col_data = [row[col] for row in clean_data]
            if len(col_data) < 10:
                continue

            # segnum = np.percentile(col_data, random.uniform(20, 80))

            print(col_data)

            timestamps = [dt.timestamp() for dt in col_data]  # 转换为时间戳
            
            percentile_value = np.percentile(timestamps, random.uniform(20, 80))
            segnum = datetime.fromtimestamp(percentile_value) 

            

            # 将数据分为大于和小于等于 segnum 的两组
            # print(segnum)
            above_segnum_data = [row for row in clean_data if row[col] > segnum]
            below_or_equal_segnum_data = [row for row in clean_data if row[col] <= segnum]

            # 从大于 segnum 的组中随机选择 80%-100% 的行作为 datalake_data 的一部分
            datalake_above_proportion = random.uniform(0.8, 1.0)
            datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * datalake_above_proportion)]

            # 计算 datalake-qualifynum
            datalake_qualifynum = len(datalake_above_segnum)

            # 从小于等于 segnum 的组中随机选择 0 到 datalake-qualifynum/4 的行作为 datalake_data 的另一部分
            datalake_below_proportion = random.uniform(0, min(len(below_or_equal_segnum_data), datalake_qualifynum / 4))
            datalake_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * datalake_below_proportion)]

            # 合并 datalake_data
            datalake_data = datalake_above_segnum + datalake_below_segnum
            
            average_flag = 1
            try:
            # 计算平均值
                timestamps_datalake = [dt[col].timestamp() for dt in datalake_data]
                average_timestamp = np.mean(timestamps_datalake)  # 计算时间戳的平均值
                average_datetime = datetime.fromtimestamp(average_timestamp)  # 转换回 datetime 对象
                average_datetime_str = average_datetime.strftime(global_fmt)
            except Exception as e:
                print(e)
                average_flag = 0


            for row in datalake_data:
                try:
                    row[col] =  row[col].strftime(global_fmt)
                    print("datalake_data row[col]:", row[col])
                except Exception as e:
                    print(e)

            print("datalake_data:", datalake_data)

            print("datalake_data:",len(datalake_data))
            if len(datalake_data) < 10:
               continue

            # # 计算 datalake_data 在 row[col] 的平均值
            # average = round(np.mean([float(row[col]) for row in datalake_data]))

            # 从大于 segnum 的组中随机选择 0%-30% 的行作为 query_data 的一部分
            query_above_proportion = random.uniform(0, 0.3)
            query_above_segnum = above_segnum_data[int(len(above_segnum_data) * (1 - query_above_proportion)):]

            # 从小于等于 segnum 的组中随机选择 20%-80% 的行作为 query_data 的另一部分
            query_below_proportion = random.uniform(0.2, 0.8)
            query_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * query_below_proportion)]

            # 合并 query_data
            query_data = query_above_segnum + query_below_segnum

            for row in query_data:
                print(row[col])
                try:
                    row[col] =  row[col].strftime(global_fmt)
                except Exception as e:
                    print(e)


            # 获取原表名并去掉前三个字母
            original_name = table_key
            modified_name = original_name[3:]

            query_name = f"q{table_key[3:]}_3_6_{num_i}.json"
            query_name_nojson = f"q{table_key[3:]}_3_6_{num_i}"
            datalake_name = f"dl{table_key[3:]}_3_6_{num_i}.json"
            datalake_name_nojson = f"dl{table_key[3:]}_3_6_{num_i}"

            # 创建新的表格数据字典，保留原始表格的格式
            query_table_data = original_table.copy()
            datalake_table_data = original_table.copy()

            # 更新分割后的表格数据
            query_table_data["data"] = query_data
            query_table_data["numDataRows"] = len(query_data)
            query_table_data["table_array"] = titles + query_data

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["table_array"] = titles + datalake_data

            
            # 统计query_data中所有单元格值的出现频率
            cell_values = [cell for row in datalake_data for cell in row]
            value_counts = Counter(cell_values)

            # 选择排名前difficulty%的值
            top_values = [value for value, count in value_counts.most_common(int(difficulty * len(value_counts)) + 1)]

            # 从排名前difficulty%的值中随机选择一个作为caption
            selected_caption = random.choice(top_values)

            # 删除包含selected_caption的行
            data_without_caption = [row for row in data if selected_caption not in row]

            if len(data_without_caption)==0:
                continue

            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            if average_flag:
                if random.choice([True, False]):
                    # 随机选择template_num个模板
                    selected_templates = query_larger_templates[:template_num]
                
                    # 写入query.txt
                    with open(query_txt, 'a', encoding='utf-8') as q_txt:
                        selected_template = random.choice(selected_templates)
                        nl_query = selected_template.format(caption=titles[col], seg_num=segnum, second_caption=selected_caption)
                        # nl_query += f" and mention {selected_caption}"
                        q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
                
                # 写入groundtruth.txt
                    with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                        gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")
                
                else:
                    # 随机选择template_num个模板
                    selected_templates = query_average_templates[:template_num]
                
                    # 写入query.txt
                    with open(query_txt, 'a', encoding='utf-8') as q_txt:
                        selected_template = random.choice(selected_templates)
                        nl_query = selected_template.format(caption=titles[col], template_average=average_datetime_str, second_caption=selected_caption)
                        # nl_query += f" and mention {selected_caption}"
                        q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")

            else:
                selected_templates = query_larger_templates[:template_num]
                
                    # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], seg_num=segnum, second_caption=selected_caption)
                    # nl_query += f" and mention {selected_caption}"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(1, len(titles) - 1)
                available_indices = [i for i in range(len(titles)) if i != col]
                selected_col_indices = random.sample(available_indices, selected_col_count)
                
                # 随机选择行数，确保不少于1行
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue
                
                # for row in neg_datalake_data:
                #     print(row[col])
                #     try:
                #         row[col] =  row[col].strftime(global_fmt)
                #     except Exception as e:
                #         print(e)

                neg_datalake_name = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
            
                # 从小于等于 segnum 的组中随机选择 60%-80% 的行作为 datalake_data 的一部分
                neg_datalake_below_proportion = random.uniform(0.6, 0.8)
                neg_datalake_below_segnum = below_or_equal_segnum_data[:int(len(below_or_equal_segnum_data) * neg_datalake_below_proportion)]

                # 计算 datalake-qualifynum
                neg_datalake_qualifynum = len(neg_datalake_below_segnum)
                print("neg_datalake_qualifynum:", neg_datalake_qualifynum)

                # 从大于 segnum 的组中随机选择 0 到 datalake-qualifynum 的行作为 datalake_data 的另一部分
                neg_datalake_above_proportion = random.uniform(0, min(len(above_segnum_data),neg_datalake_qualifynum))
                neg_datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_above_proportion)]

                # 合并 datalake_data
                neg_datalake_data =  neg_datalake_below_segnum + neg_datalake_above_segnum

                # 从 unclean_data 中随机选择一定比例的数据添加到 neg_datalake_data
                unclean_proportion = random.uniform(0.1, 0.3)  # 假设我们想要添加 10%-30% 的 unclean_data
                num_unclean_to_add = int(len(unclean_data) * unclean_proportion)
                added_unclean_data = random.sample(unclean_data, num_unclean_to_add)
                neg_datalake_data += added_unclean_data
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue

                for row in neg_datalake_data:
                    print(row[col])
                    try:
                        row[col] =  row[col].strftime(global_fmt)
                    except Exception as e:
                        print(e)


                neg_datalake_name = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 

            for neg_number in range(neg_num*2 + 1, neg_num*3 + 1):
                # 随机选择行数，确保不少于1行且小于原表
                
                selected_row_count = random.randint(1, len(data_without_caption))
                neg_datalake_data = random.sample(data_without_caption, selected_row_count)
                
                neg_datalake_name = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

    return index

def split_cellvalue_smaller_date_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, neg_num=10, shuffle=1, template_num=3, difficulty=0.3):
    with open(json_file, 'r', encoding='utf-8') as file:
        tables = json.load(file)
    
    # 定义查询模板
    query_smaller_templates = [
        "I am on the lookout for additional unionable tables where {caption} is prior to {seg_num} and the tables include the keyword {second_caption}.",
        "I need to track down more unionable tables with {caption} preceding {seg_num} and containing the keyword {second_caption}.",
        "I am exploring further unionable tables, where {caption} is earlier than {seg_num} and the tables feature the keyword {second_caption}.",
        "I aim to locate more unionable tables with {caption} before {seg_num} and incorporating the keyword {second_caption}."
    ]

    query_average_templates = [
        "I'm seeking out additional unionable tables where {caption} is roughly {template_average} and the tables include the keyword {second_caption}.",
        "I need to uncover more unionable tables with {caption} hovering around {template_average} and featuring the keyword {second_caption}.",
        "I'm investigating further unionable tables, where {caption} is in the vicinity of {template_average} and the tables have the keyword {second_caption}.",
        "I aim to locate more unionable tables with {caption} close to {template_average} and containing the keyword {second_caption}."
    ]

    date_formats = [
        "%Y/%m/%d",  # 年/月/日
        "%Y.%m.%d",  # 年.月.日
        "%Y-%m-%d",  # 年-月-日
        "%d/%m/%Y",   # 日/月/年
        "%d-%m-%Y %H:%M:%S",  # 日-月-年 时:分:秒
        "%d.%m.%Y %H:%M",  # 日-月-年 时:分:秒
        "%m/%d/%Y %I:%M:%S %p",  # 日-月-年 时:分:秒
        "%m/%d/%Y %H:%M",  # 日-月-年 时:分
        "%m/%d/%Y",   # 月/日/年        
        "%d.%m.%Y",   # 日/月/年
        "%d %B %Y", # 日 月 年
        "%d %b %Y",
        "%d-%b-%y",       # 日-月缩写-年（例如：01-Nov-16）
        "%b %d, %y",
        "%m/%d",   # 月/日
        "%m/%Y",   # 月/年
        "%b%y",   # 月/年
        "%Y-%m-%dT%H:%M:%S.%fZ",  # 年-月-日T时:分:秒.毫秒Z（UTC）
        "%I:%M %p",  # 时:分 %p（上下午）
        "%b %d",     # 月缩写 日
        "%A, %b %d",  # 星期全称，月缩写，日
        "%H:%M:%S",
        "%B", #月
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

        if len(data) >= 1000:
            data = data[:1000]  # 取前500条数据

        data_copy = copy.deepcopy(data)

        date_cols = original_table['dateColumns']  # 假设日期列在dateColumns中标记
        
        if not date_cols:
            continue  # 如果没有日期列，则跳过
        
        num_i = 0
        for col in date_cols:
            # global index
            index += 1
            num_i += 1 

            data_loop = copy.deepcopy(data_copy)
            print("ori_data",data_loop)

            clean_data = []
            clean_data_copy = []
            unclean_data = []
            for row in data_loop:
                parsed_value = parse_date1(row[col])
                if parsed_value is None:
                    unclean_data.append(row)
                else:
                    clean_data.append(row)

            clean_data_copy = copy.deepcopy(clean_data)

            print(f'first_clean_data:{clean_data_copy}')

            global global_fmt
            global_fmt = "%Y/%m/%d"

            # col_data = [parse_date(row[col]) for row in data]
            while True:  # 使用 while 循环来重新解析表格
                need_reparse = False
                print("clean_data_copy:",clean_data_copy[:10])
                clean_data = copy.deepcopy(clean_data_copy)
                print("clean_data:",clean_data[:10])
                print("2222222222")
                for row in clean_data:
                    try:
                        print("111111111")
                        print(row[col])
                        print(global_fmt)
                        parse_date = datetime.strptime(row[col], global_fmt)
                        print(row[col])
                        print("666666666666666666666666")
                        row[col] = parse_date
                        print(global_fmt)
                    except ValueError as e:
                        # 如果出现不符合当前 global_fmt 的情况，重置 global_fmt
                        # global_fmt = None
                        print("++++++++++++++++++++++++++++")
                        print(e)
                        for fmt in date_formats:
                            print("******************************")
                            try:
                                # 如果解析成功，更新全局日期格式并返回日期对象      
                                global_fmt = fmt
                                datetime.strptime(row[col], global_fmt)
                                break
                            except ValueError:
                                continue
                        # row[col] = parse_date3(row[col])
                        print(global_fmt)
                        need_reparse = True
                        # clean_data = clean_data_copy
                        break  # 跳出当前循环，重新解析
                if not need_reparse:
                    break  # 如果没有需要重新解析的情况，退出 while 循环
                else:
                    # 重置 global_fmt 后，重新解析整张表格
                    continue
            print(f"Processed {table_key} with format {global_fmt}")

            col_data = [row[col] for row in clean_data]
            if len(col_data) < 10:
                continue

            # segnum = np.percentile(col_data, random.uniform(20, 80))

            print(col_data)

            timestamps = [dt.timestamp() for dt in col_data]  # 转换为时间戳
            
            percentile_value = np.percentile(timestamps, random.uniform(20, 80))
            segnum = datetime.fromtimestamp(percentile_value) 

            

            # 将数据分为大于和小于等于 segnum 的两组
            # print(segnum)
            above_segnum_data = [row for row in clean_data if row[col] > segnum]
            below_or_equal_segnum_data = [row for row in clean_data if row[col] <= segnum]

            # 从大于 segnum 的组中随机选择 80%-100% 的行作为 datalake_data 的一部分
            datalake_below_proportion = random.uniform(0.8, 1.0)
            datalake_below_segnum = above_segnum_data[:int(len(below_or_equal_segnum_data) * datalake_below_proportion)]

            # 计算 datalake-qualifynum
            datalake_qualifynum = len(datalake_below_segnum)

            # 从小于等于 segnum 的组中随机选择 0 到 datalake-qualifynum/4 的行作为 datalake_data 的另一部分
            datalake_above_proportion = random.uniform(0, min(len(above_segnum_data), datalake_qualifynum / 4))
            datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * datalake_above_proportion)]

            # 合并 datalake_data
            datalake_data = datalake_below_segnum + datalake_above_segnum

            average_flag = 1
            try:
            # 计算平均值
                timestamps_datalake = [dt[col].timestamp() for dt in datalake_data]
                average_timestamp = np.mean(timestamps_datalake)  # 计算时间戳的平均值
                average_datetime = datetime.fromtimestamp(average_timestamp)  # 转换回 datetime 对象
                average_datetime_str = average_datetime.strftime(global_fmt)
            except Exception as e:
                print(e)
                average_flag = 0

            for row in datalake_data:
                try:
                    row[col] =  row[col].strftime(global_fmt)
                    print("datalake_data row[col]:", row[col])
                except Exception as e:
                    print(e)

            print("datalake_data:", datalake_data)

            print("datalake_data:",len(datalake_data))
            if len(datalake_data) < 10:
               continue

            # # 计算 datalake_data 在 row[col] 的平均值
            # average = round(np.mean([float(row[col]) for row in datalake_data]))

            # 从大于 segnum 的组中随机选择 0%-30% 的行作为 query_data 的一部分
            query_below_proportion = random.uniform(0, 0.3)
            query_below_segnum = above_segnum_data[int(len(below_or_equal_segnum_data) * (1 - query_below_proportion)):]

            # 从小于等于 segnum 的组中随机选择 20%-80% 的行作为 query_data 的另一部分
            query_above_proportion = random.uniform(0.2, 0.8)
            query_above_segnum = above_segnum_data[:int(len(above_segnum_data) * query_above_proportion)]

            # 合并 query_data
            query_data = query_below_segnum + query_above_segnum

            for row in query_data:
                print(row[col])
                try:
                    row[col] =  row[col].strftime(global_fmt)
                except Exception as e:
                    print(e)


            # 获取原表名并去掉前三个字母
            original_name = table_key
            modified_name = original_name[3:]

            query_name = f"q{table_key[3:]}_3_6_{num_i}.json"
            query_name_nojson = f"q{table_key[3:]}_3_6_{num_i}"
            datalake_name = f"dl{table_key[3:]}_3_6_{num_i}.json"
            datalake_name_nojson = f"dl{table_key[3:]}_3_6_{num_i}"

            # 创建新的表格数据字典，保留原始表格的格式
            query_table_data = original_table.copy()
            datalake_table_data = original_table.copy()

            # 更新分割后的表格数据
            query_table_data["data"] = query_data
            query_table_data["numDataRows"] = len(query_data)
            query_table_data["table_array"] = titles + query_data

            datalake_table_data["data"] = datalake_data
            datalake_table_data["numDataRows"] = len(datalake_data)
            datalake_table_data["table_array"] = titles + datalake_data

            # 统计query_data中所有单元格值的出现频率
            cell_values = [cell for row in datalake_data for cell in row]
            value_counts = Counter(cell_values)

            # 选择排名前difficulty%的值
            top_values = [value for value, count in value_counts.most_common(int(difficulty * len(value_counts)) + 1)]

            # 从排名前difficulty%的值中随机选择一个作为caption
            selected_caption = random.choice(top_values)

            # 删除包含selected_caption的行
            data_without_caption = [row for row in data if selected_caption not in row]

            if len(data_without_caption)==0:
                continue

            # 写入新表
            with open(os.path.join(query_folder, query_name), 'w', encoding='utf-8') as q_file:
                json.dump(query_table_data, q_file, indent=4)
            with open(os.path.join(datalake_folder, datalake_name), 'w', encoding='utf-8') as dl_file:
                json.dump(datalake_table_data, dl_file, indent=4)
            
            
            if average_flag:
                if random.choice([True, False]):
                    # 随机选择template_num个模板
                    selected_templates = query_smaller_templates[:template_num]
                
                    # 写入query.txt
                    with open(query_txt, 'a', encoding='utf-8') as q_txt:
                        selected_template = random.choice(selected_templates)
                        nl_query = selected_template.format(caption=titles[col], seg_num=segnum, second_caption=selected_caption)
                        # nl_query += f" and mention {selected_caption}"
                        q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
                
                # 写入groundtruth.txt
                    with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                        gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")
                
                else:
                    # 随机选择template_num个模板
                    selected_templates = query_average_templates[:template_num]
                
                    # 写入query.txt
                    with open(query_txt, 'a', encoding='utf-8') as q_txt:
                        selected_template = random.choice(selected_templates)
                        nl_query = selected_template.format(caption=titles[col], template_average=average_datetime_str, second_caption=selected_caption)
                        # nl_query += f" and mention {selected_caption}"
                        q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")

            else:
                selected_templates = query_smaller_templates[:template_num]
                
                    # 写入query.txt
                with open(query_txt, 'a', encoding='utf-8') as q_txt:
                    selected_template = random.choice(selected_templates)
                    nl_query = selected_template.format(caption=titles[col], seg_num=segnum, second_caption=selected_caption)
                    # nl_query += f" and mention {selected_caption}"
                    q_txt.write(f"{index}\t{nl_query}\t{query_name_nojson}\n")
            
            # 写入groundtruth.txt
            with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                gt_txt.write(f"{index}\t0\t{datalake_name_nojson}\t2\n")

            for neg_number in range(1, neg_num + 1):
                # 随机选择列数，确保少于原表
                selected_col_count = random.randint(1, len(titles) - 1)
                available_indices = [i for i in range(len(titles)) if i != col]
                selected_col_indices = random.sample(available_indices, selected_col_count)
                
                # 随机选择行数，确保不少于1行
                selected_row_count = random.randint(1, int(len(data) * 0.5))
                neg_datalake_data = [[row[i] for i in selected_col_indices] for row in random.sample(data, selected_row_count)]
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue
                
                # for row in neg_datalake_data:
                #     print(row[col])
                #     try:
                #         row[col] =  row[col].strftime(global_fmt)
                #     except Exception as e:
                #         print(e)

                neg_datalake_name = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["title"] = [titles[i] for i in selected_col_indices]
                neg_datalake_table_data["table_array"] = neg_datalake_table_data["title"] + neg_datalake_data
                neg_datalake_table_data["numCols"] = selected_col_count
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

            for neg_number in range(neg_num + 1, neg_num + neg_num + 1):
                # 随机选择行数，确保不少于1行且小于原表
            
                # 从小于等于 segnum 的组中随机选择 60%-80% 的行作为 datalake_data 的一部分
                neg_datalake_above_proportion = random.uniform(0.6, 0.8)
                neg_datalake_above_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_above_proportion)]

                # 计算 datalake-qualifynum
                neg_datalake_qualifynum = len(neg_datalake_above_segnum)
                print("neg_datalake_qualifynum:", neg_datalake_qualifynum)

                # 从大于 segnum 的组中随机选择 0 到 datalake-qualifynum 的行作为 datalake_data 的另一部分
                neg_datalake_below_proportion = random.uniform(0, min(len(below_or_equal_segnum_data),neg_datalake_qualifynum))
                neg_datalake_below_segnum = above_segnum_data[:int(len(above_segnum_data) * neg_datalake_below_proportion)]

                # 合并 datalake_data
                neg_datalake_data = neg_datalake_above_segnum + neg_datalake_below_segnum

                # 从 unclean_data 中随机选择一定比例的数据添加到 neg_datalake_data
                unclean_proportion = random.uniform(0.1, 0.3)  # 假设我们想要添加 10%-30% 的 unclean_data
                num_unclean_to_add = int(len(unclean_data) * unclean_proportion)
                added_unclean_data = random.sample(unclean_data, num_unclean_to_add)
                neg_datalake_data += added_unclean_data
                
                print("neg_datalake_data:", len(neg_datalake_data))
                if len(neg_datalake_data) < 10:
                    continue

                for row in neg_datalake_data:
                    print(row[col])
                    try:
                        row[col] =  row[col].strftime(global_fmt)
                    except Exception as e:
                        print(e)


                neg_datalake_name = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n") 

            for neg_number in range(neg_num*2 + 1, neg_num*3 + 1):
                # 随机选择行数，确保不少于1行且小于原表
                
                selected_row_count = random.randint(1, len(data_without_caption))
                neg_datalake_data = random.sample(data_without_caption, selected_row_count)
                
                neg_datalake_name = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}.json"
                neg_datalake_name_nojson = f"dl{modified_name}_3_6_{num_i}_n_{neg_number}"
                neg_datalake_table_data = original_table.copy()
                neg_datalake_table_data["data"] = neg_datalake_data
                neg_datalake_table_data["table_array"] = titles + neg_datalake_data
                neg_datalake_table_data["numDataRows"] = selected_row_count
                
                with open(os.path.join(datalake_folder, neg_datalake_name), 'w', encoding='utf-8') as dl_file:
                    json.dump(neg_datalake_table_data, dl_file, indent=4)
                with open(groundtruth_txt, 'a', encoding='utf-8') as gt_txt:
                    gt_txt.write(f"{index}\t0\t{neg_datalake_name_nojson}\t0\n")

    return index



def process_dataset(exp, dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt):

    if exp == 'scale_category':
        for json_file in os.listdir(dataset_folder):
            if json_file.endswith('.json'):
                split_scale_category_table(
                    os.path.join(dataset_folder, json_file),
                    query_folder, 
                    datalake_folder, 
                    query_txt, 
                    groundtruth_txt
                )  
    
    if exp == 'scale_numerical':
        for json_file in os.listdir(dataset_folder):
            if json_file.endswith('.json'):
                if random.choice([True, False]):
                    split_scale_numerical_larger_table(
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )    
                else:
                    split_scale_numerical_smaller_table(
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )   

    if exp == 'scale_date':
        for json_file in os.listdir(dataset_folder):
            if json_file.endswith('.json'):
                if random.choice([True, False]):
                    split_scale_larger_date_table(
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )    
                else:
                    split_scale_smaller_date_table(
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    ) 

    if exp == 'cellvalue_category':
        for json_file in os.listdir(dataset_folder):
            if json_file.endswith('.json'):
                split_cellvalue_category_table(
                    os.path.join(dataset_folder, json_file),
                    query_folder, 
                    datalake_folder, 
                    query_txt, 
                    groundtruth_txt
                )  
    
    if exp == 'cellvalue_numerical':
        for json_file in os.listdir(dataset_folder):
            if json_file.endswith('.json'):
                if random.choice([True, False]):
                    split_cellvalue_numerical_larger_table(
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )    
                else:
                    split_cellvalue_numerical_smaller_table(
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )   

    if exp == 'cellvalue_date':
        for json_file in os.listdir(dataset_folder):
            if json_file.endswith('.json'):
                if random.choice([True, False]):
                    split_cellvalue_larger_date_table(
                        os.path.join(dataset_folder, json_file),
                        query_folder, 
                        datalake_folder, 
                        query_txt, 
                        groundtruth_txt
                    )    
                else:
                    split_cellvalue_smaller_date_table(
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

# dataset_folder = 'qualified-ori-datalake4'
# query_folder = 'Union1/multi/query-test'
# datalake_folder = 'Union1/multi/datalake-test'
# query_txt = 'Union1/multi/queries-test.txt'
# groundtruth_txt = 'Union1/multi/qtrels-test.txt'

# if not os.path.exists(query_folder):
#     os.makedirs(query_folder)
# if not os.path.exists(datalake_folder):
#     os.makedirs(datalake_folder)

# process_dataset(args.exp,dataset_folder, query_folder, datalake_folder, query_txt, groundtruth_txt)