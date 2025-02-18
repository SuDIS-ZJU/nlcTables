import httpx
import requests
import os
import json
from openai import OpenAI

client = OpenAI(
    base_url="https://api.xiaoai.plus/v1",
    api_key="sk-zxQPjRNR374gmUsA5cFfEbFd7c37408a83118f3598C53c06",
    http_client=httpx.Client(
        base_url="https://api.xiaoai.plus/v1",
        follow_redirects=True,
    ),
)

def gpt_answer(prompt, temperature = 0.0, system_content = "You are a helpful assistant.", model = "gpt-4o-mini"):
    while True:
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature
            )
        except requests.exceptions.RequestException as e:
            print("请求错误:", e)
        except Exception as e:
            print("发生错误:", e)
        else:
            answer = response.choices[0].message.content
            print(answer)
            print()
            return answer
        
if __name__ == '__main__':
    input_folder = 'fuzzy-join/fuzzy_test/oridata'  # 替换为包含 JSON 文件的文件夹路径
    output_folder = 'fuzzy-join/fuzzy_test/result'  # 替换为新的输出文件夹路径

    # 确保输出文件夹存在
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # 遍历文件夹中的所有 JSON 文件
    for filename in os.listdir(input_folder):
        if filename.endswith('.json'):  # 确保处理的是 JSON 文件
            file_path = os.path.join(input_folder, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                try:
                    table_data = json.load(file)
                    # 提取 "table-array" 内容
                    table_data_only = table_data["data"]
                    title = table_data["title"]
                    targetCol = table_data["targetCol"]
                    if len(table_data_only) > 100:
                        table_data_only = table_data_only[:100]
                        # continue
                
                    # 构建提示词
                    # prompt = (
                    #     "我需要你将其中一列的内容全部转化为同一格式的相似值（语义相似变换），"
                    #     "例如原表格内容如下:"
                    #     "'[Country,Continent],[China,Asia],[United States,North America],[Brazil,South America],[Germany,Europe],[Australia,Oceania],[India,Asia],[Russia,Europe],[Canada,North America],[South Africa,Africa]',\n"
                    #     "对于原表中的Country列进行语义相似变换得到:\n"
                    #     "'[Cntr,Continent],[CN,Asia],[US,North America],[BR,South America],[DE,Europe],[AU,Oceania],[IN,Asia],[RU,Europe],[CA,North America][ZA,Africa]'，\n"
                    #     "参照上述例子，现在我需要你帮我对下面这张表进行语义相似变换，随机选择至少一列进行变换，变换前后不改变语义。\n"
                    #     "注意！仍然按以下的表格格式返回给我变换后的表格，不要添加任何除表格外的信息:\n"
                    # )

                    prompt = (
                        "I need you to convert all the contents of one column into similar values in the same format (semantic similarity transformation)"
                        "For example, the original table content is as follows:"
                        "'[Country,Continent],[China,Asia],[United States,North America],[Brazil,South America],[Germany,Europe],[Australia,Oceania],[India,Asia],[Russia,Europe],[Canada,North America],[South Africa,Africa]',\n"
                        "Performing semantic similarity transformation on the Country column in the original table yields: \n"
                        "'[Cntr,Continent],[CN,Asia],[US,North America],[BR,South America],[DE,Europe],[AU,Oceania],[IN,Asia],[RU,Europe],[CA,North America][ZA,Africa]',\n"
                        "Referring to the above example, now I need you to help me perform semantic similarity transformation on the json table below."
                        
                    )
                    prompt += f"Please try to perform semantic similarity transformation on the {targetCol}. And you can futher select at least one column for transformation. Semantic similarity transformation does not change the semantics before and after the transformation"
                    prompt +="Attention! Please return the transformed table to me in the following json format and do not add any information other than the table!:"
                    
                    # 将表格内容添加到提示词中
                    prompt += str(title+table_data_only)

                    print(prompt)

                    # 调用 GPT 模型进行处理
                    transformed_data = gpt_answer(prompt)

                    # 替换原表的 "table-array"
                    table_data["data"] = transformed_data
                    table_data["table_array"] = transformed_data

                    # 构建新的 JSON 文件路径
                    new_file_path = os.path.join(output_folder, filename)

                    # 将新获得的 JSON 表格存入新的文件夹
                    with open(new_file_path, 'w', encoding='utf-8') as new_file:
                        json.dump(table_data, new_file, indent=4)

                    print(f"Processed {filename} and saved to {new_file_path}")

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from {filename}: {e}")
                except Exception as e:
                    print(f"Error processing {filename}: {e}")
        